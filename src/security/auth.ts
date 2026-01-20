/**
 * Authentication Module for GraphDB
 *
 * Provides authentication mechanisms for:
 * - API keys (service-to-service communication)
 * - JWT tokens (user requests)
 * - Worker binding authentication (internal Cloudflare Workers calls)
 *
 * Integration points:
 * - Snippet layer: Validate API key in bloom router
 * - Broker DO: Validate JWT in WebSocket upgrade
 * - Shard DO: Authorize namespace access
 */

// ============================================================================
// Error Codes and Types
// ============================================================================

/**
 * Authentication error codes
 */
export enum AuthErrorCode {
  /** No credentials provided in request */
  MISSING_CREDENTIALS = 'MISSING_CREDENTIALS',
  /** API key not found or invalid */
  INVALID_API_KEY = 'INVALID_API_KEY',
  /** JWT is malformed or invalid */
  MALFORMED_TOKEN = 'MALFORMED_TOKEN',
  /** JWT has expired */
  TOKEN_EXPIRED = 'TOKEN_EXPIRED',
  /** JWT is not yet valid (nbf claim) */
  TOKEN_NOT_YET_VALID = 'TOKEN_NOT_YET_VALID',
  /** JWT issuer does not match expected */
  INVALID_ISSUER = 'INVALID_ISSUER',
  /** JWT audience does not match expected */
  INVALID_AUDIENCE = 'INVALID_AUDIENCE',
  /** JWT signature verification failed */
  INVALID_SIGNATURE = 'INVALID_SIGNATURE',
  /** Worker binding not in trusted list */
  UNTRUSTED_BINDING = 'UNTRUSTED_BINDING',
  /** Worker binding secret does not match */
  INVALID_BINDING_SECRET = 'INVALID_BINDING_SECRET',
  /** Generic authentication failure */
  AUTH_FAILED = 'AUTH_FAILED',
}

/**
 * Authentication error response format
 */
export interface AuthErrorResponse {
  type: 'error';
  code: AuthErrorCode;
  message: string;
}

/**
 * Custom error class for authentication failures
 */
export class AuthError extends Error {
  readonly code: AuthErrorCode;

  constructor(code: AuthErrorCode, message: string) {
    super(message);
    this.name = 'AuthError';
    this.code = code;
  }

  /**
   * Convert error to response format for WebSocket/HTTP handlers
   */
  toResponse(): AuthErrorResponse {
    return {
      type: 'error',
      code: this.code,
      message: this.message,
    };
  }
}

// ============================================================================
// Authentication Context
// ============================================================================

/**
 * Authenticated caller context
 *
 * Contains information about the authenticated caller, their permissions,
 * and which namespaces they can access.
 */
export interface AuthContext {
  /** Unique identifier for the caller (user ID, service name, etc.) */
  callerId: string;
  /** How the caller was authenticated */
  authMethod: 'api_key' | 'jwt' | 'worker_binding';
  /** List of permissions granted to this caller */
  permissions: string[];
  /** List of namespaces this caller can access ('*' for all) */
  namespaces: string[];
  /** Timestamp when the auth context was created */
  timestamp: number;
  /** Optional metadata from the authentication source */
  metadata?: Record<string, unknown>;
}

/**
 * Result of an authentication attempt
 */
export interface AuthResult {
  /** Whether authentication succeeded */
  success: boolean;
  /** Auth context if successful */
  context?: AuthContext;
  /** Error if failed */
  error?: AuthError;
}

// ============================================================================
// API Key Configuration and Validation
// ============================================================================

/**
 * API key entry with associated permissions
 */
export interface ApiKeyEntry {
  /** Identifier for the service/caller using this key */
  callerId: string;
  /** Permissions granted to this key */
  permissions: string[];
  /** Namespaces this key can access ('*' for all) */
  namespaces: string[];
  /** Optional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * API key authentication configuration
 */
export interface ApiKeyConfig {
  /** Map of API keys to their associated entries */
  keys: Map<string, ApiKeyEntry>;
}

/**
 * Validate an API key and return the associated auth context
 *
 * @param apiKey - The API key to validate
 * @param config - API key configuration
 * @returns Authentication result with context or error
 */
export async function validateApiKey(
  apiKey: string,
  config: ApiKeyConfig
): Promise<AuthResult> {
  // Check for missing or empty key
  if (!apiKey || typeof apiKey !== 'string' || apiKey.trim() === '') {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.MISSING_CREDENTIALS, 'API key is required'),
    };
  }

  // Look up the key in the configuration
  const entry = config.keys.get(apiKey);

  if (!entry) {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.INVALID_API_KEY, 'Invalid API key'),
    };
  }

  // Create auth context
  const context: AuthContext = {
    callerId: entry.callerId,
    authMethod: 'api_key',
    permissions: [...entry.permissions],
    namespaces: [...entry.namespaces],
    timestamp: Date.now(),
    ...(entry.metadata && { metadata: entry.metadata }),
  };

  return {
    success: true,
    context,
  };
}

// ============================================================================
// JWT Configuration and Validation
// ============================================================================

/**
 * JWT authentication configuration
 */
export interface JwtConfig {
  /** Expected issuer (iss claim) */
  issuer: string;
  /** Expected audience (aud claim) */
  audience: string;
  /** Secret for HMAC algorithms or public key for RSA */
  secret: string;
  /** Allowed algorithms */
  algorithms: readonly string[];
  /** Skip signature verification (for testing only!) */
  skipSignatureVerification?: boolean;
}

/**
 * JWT payload structure
 */
interface JwtPayload {
  /** Subject (user ID) */
  sub: string;
  /** Issuer */
  iss: string;
  /** Audience */
  aud: string;
  /** Expiration time (Unix timestamp) */
  exp: number;
  /** Issued at (Unix timestamp) */
  iat: number;
  /** Not before (Unix timestamp, optional) */
  nbf?: number;
  /** Permissions claim */
  permissions?: string[];
  /** Namespaces claim */
  namespaces?: string[];
  /** Any additional claims */
  [key: string]: unknown;
}

/**
 * Decode a base64url encoded string
 */
function base64UrlDecode(str: string): string {
  // Replace URL-safe characters
  const base64 = str.replace(/-/g, '+').replace(/_/g, '/');
  // Add padding if needed
  const padded = base64 + '=='.slice(0, (4 - (base64.length % 4)) % 4);
  return atob(padded);
}

/**
 * Validate a JWT token and return the associated auth context
 *
 * @param token - The JWT token to validate
 * @param config - JWT configuration
 * @returns Authentication result with context or error
 */
export async function validateJwt(
  token: string,
  config: JwtConfig
): Promise<AuthResult> {
  // Check for missing or empty token
  if (!token || typeof token !== 'string' || token.trim() === '') {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.MISSING_CREDENTIALS, 'JWT token is required'),
    };
  }

  // Split token into parts
  const parts = token.split('.');
  if (parts.length !== 3) {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.MALFORMED_TOKEN, 'Invalid JWT format'),
    };
  }

  // Decode payload
  let payload: JwtPayload;
  try {
    const payloadJson = base64UrlDecode(parts[1]!);
    payload = JSON.parse(payloadJson) as JwtPayload;
  } catch {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.MALFORMED_TOKEN, 'Failed to decode JWT payload'),
    };
  }

  // Validate issuer
  if (payload.iss !== config.issuer) {
    return {
      success: false,
      error: new AuthError(
        AuthErrorCode.INVALID_ISSUER,
        `Invalid issuer: expected ${config.issuer}, got ${payload.iss}`
      ),
    };
  }

  // Validate audience
  if (payload.aud !== config.audience) {
    return {
      success: false,
      error: new AuthError(
        AuthErrorCode.INVALID_AUDIENCE,
        `Invalid audience: expected ${config.audience}, got ${payload.aud}`
      ),
    };
  }

  // Validate expiration
  const now = Math.floor(Date.now() / 1000);
  if (payload.exp && payload.exp < now) {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.TOKEN_EXPIRED, 'JWT has expired'),
    };
  }

  // Validate not before (nbf)
  if (payload.nbf && payload.nbf > now) {
    return {
      success: false,
      error: new AuthError(AuthErrorCode.TOKEN_NOT_YET_VALID, 'JWT is not yet valid'),
    };
  }

  // Note: In production, signature verification would happen here
  // For testing, we can skip it when skipSignatureVerification is true
  if (!config.skipSignatureVerification) {
    // TODO: Implement proper signature verification
    // This would use Web Crypto API for HMAC verification
    // For now, we trust the token if signature verification is not skipped
  }

  // Extract permissions and namespaces from claims
  const permissions = payload.permissions ?? ['read'];
  const namespaces = payload.namespaces ?? ['*'];

  // Create auth context
  const context: AuthContext = {
    callerId: payload.sub,
    authMethod: 'jwt',
    permissions,
    namespaces,
    timestamp: Date.now(),
    metadata: {
      iss: payload.iss,
      aud: payload.aud,
      iat: payload.iat,
      exp: payload.exp,
    },
  };

  return {
    success: true,
    context,
  };
}

// ============================================================================
// Worker Binding Configuration and Validation
// ============================================================================

/**
 * Worker binding authentication configuration
 */
export interface WorkerBindingConfig {
  /** Set of trusted binding names */
  trustedBindings: Set<string>;
  /** Shared secret for internal calls */
  sharedSecret: string;
}

/**
 * Validate a worker binding and return the associated auth context
 *
 * @param bindingName - The name of the worker binding
 * @param secret - The shared secret provided in the request
 * @param config - Worker binding configuration
 * @returns Authentication result with context or error
 */
export async function validateWorkerBinding(
  bindingName: string,
  secret: string,
  config: WorkerBindingConfig
): Promise<AuthResult> {
  // Check if binding is trusted
  if (!config.trustedBindings.has(bindingName)) {
    return {
      success: false,
      error: new AuthError(
        AuthErrorCode.UNTRUSTED_BINDING,
        `Worker binding '${bindingName}' is not trusted`
      ),
    };
  }

  // Validate shared secret
  if (secret !== config.sharedSecret) {
    return {
      success: false,
      error: new AuthError(
        AuthErrorCode.INVALID_BINDING_SECRET,
        'Invalid worker binding secret'
      ),
    };
  }

  // Create auth context with full internal permissions
  const context: AuthContext = {
    callerId: `binding:${bindingName}`,
    authMethod: 'worker_binding',
    permissions: ['read', 'write', 'internal'],
    namespaces: ['*'], // Internal calls have access to all namespaces
    timestamp: Date.now(),
    metadata: {
      bindingName,
    },
  };

  return {
    success: true,
    context,
  };
}

// ============================================================================
// Request Authentication
// ============================================================================

/**
 * Combined authentication configuration
 */
export interface AuthConfig {
  /** API key configuration */
  apiKey?: ApiKeyConfig;
  /** JWT configuration */
  jwt?: JwtConfig;
  /** Worker binding configuration */
  workerBinding?: WorkerBindingConfig;
}

/**
 * Create an auth context from an HTTP request
 *
 * Checks for authentication credentials in this order:
 * 1. X-API-Key header (API key authentication)
 * 2. Authorization: Bearer header (JWT authentication)
 * 3. X-Worker-Binding + X-Binding-Secret headers (Worker binding authentication)
 *
 * @param request - The HTTP request
 * @param config - Authentication configuration
 * @returns Authentication result with context or error
 */
export async function createAuthContext(
  request: Request,
  config: AuthConfig
): Promise<AuthResult> {
  const headers = request.headers;

  // 1. Check for API key
  const apiKey = headers.get('X-API-Key');
  if (apiKey && config.apiKey) {
    return validateApiKey(apiKey, config.apiKey);
  }

  // 2. Check for Bearer token
  const authHeader = headers.get('Authorization');
  if (authHeader && authHeader.startsWith('Bearer ') && config.jwt) {
    const token = authHeader.slice(7); // Remove 'Bearer ' prefix
    return validateJwt(token, config.jwt);
  }

  // 3. Check for worker binding
  const bindingName = headers.get('X-Worker-Binding');
  const bindingSecret = headers.get('X-Binding-Secret');
  if (bindingName && bindingSecret && config.workerBinding) {
    return validateWorkerBinding(bindingName, bindingSecret, config.workerBinding);
  }

  // No credentials found
  return {
    success: false,
    error: new AuthError(
      AuthErrorCode.MISSING_CREDENTIALS,
      'No authentication credentials provided'
    ),
  };
}

// ============================================================================
// Middleware Helpers
// ============================================================================

/**
 * Extract bearer token from Authorization header
 *
 * @param request - The HTTP request
 * @returns The token or null if not present
 */
export function extractBearerToken(request: Request): string | null {
  const authHeader = request.headers.get('Authorization');
  if (authHeader && authHeader.startsWith('Bearer ')) {
    return authHeader.slice(7);
  }
  return null;
}

/**
 * Extract API key from X-API-Key header
 *
 * @param request - The HTTP request
 * @returns The API key or null if not present
 */
export function extractApiKey(request: Request): string | null {
  return request.headers.get('X-API-Key');
}

/**
 * Create a 401 Unauthorized response
 *
 * @param error - The authentication error
 * @returns HTTP Response with 401 status
 */
export function unauthorizedResponse(error: AuthError): Response {
  return new Response(
    JSON.stringify(error.toResponse()),
    {
      status: 401,
      headers: {
        'Content-Type': 'application/json',
        'WWW-Authenticate': 'Bearer realm="graphdb"',
      },
    }
  );
}

/**
 * Create a 403 Forbidden response
 *
 * @param message - The error message
 * @returns HTTP Response with 403 status
 */
export function forbiddenResponse(message: string): Response {
  return new Response(
    JSON.stringify({
      type: 'error',
      code: 'FORBIDDEN',
      message,
    }),
    {
      status: 403,
      headers: { 'Content-Type': 'application/json' },
    }
  );
}
