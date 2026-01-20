/**
 * JSON Validator - Safe JSON parsing with DoS protection
 *
 * Provides protection against:
 * - Memory exhaustion (size limits)
 * - Stack overflow (depth limits)
 * - Hash collision attacks (key count limits)
 * - Malformed input (graceful error handling)
 *
 * IMPORTANT: Size is checked BEFORE parsing to prevent memory exhaustion.
 */

// ============================================================================
// Constants
// ============================================================================

/** Default maximum message size in bytes (64KB) */
export const DEFAULT_MAX_SIZE = 65536;

/** Default maximum nesting depth */
export const DEFAULT_MAX_DEPTH = 10;

/** Default maximum total key count across all objects */
export const DEFAULT_MAX_KEYS = 1000;

// ============================================================================
// Types
// ============================================================================

/**
 * Options for safe JSON parsing
 */
export interface JsonParseOptions {
  /** Maximum size in bytes (default 64KB) */
  maxSize?: number;
  /** Maximum nesting depth (default 10) */
  maxDepth?: number;
  /** Maximum total key count (default 1000) */
  maxKeys?: number;
}

/**
 * Error codes for JSON parsing failures
 */
export enum JsonParseErrorCode {
  /** Input exceeds maximum allowed size */
  SIZE_EXCEEDED = 'SIZE_EXCEEDED',
  /** Nesting depth exceeds maximum */
  DEPTH_EXCEEDED = 'DEPTH_EXCEEDED',
  /** Total key count exceeds maximum */
  KEYS_EXCEEDED = 'KEYS_EXCEEDED',
  /** JSON syntax error or invalid input */
  PARSE_ERROR = 'PARSE_ERROR',
}

/**
 * Error response format for WebSocket handlers
 */
export interface JsonErrorResponse {
  type: 'error';
  code: JsonParseErrorCode;
  message: string;
}

/**
 * Custom error class for JSON parsing failures
 */
export class JsonParseError extends Error {
  readonly code: JsonParseErrorCode;

  constructor(code: JsonParseErrorCode, message: string) {
    super(message);
    this.name = 'JsonParseError';
    this.code = code;
    // Maintains proper stack trace for V8
    // captureStackTrace is a V8-specific API, check existence at runtime
    const ErrorWithCapture = Error as typeof Error & {
      captureStackTrace?: (error: Error, constructor: Function) => void;
    };
    if (ErrorWithCapture.captureStackTrace) {
      ErrorWithCapture.captureStackTrace(this, JsonParseError);
    }
  }

  /**
   * Convert error to response format for WebSocket handlers
   */
  toResponse(): JsonErrorResponse {
    return {
      type: 'error',
      code: this.code,
      message: this.message,
    };
  }
}

// ============================================================================
// Main Function
// ============================================================================

/**
 * Safely parse JSON with protection against DoS attacks
 *
 * CRITICAL: Checks size BEFORE parsing to prevent memory exhaustion.
 *
 * @param input - JSON string to parse
 * @param options - Optional parsing limits
 * @returns Parsed value or JsonParseError
 *
 * @example
 * ```typescript
 * const result = safeJsonParse<{ type: string }>(message);
 * if (result instanceof JsonParseError) {
 *   ws.send(JSON.stringify(result.toResponse()));
 *   return;
 * }
 * // result is now typed as { type: string }
 * ```
 */
export function safeJsonParse<T = unknown>(
  input: string,
  options?: JsonParseOptions
): T | JsonParseError {
  const maxSize = options?.maxSize ?? DEFAULT_MAX_SIZE;
  const maxDepth = options?.maxDepth ?? DEFAULT_MAX_DEPTH;
  const maxKeys = options?.maxKeys ?? DEFAULT_MAX_KEYS;

  // 1. Validate input type
  if (typeof input !== 'string') {
    return new JsonParseError(
      JsonParseErrorCode.PARSE_ERROR,
      'Invalid JSON: input must be a string'
    );
  }

  // 2. Check size BEFORE parsing (critical for memory protection)
  if (input.length > maxSize) {
    return new JsonParseError(
      JsonParseErrorCode.SIZE_EXCEEDED,
      `Message size (${input.length} bytes) exceeds maximum allowed size (${maxSize} bytes)`
    );
  }

  // 3. Handle empty input
  if (input.length === 0) {
    return new JsonParseError(
      JsonParseErrorCode.PARSE_ERROR,
      'Invalid JSON: empty input'
    );
  }

  // 4. Parse JSON
  let parsed: unknown;
  try {
    parsed = JSON.parse(input);
  } catch (e) {
    const message = e instanceof Error ? e.message : 'Unknown parse error';
    return new JsonParseError(
      JsonParseErrorCode.PARSE_ERROR,
      `Invalid JSON: ${message}`
    );
  }

  // 5. Validate depth and key count
  const validationError = validateStructure(parsed, maxDepth, maxKeys);
  if (validationError) {
    return validationError;
  }

  return parsed as T;
}

// ============================================================================
// Structure Validation
// ============================================================================

/**
 * Validate parsed JSON structure for depth and key count limits
 *
 * Uses iterative approach with explicit stack to avoid call stack overflow
 * when validating deeply nested structures.
 */
function validateStructure(
  value: unknown,
  maxDepth: number,
  maxKeys: number
): JsonParseError | null {
  // Track total keys across all objects
  let totalKeys = 0;

  // Use explicit stack to avoid call stack overflow
  // Each item: [value, currentDepth]
  const stack: Array<[unknown, number]> = [[value, 1]];

  while (stack.length > 0) {
    const [current, depth] = stack.pop()!;

    // Check depth
    if (depth > maxDepth) {
      return new JsonParseError(
        JsonParseErrorCode.DEPTH_EXCEEDED,
        `JSON nesting depth exceeds maximum allowed depth (${maxDepth})`
      );
    }

    // Handle arrays
    if (Array.isArray(current)) {
      for (const item of current) {
        if (isObject(item) || Array.isArray(item)) {
          stack.push([item, depth + 1]);
        }
      }
      continue;
    }

    // Handle objects
    if (isObject(current)) {
      const keys = Object.keys(current);
      totalKeys += keys.length;

      // Check key count
      if (totalKeys > maxKeys) {
        return new JsonParseError(
          JsonParseErrorCode.KEYS_EXCEEDED,
          `Total key count (${totalKeys}) exceeds maximum allowed key count (${maxKeys})`
        );
      }

      // Add nested values to stack
      for (const key of keys) {
        const val = (current as Record<string, unknown>)[key];
        if (isObject(val) || Array.isArray(val)) {
          stack.push([val, depth + 1]);
        }
      }
    }
  }

  return null;
}

/**
 * Type guard for plain objects (not arrays, null, etc.)
 */
function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

// ============================================================================
// Convenience Functions
// ============================================================================

/**
 * Check if a result is a JsonParseError
 */
export function isJsonParseError(value: unknown): value is JsonParseError {
  return value instanceof JsonParseError;
}

/**
 * Create a typed result handler for safeJsonParse
 *
 * @example
 * ```typescript
 * const result = safeJsonParse<MyType>(input);
 * handleJsonResult(result, {
 *   onSuccess: (data) => { ... },
 *   onError: (error) => { ... }
 * });
 * ```
 */
export function handleJsonResult<T>(
  result: T | JsonParseError,
  handlers: {
    onSuccess: (data: T) => void;
    onError: (error: JsonParseError) => void;
  }
): void {
  if (result instanceof JsonParseError) {
    handlers.onError(result);
  } else {
    handlers.onSuccess(result);
  }
}
