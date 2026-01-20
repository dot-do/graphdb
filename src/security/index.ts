/**
 * Security Module
 *
 * Exports security-related utilities for preventing injection attacks
 * and ensuring safe handling of user input.
 */

export {
  sanitizeFtsQuery,
  isValidFtsQuery,
  FtsSanitizationError,
} from './fts-sanitizer.js';

export {
  safeJsonParse,
  JsonParseError,
  JsonParseErrorCode,
  isJsonParseError,
  handleJsonResult,
  DEFAULT_MAX_SIZE,
  DEFAULT_MAX_DEPTH,
  DEFAULT_MAX_KEYS,
} from './json-validator.js';

export type { JsonParseOptions, JsonErrorResponse } from './json-validator.js';

export {
  validateEntityId,
  isValidEntityIdFormat,
  EntityIdValidationError,
  EntityIdErrorCode,
  MAX_ID_LENGTH,
} from './entity-validator.js';

export {
  createRateLimiter,
} from './rate-limiter.js';

export type {
  RateLimiterConfig,
  RateLimiter,
  RateLimitResult,
} from './rate-limiter.js';

// Authentication & Authorization
export {
  validateApiKey,
  validateJwt,
  validateWorkerBinding,
  createAuthContext,
  AuthError,
  AuthErrorCode,
  extractBearerToken,
  extractApiKey,
  unauthorizedResponse,
  forbiddenResponse,
} from './auth.js';

export type {
  AuthContext,
  AuthResult,
  AuthConfig,
  ApiKeyConfig,
  ApiKeyEntry,
  JwtConfig,
  WorkerBindingConfig,
  AuthErrorResponse,
} from './auth.js';

// Permissions
export {
  checkPermission,
  checkNamespaceAccess,
  checkEntityAccess,
  createPermissionContext,
  hasReadPermission,
  hasWritePermission,
  hasInternalPermission,
  createEntityACL,
  isDenied,
  isAllowed,
  mergePermissionResults,
  PermissionDeniedReason,
} from './permissions.js';

export type {
  Permission,
  PermissionCheckResult,
  PermissionContext,
  EntityACL,
} from './permissions.js';
