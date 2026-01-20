/**
 * Core Validation Module
 *
 * Contains foundational validation logic for entity IDs and other core types.
 * This module is part of core/ and has no dependencies on higher-level modules.
 *
 * Security considerations:
 * - Length limits prevent memory exhaustion attacks
 * - Character validation prevents injection and parsing issues
 * - URL format enforcement ensures proper handling downstream
 */

/**
 * Maximum length for entity IDs (2048 characters)
 *
 * This limit is based on:
 * - Common URL length limits in browsers (2048-8192)
 * - Database column size considerations
 * - Prevention of memory-based DoS attacks
 */
export const MAX_ID_LENGTH = 2048;

/**
 * Error thrown when entity ID validation fails
 */
export class EntityIdValidationError extends Error {
  constructor(
    message: string,
    public readonly code: EntityIdErrorCode
  ) {
    super(message);
    this.name = 'EntityIdValidationError';
  }
}

/**
 * Error codes for entity ID validation failures
 */
export enum EntityIdErrorCode {
  /** ID is empty or only whitespace */
  EMPTY = 'EMPTY',
  /** ID exceeds maximum length */
  TOO_LONG = 'TOO_LONG',
  /** ID contains invalid characters */
  INVALID_CHARACTERS = 'INVALID_CHARACTERS',
  /** ID is not a valid URL */
  INVALID_URL = 'INVALID_URL',
  /** ID uses non-http(s) protocol */
  INVALID_PROTOCOL = 'INVALID_PROTOCOL',
  /** ID has invalid hostname */
  INVALID_HOSTNAME = 'INVALID_HOSTNAME',
  /** ID contains user info (security risk) */
  HAS_USER_INFO = 'HAS_USER_INFO',
}

/**
 * Characters that are not allowed in entity IDs
 *
 * Includes:
 * - Null bytes (\x00)
 * - Control characters (0x00-0x1F, 0x7F)
 * - Zero-width characters (U+200B-U+200D, U+FEFF)
 * - Soft hyphen (U+00AD)
 * - Unicode replacement character (U+FFFD)
 */
const INVALID_CHARS_PATTERN =
  /[\x00-\x1F\x7F\u200B-\u200D\uFEFF\u00AD\uFFFD]/;

/**
 * Validates an entity ID and returns it if valid.
 *
 * Performs the following checks:
 * 1. Non-empty string
 * 2. Length within MAX_ID_LENGTH
 * 3. No invalid characters (null bytes, control chars, zero-width)
 * 4. Valid URL format
 * 5. HTTP or HTTPS protocol only
 * 6. Valid hostname (not empty or just dots)
 * 7. No user info (user:pass@) in URL
 *
 * @param id - The entity ID to validate
 * @returns The validated entity ID (unchanged if valid)
 * @throws EntityIdValidationError if validation fails
 */
export function validateEntityId(id: string): string {
  // Check for null/undefined/non-string
  if (id === null || id === undefined || typeof id !== 'string') {
    throw new EntityIdValidationError(
      'Entity ID must be a non-null string',
      EntityIdErrorCode.EMPTY
    );
  }

  // Check for empty or whitespace-only
  const trimmed = id.trim();
  if (trimmed.length === 0) {
    throw new EntityIdValidationError(
      'Entity ID cannot be empty or whitespace-only',
      EntityIdErrorCode.EMPTY
    );
  }

  // Check length limit BEFORE any parsing to prevent DoS
  if (id.length > MAX_ID_LENGTH) {
    throw new EntityIdValidationError(
      `Entity ID exceeds maximum length of ${MAX_ID_LENGTH} characters (got ${id.length})`,
      EntityIdErrorCode.TOO_LONG
    );
  }

  // Check for invalid characters
  if (INVALID_CHARS_PATTERN.test(id)) {
    throw new EntityIdValidationError(
      'Entity ID contains invalid characters (null bytes, control characters, or zero-width characters)',
      EntityIdErrorCode.INVALID_CHARACTERS
    );
  }

  // Parse as URL
  let url: URL;
  try {
    url = new URL(id);
  } catch {
    throw new EntityIdValidationError(
      `Entity ID must be a valid URL: "${truncateForError(id)}"`,
      EntityIdErrorCode.INVALID_URL
    );
  }

  // Check protocol (only http and https allowed)
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new EntityIdValidationError(
      `Entity ID must use http:// or https:// protocol, got: "${url.protocol}"`,
      EntityIdErrorCode.INVALID_PROTOCOL
    );
  }

  // Check hostname validity
  if (!url.hostname || url.hostname === '.' || url.hostname === '..') {
    throw new EntityIdValidationError(
      'Entity ID must have a valid hostname',
      EntityIdErrorCode.INVALID_HOSTNAME
    );
  }

  // Check for user info (user:pass@host) - security risk
  if (url.username || url.password) {
    throw new EntityIdValidationError(
      'Entity ID must not contain user credentials (user:pass@)',
      EntityIdErrorCode.HAS_USER_INFO
    );
  }

  return id;
}

/**
 * Checks if an entity ID is valid without throwing.
 *
 * Use this for conditional validation where you don't need
 * detailed error information.
 *
 * @param id - The entity ID to check
 * @returns true if the ID is valid, false otherwise
 */
export function isValidEntityIdFormat(id: string): boolean {
  // Handle null/undefined/non-string
  if (id === null || id === undefined || typeof id !== 'string') {
    return false;
  }

  try {
    validateEntityId(id);
    return true;
  } catch {
    return false;
  }
}

/**
 * Truncates a string for safe inclusion in error messages.
 * Prevents overly long error messages from large inputs.
 */
function truncateForError(str: string, maxLength: number = 100): string {
  if (str.length <= maxLength) {
    return str;
  }
  return str.slice(0, maxLength) + '...[truncated]';
}
