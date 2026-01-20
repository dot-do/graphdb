/**
 * Core type definitions for GraphDB
 *
 * ObjectType enum matching SCHEMA_DESIGN.md
 * Branded types for type safety
 */

import {
  validateEntityId,
  isValidEntityIdFormat,
  MAX_ID_LENGTH,
} from './validation.js';

/**
 * ObjectType enum - represents the type of a triple's object value.
 * Values 0-16 match the schema design specification.
 *
 * @example
 * ```typescript
 * const obj = { type: ObjectType.STRING, value: "hello" };
 * if (obj.type === ObjectType.REF) {
 *   console.log("Reference to:", obj.value);
 * }
 * ```
 */
export enum ObjectType {
  NULL = 0,
  BOOL = 1,
  INT32 = 2,
  INT64 = 3,
  FLOAT64 = 4,
  STRING = 5,
  BINARY = 6,
  TIMESTAMP = 7,
  DATE = 8,
  DURATION = 9,
  REF = 10,
  REF_ARRAY = 11,
  JSON = 12,
  GEO_POINT = 13,
  GEO_POLYGON = 14,
  GEO_LINESTRING = 15,
  URL = 16,
  VECTOR = 17,
}

// ============================================================================
// Branded Types
// ============================================================================

/**
 * EntityId - URL-based identifier for entities ($id).
 * Must be a valid http:// or https:// URL.
 *
 * This is a branded type that provides compile-time safety for entity identifiers.
 * Use createEntityId() to create instances from raw strings.
 *
 * @example
 * ```typescript
 * const id = createEntityId("https://example.com/users/123");
 * ```
 */
export type EntityId = string & { readonly __brand: 'EntityId' };

/**
 * Predicate - field name for a triple.
 * MUST NOT contain colons (JS/TS native naming, no RDF prefixes).
 *
 * This is a branded type that provides compile-time safety for predicate names.
 * Use createPredicate() to create instances from raw strings.
 *
 * @example
 * ```typescript
 * const pred = createPredicate("friends");
 * // Throws: createPredicate("schema:name") - colons not allowed
 * ```
 */
export type Predicate = string & { readonly __brand: 'Predicate' };

/**
 * Namespace - URL-based namespace for entities.
 * Must be a valid http:// or https:// URL.
 *
 * Used for grouping entities and determining shard routing.
 *
 * @example
 * ```typescript
 * const ns = createNamespace("https://example.com/crm/");
 * ```
 */
export type Namespace = string & { readonly __brand: 'Namespace' };

/**
 * TransactionId - ULID-format transaction identifier.
 * 26-character Crockford Base32 string.
 *
 * ULIDs provide lexicographically sortable unique identifiers
 * with embedded timestamps for transaction ordering.
 *
 * @example
 * ```typescript
 * const txId = createTransactionId("01ARZ3NDEKTSV4RRFFQ69G5FAV");
 * ```
 */
export type TransactionId = string & { readonly __brand: 'TransactionId' };

// ============================================================================
// Type Guards
// ============================================================================

/**
 * Valid characters for predicates: alphanumeric, underscore, and $ prefix
 * NO colons, spaces, or other special characters
 */
const PREDICATE_PATTERN = /^[$a-zA-Z_][a-zA-Z0-9_$]*$/;

/**
 * ULID pattern: 26 characters of Crockford Base32
 * Valid chars: 0123456789ABCDEFGHJKMNPQRSTVWXYZ (no I, L, O, U)
 */
const ULID_PATTERN = /^[0-9A-HJKMNP-TV-Z]{26}$/;

/**
 * Check if a string is a valid EntityId (http/https URL).
 *
 * This function also enforces security constraints:
 * - Maximum length of MAX_ID_LENGTH (2048) characters
 * - No invalid characters (null bytes, control chars, zero-width)
 * - Valid URL format with http/https protocol
 *
 * @param value - The string to validate
 * @returns True if the string is a valid EntityId
 * @example
 * ```typescript
 * if (isEntityId(userInput)) {
 *   // TypeScript now knows userInput is EntityId
 *   const entity = await getEntity(userInput);
 * }
 * ```
 */
export function isEntityId(value: string): value is EntityId {
  // Use the security validator for comprehensive checks
  return isValidEntityIdFormat(value);
}

/**
 * Create an EntityId from a URL string.
 *
 * Performs security validation including:
 * - Length limit enforcement (MAX_ID_LENGTH = 2048 characters)
 * - Invalid character rejection (null bytes, control chars, zero-width)
 * - URL format validation (http/https only)
 *
 * @param url - The URL string to convert to an EntityId
 * @returns A validated EntityId
 * @throws EntityIdValidationError if the URL is invalid, too long, or contains invalid characters
 * @example
 * ```typescript
 * const id = createEntityId("https://example.com/users/123");
 * ```
 */
export function createEntityId(url: string): EntityId {
  // Use validator which throws on invalid input
  validateEntityId(url);
  return url as EntityId;
}

// Re-export MAX_ID_LENGTH for use by other modules
export { MAX_ID_LENGTH };

/**
 * Check if a string is a valid Predicate (no colons, valid JS identifier-like).
 *
 * Predicates must:
 * - Not contain colons (no RDF prefixes)
 * - Not contain whitespace
 * - Match valid JS identifier pattern (alphanumeric, underscore, $ prefix)
 *
 * @param value - The string to validate
 * @returns True if the string is a valid Predicate
 * @example
 * ```typescript
 * isPredicate("friends")     // true
 * isPredicate("$type")       // true
 * isPredicate("schema:name") // false - contains colon
 * ```
 */
export function isPredicate(value: string): value is Predicate {
  if (!value || typeof value !== 'string') {
    return false;
  }

  // Must not contain colons (no RDF prefixes)
  if (value.includes(':')) {
    return false;
  }

  // Must not contain whitespace
  if (/\s/.test(value)) {
    return false;
  }

  // Must match valid predicate pattern
  return PREDICATE_PATTERN.test(value);
}

/**
 * Create a Predicate from a string.
 *
 * @param name - The predicate name to validate and convert
 * @returns A validated Predicate
 * @throws Error if the string contains colons, whitespace, or invalid characters
 * @example
 * ```typescript
 * const pred = createPredicate("friends");
 * // Throws: createPredicate("schema:name")
 * ```
 */
export function createPredicate(name: string): Predicate {
  if (!isPredicate(name)) {
    if (name.includes(':')) {
      throw new Error(
        `Invalid Predicate: "${name}". Predicates must NOT contain colons (no RDF prefixes).`
      );
    }
    if (/\s/.test(name)) {
      throw new Error(
        `Invalid Predicate: "${name}". Predicates must not contain whitespace.`
      );
    }
    throw new Error(
      `Invalid Predicate: "${name}". Must be a valid JS identifier-like name.`
    );
  }
  return name as Predicate;
}

/**
 * Check if a string is a valid Namespace (http/https URL).
 *
 * @param value - The string to validate
 * @returns True if the string is a valid Namespace
 * @example
 * ```typescript
 * isNamespace("https://example.com/") // true
 * isNamespace("ftp://example.com/")   // false - not http/https
 * ```
 */
export function isNamespace(value: string): value is Namespace {
  if (!value || typeof value !== 'string') {
    return false;
  }

  try {
    const url = new URL(value);
    // Only allow http and https protocols
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * Create a Namespace from a URL string.
 *
 * @param url - The URL string to convert to a Namespace
 * @returns A validated Namespace
 * @throws Error if the URL is invalid or not http/https
 * @example
 * ```typescript
 * const ns = createNamespace("https://example.com/crm/");
 * ```
 */
export function createNamespace(url: string): Namespace {
  if (!isNamespace(url)) {
    throw new Error(
      `Invalid Namespace: "${url}". Must be a valid http:// or https:// URL.`
    );
  }
  return url as Namespace;
}

/**
 * Check if a string is a valid TransactionId (ULID format).
 * ULIDs are 26-character Crockford Base32 strings.
 *
 * @param value - The string to validate
 * @returns True if the string is a valid TransactionId
 * @example
 * ```typescript
 * isTransactionId("01ARZ3NDEKTSV4RRFFQ69G5FAV") // true
 * isTransactionId("invalid")                    // false
 * ```
 */
export function isTransactionId(value: string): value is TransactionId {
  if (!value || typeof value !== 'string') {
    return false;
  }

  // Must be exactly 26 characters and match ULID pattern
  return ULID_PATTERN.test(value);
}

/**
 * Create a TransactionId from a string.
 *
 * @param id - The ULID string to convert to a TransactionId
 * @returns A validated TransactionId
 * @throws Error if the string is not a valid ULID (26-char Crockford Base32)
 * @example
 * ```typescript
 * const txId = createTransactionId("01ARZ3NDEKTSV4RRFFQ69G5FAV");
 * ```
 */
export function createTransactionId(id: string): TransactionId {
  if (!isTransactionId(id)) {
    throw new Error(
      `Invalid TransactionId: "${id}". Must be a 26-character ULID (Crockford Base32).`
    );
  }
  return id as TransactionId;
}

// ============================================================================
// Validation Error Classes
// ============================================================================

/**
 * Error codes for branded type validation failures
 */
export enum BrandedTypeErrorCode {
  /** Value is not a valid EntityId */
  INVALID_ENTITY_ID = 'INVALID_ENTITY_ID',
  /** Value is not a valid Predicate */
  INVALID_PREDICATE = 'INVALID_PREDICATE',
  /** Value is not a valid Namespace */
  INVALID_NAMESPACE = 'INVALID_NAMESPACE',
  /** Value is not a valid TransactionId */
  INVALID_TRANSACTION_ID = 'INVALID_TRANSACTION_ID',
}

/**
 * Error thrown when branded type validation fails.
 * Provides detailed information about the validation failure.
 */
export class BrandedTypeValidationError extends Error {
  constructor(
    message: string,
    public readonly code: BrandedTypeErrorCode,
    public readonly value: unknown
  ) {
    super(message);
    this.name = 'BrandedTypeValidationError';
  }
}

// ============================================================================
// Assertion Functions for Runtime Validation at Boundaries
// ============================================================================

/**
 * Truncates a value for safe inclusion in error messages.
 * Prevents overly long error messages from large inputs.
 */
function truncateValue(value: unknown, maxLength: number = 100): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';

  const str = typeof value === 'string' ? value : String(value);
  if (str.length <= maxLength) {
    return str;
  }
  return str.slice(0, maxLength) + '...[truncated]';
}

/**
 * Assert that a value is a valid EntityId.
 * Use this at API boundaries to validate untrusted input.
 *
 * Unlike createEntityId(), this function:
 * - Throws a BrandedTypeValidationError with detailed context
 * - Is designed for use at entry points where security matters
 *
 * @param value - The value to validate
 * @param fieldName - Optional field name for better error messages
 * @returns A validated EntityId
 * @throws BrandedTypeValidationError if the value is not a valid EntityId
 * @example
 * ```typescript
 * // At API boundary
 * const entityId = assertEntityId(request.body.subject, 'subject');
 * ```
 */
export function assertEntityId(value: unknown, fieldName?: string): EntityId {
  if (typeof value !== 'string') {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid EntityId${field}: expected string, got ${typeof value}`,
      BrandedTypeErrorCode.INVALID_ENTITY_ID,
      value
    );
  }

  if (!isEntityId(value)) {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid EntityId${field}: "${truncateValue(value)}" is not a valid http:// or https:// URL`,
      BrandedTypeErrorCode.INVALID_ENTITY_ID,
      value
    );
  }

  return value;
}

/**
 * Assert that a value is a valid Predicate.
 * Use this at API boundaries to validate untrusted input.
 *
 * @param value - The value to validate
 * @param fieldName - Optional field name for better error messages
 * @returns A validated Predicate
 * @throws BrandedTypeValidationError if the value is not a valid Predicate
 * @example
 * ```typescript
 * // At API boundary
 * const pred = assertPredicate(request.body.predicate, 'predicate');
 * ```
 */
export function assertPredicate(value: unknown, fieldName?: string): Predicate {
  if (typeof value !== 'string') {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid Predicate${field}: expected string, got ${typeof value}`,
      BrandedTypeErrorCode.INVALID_PREDICATE,
      value
    );
  }

  if (!isPredicate(value)) {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    let reason = 'must be a valid JS identifier-like name';
    if (value.includes(':')) {
      reason = 'must NOT contain colons (no RDF prefixes)';
    } else if (/\s/.test(value)) {
      reason = 'must not contain whitespace';
    }
    throw new BrandedTypeValidationError(
      `Invalid Predicate${field}: "${truncateValue(value)}" ${reason}`,
      BrandedTypeErrorCode.INVALID_PREDICATE,
      value
    );
  }

  return value;
}

/**
 * Assert that a value is a valid Namespace.
 * Use this at API boundaries to validate untrusted input.
 *
 * @param value - The value to validate
 * @param fieldName - Optional field name for better error messages
 * @returns A validated Namespace
 * @throws BrandedTypeValidationError if the value is not a valid Namespace
 * @example
 * ```typescript
 * // At API boundary
 * const ns = assertNamespace(request.body.namespace, 'namespace');
 * ```
 */
export function assertNamespace(value: unknown, fieldName?: string): Namespace {
  if (typeof value !== 'string') {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid Namespace${field}: expected string, got ${typeof value}`,
      BrandedTypeErrorCode.INVALID_NAMESPACE,
      value
    );
  }

  if (!isNamespace(value)) {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid Namespace${field}: "${truncateValue(value)}" is not a valid http:// or https:// URL`,
      BrandedTypeErrorCode.INVALID_NAMESPACE,
      value
    );
  }

  return value;
}

/**
 * Assert that a value is a valid TransactionId.
 * Use this at API boundaries to validate untrusted input.
 *
 * @param value - The value to validate
 * @param fieldName - Optional field name for better error messages
 * @returns A validated TransactionId
 * @throws BrandedTypeValidationError if the value is not a valid TransactionId
 * @example
 * ```typescript
 * // At API boundary
 * const txId = assertTransactionId(request.body.txId, 'txId');
 * ```
 */
export function assertTransactionId(value: unknown, fieldName?: string): TransactionId {
  if (typeof value !== 'string') {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid TransactionId${field}: expected string, got ${typeof value}`,
      BrandedTypeErrorCode.INVALID_TRANSACTION_ID,
      value
    );
  }

  if (!isTransactionId(value)) {
    const field = fieldName ? ` for field "${fieldName}"` : '';
    throw new BrandedTypeValidationError(
      `Invalid TransactionId${field}: "${truncateValue(value)}" is not a valid 26-character ULID (Crockford Base32)`,
      BrandedTypeErrorCode.INVALID_TRANSACTION_ID,
      value
    );
  }

  return value;
}

/**
 * Assert that a value is a valid array of EntityIds.
 * Use this at API boundaries to validate untrusted input.
 *
 * @param value - The value to validate
 * @param fieldName - Optional field name for better error messages
 * @returns A validated array of EntityIds
 * @throws BrandedTypeValidationError if any value in the array is not a valid EntityId
 * @example
 * ```typescript
 * // At API boundary
 * const entityIds = assertEntityIdArray(request.body.ids, 'ids');
 * ```
 */
export function assertEntityIdArray(value: unknown, fieldName?: string): EntityId[] {
  const field = fieldName ? ` for field "${fieldName}"` : '';

  if (!Array.isArray(value)) {
    throw new BrandedTypeValidationError(
      `Invalid EntityId array${field}: expected array, got ${typeof value}`,
      BrandedTypeErrorCode.INVALID_ENTITY_ID,
      value
    );
  }

  const result: EntityId[] = [];
  for (let i = 0; i < value.length; i++) {
    const element = value[i];
    if (typeof element !== 'string') {
      throw new BrandedTypeValidationError(
        `Invalid EntityId${field}[${i}]: expected string, got ${typeof element}`,
        BrandedTypeErrorCode.INVALID_ENTITY_ID,
        element
      );
    }
    if (!isEntityId(element)) {
      throw new BrandedTypeValidationError(
        `Invalid EntityId${field}[${i}]: "${truncateValue(element)}" is not a valid http:// or https:// URL`,
        BrandedTypeErrorCode.INVALID_ENTITY_ID,
        element
      );
    }
    result.push(element);
  }

  return result;
}
