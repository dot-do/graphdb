/**
 * Core module exports for GraphDB
 *
 * Re-exports all types, type guards, and utilities from:
 * - env.ts: Env interface for worker bindings
 * - types.ts: ObjectType enum and branded types
 * - geo.ts: Geospatial types and geohash functions
 *
 * @example
 * ```typescript
 * import {
 *   ObjectType,
 *   createEntityId,
 *   encodeGeohash,
 *   fnv1aHash,
 * } from '@dotdo/graphdb/core';
 *
 * // Create type-safe entity ID
 * const id = createEntityId('https://example.com/user/123');
 *
 * // Encode geohash for spatial queries
 * const hash = encodeGeohash(37.7749, -122.4194, 8);
 * ```
 *
 * @packageDocumentation
 */

// Environment bindings interface
/** Environment bindings interface for Cloudflare Workers */
export type { Env } from './env';

// Types and branded types
export {
  /**
   * Enum for triple object types in the typed object column schema.
   * Maps to the obj_type column in the triples table.
   */
  ObjectType,
  type EntityId,
  type Predicate,
  type Namespace,
  type TransactionId,
  /**
   * Type guard to check if a string is a valid EntityId.
   * @param value - String to check
   * @returns True if the value is a valid URL-based entity ID
   */
  isEntityId,
  /**
   * Create a branded EntityId from a URL string.
   * @param url - URL string for the entity
   * @returns Branded EntityId
   * @throws {Error} If the URL is invalid
   */
  createEntityId,
  /**
   * Type guard to check if a string is a valid Predicate.
   * @param value - String to check
   * @returns True if the value is a valid predicate name
   */
  isPredicate,
  /**
   * Create a branded Predicate from a string.
   * @param name - Predicate name
   * @returns Branded Predicate
   */
  createPredicate,
  /**
   * Type guard to check if a string is a valid Namespace.
   * @param value - String to check
   * @returns True if the value is a valid namespace URL
   */
  isNamespace,
  /**
   * Create a branded Namespace from a URL string.
   * @param url - Namespace URL
   * @returns Branded Namespace
   */
  createNamespace,
  /**
   * Type guard to check if a string is a valid TransactionId.
   * @param value - String to check
   * @returns True if the value is a valid transaction ID
   */
  isTransactionId,
  /**
   * Create a branded TransactionId from a string.
   * @param id - Transaction ID string
   * @returns Branded TransactionId
   */
  createTransactionId,
} from './types';

// Geospatial types and functions
export {
  type GeoPoint,
  type GeoPolygon,
  type GeoLineString,
  /**
   * Validate a GeoPoint object.
   * @param point - Object to validate
   * @returns True if valid GeoPoint with lat/lng in valid ranges
   */
  isValidGeoPoint,
  /**
   * Validate a GeoPolygon object.
   * @param polygon - Object to validate
   * @returns True if valid GeoPolygon with closed rings
   */
  isValidGeoPolygon,
  /**
   * Validate a GeoLineString object.
   * @param line - Object to validate
   * @returns True if valid GeoLineString with at least 2 points
   */
  isValidGeoLineString,
  /**
   * Encode lat/lng coordinates to a geohash string.
   * @param lat - Latitude (-90 to 90)
   * @param lng - Longitude (-180 to 180)
   * @param precision - Geohash precision (1-12, default 8)
   * @returns Geohash string
   * @example
   * ```typescript
   * const hash = encodeGeohash(37.7749, -122.4194, 8);
   * // Returns '9q8yyk8y' (San Francisco)
   * ```
   */
  encodeGeohash,
  /**
   * Decode a geohash string to lat/lng coordinates.
   * @param hash - Geohash string to decode
   * @returns Object with lat, lng, and error bounds
   */
  decodeGeohash,
  /**
   * Get the 8 neighboring geohash cells for a given geohash.
   * @param hash - Center geohash
   * @returns Array of 8 neighboring geohash strings
   */
  getGeohashNeighbors,
} from './geo';

// Hash functions for consistent hashing and bloom filters
export {
  /**
   * FNV-1a hash function for consistent hashing.
   * Used for shard routing and bloom filter hashing.
   * @param str - String to hash
   * @returns 32-bit unsigned integer hash
   */
  fnv1aHash,
  /**
   * Double FNV-1a hash for bloom filter bit positions.
   * Uses the formula: h1 + i * h2 for multiple hash values.
   * @param str - String to hash
   * @returns Tuple of [h1, h2] hash values
   */
  fnv1aDoubleHash,
  /**
   * Convert a hash value to a hex string.
   * @param hash - 32-bit hash value
   * @returns 8-character hex string (padded)
   */
  hashToHex,
} from './hash';

// Binary encoding utilities (varint, CRC32)
export {
  /**
   * Encode a number as a variable-length integer.
   * @param value - Non-negative integer to encode
   * @returns Uint8Array containing the varint bytes
   */
  encodeVarint,
  /**
   * Decode a variable-length integer from bytes.
   * @param bytes - Uint8Array containing varint
   * @param offset - Starting offset (default 0)
   * @returns Object with decoded value and bytes read
   */
  decodeVarint,
  /**
   * Calculate the byte size of a varint encoding.
   * @param value - Value to measure
   * @returns Number of bytes needed
   */
  varintSize,
  /**
   * Encode a signed integer as a zigzag-encoded varint.
   * @param value - Signed integer to encode
   * @returns Uint8Array containing the encoded bytes
   */
  encodeSignedVarint,
  /**
   * Decode a zigzag-encoded varint to a signed integer.
   * @param bytes - Uint8Array containing encoded value
   * @param offset - Starting offset (default 0)
   * @returns Object with decoded value and bytes read
   */
  decodeSignedVarint,
  /**
   * Calculate CRC32 checksum of data.
   * @param data - Data to checksum
   * @returns 32-bit CRC32 value
   */
  crc32,
  /** Maximum safe value for varint encoding (2^53 - 1) */
  MAX_SAFE_VARINT,
  /** Maximum bytes in a varint (10 for 64-bit values) */
  MAX_VARINT_BYTES,
} from './encoding';

// Validation functions
export {
  /** Maximum allowed length for entity IDs */
  MAX_ID_LENGTH,
  /**
   * Error thrown when entity ID validation fails.
   * Contains an error code for programmatic handling.
   */
  EntityIdValidationError,
  /** Error codes for entity ID validation failures */
  EntityIdErrorCode,
  /**
   * Validate an entity ID with detailed error reporting.
   * @param id - Entity ID to validate
   * @throws {EntityIdValidationError} If validation fails
   */
  validateEntityId,
  /**
   * Check if an entity ID has valid format without throwing.
   * @param id - Entity ID to check
   * @returns True if the format is valid
   */
  isValidEntityIdFormat,
} from './validation';

// Triple types and validation
export {
  type Triple,
  type TypedObject,
  // Individual TypedObject variant types for type narrowing
  type NullTypedObject,
  type BoolTypedObject,
  type Int32TypedObject,
  type Int64TypedObject,
  type Float64TypedObject,
  type StringTypedObject,
  type BinaryTypedObject,
  type TimestampTypedObject,
  type DateTypedObject,
  type DurationTypedObject,
  type RefTypedObject,
  type RefArrayTypedObject,
  type JsonTypedObject,
  type GeoPointTypedObject,
  type GeoPolygonTypedObject,
  type GeoLineStringTypedObject,
  type UrlTypedObject,
  type TripleValidationResult,
  /**
   * Validate a TypedObject value.
   * @param obj - Object to validate
   * @returns True if valid TypedObject with correct type/value pairing
   */
  isValidTypedObject,
  /**
   * Validate a complete Triple.
   * @param triple - Triple to validate
   * @returns Validation result with success flag and optional errors
   */
  validateTriple,
  /**
   * Create a new Triple from subject, predicate, and value.
   * @param subject - Entity ID of the subject
   * @param predicate - Predicate name
   * @param object - Typed object value
   * @returns New Triple instance
   */
  createTriple,
  /**
   * Infer ObjectType from a JavaScript value.
   * @param value - Value to analyze
   * @returns Inferred ObjectType enum value
   */
  inferObjectType,
  /**
   * Extract the raw value from a TypedObject.
   * @param obj - TypedObject to extract from
   * @returns The underlying JavaScript value
   */
  extractValue,
} from './triple';

// Entity interface and URL utilities
export {
  type Entity,
  type TripleValue,
  /**
   * Check if a field name is valid (no colons, reserved prefixes).
   * @param name - Field name to check
   * @returns True if valid field name
   */
  isValidFieldName,
  /**
   * Convert a URL-based entity ID to a storage path.
   * @param url - Entity ID URL
   * @returns Storage path string
   */
  urlToStoragePath,
  /**
   * Convert a storage path back to a URL-based entity ID.
   * @param path - Storage path
   * @returns Entity ID URL
   */
  storagePathToUrl,
  /**
   * Resolve namespace information from an entity ID.
   * @param entityId - Entity ID URL
   * @returns Object with namespace, context, and localId
   */
  resolveNamespace,
  /**
   * Parse an entity ID into its components.
   * @param entityId - Entity ID URL
   * @returns Parsed components (scheme, host, path segments)
   */
  parseEntityId,
  /**
   * Create a new Entity object from properties.
   * @param props - Entity properties including $id and $type
   * @returns New Entity instance
   */
  createEntity,
  /**
   * Validate an Entity object for required fields and structure.
   * @param entity - Entity to validate
   * @throws {Error} If validation fails
   */
  validateEntity,
} from './entity';

// Type conversion utilities
export {
  /**
   * Convert a TypedObject to a JSON-safe value.
   * @param obj - TypedObject to convert
   * @param options - Conversion options
   * @returns JSON-serializable value
   */
  typedObjectToJson,
  /**
   * Convert a JSON value to a TypedObject.
   * @param value - JSON value to convert
   * @param options - Conversion options
   * @returns TypedObject with inferred type
   */
  jsonToTypedObject,
  /**
   * Convert a TypedObject to SQL column values.
   * @param obj - TypedObject to convert
   * @returns Object with SQL column values
   */
  typedObjectToSqlValue,
  /**
   * Convert SQL row values to a TypedObject.
   * @param row - SQL row input
   * @param objectType - The ObjectType of the row
   * @returns TypedObject instance
   */
  sqlValueToTypedObject,
  /**
   * Extract a JSON value from a TypedObject.
   * @param obj - TypedObject to extract from
   * @returns Raw JSON value
   */
  extractJsonValue,
  /**
   * Get the SQL column name for an ObjectType.
   * @param type - ObjectType enum value
   * @returns Column name (e.g., 'obj_string', 'obj_int64')
   */
  getValueColumn,
  /**
   * Infer ObjectType from a raw JavaScript value.
   * @param value - Value to analyze
   * @returns Inferred ObjectType
   */
  inferObjectTypeFromValue,
  /**
   * Get a human-readable name for an ObjectType.
   * @param type - ObjectType enum value
   * @returns Human-readable type name
   */
  getObjectTypeName,
  type JsonTypedObjectValue,
  type JsonConversionOptions,
  type SqlObjectColumns,
  type SqlRowInput,
} from './type-converters';

// ============================================================================
// Workers-Compatible Binary Utilities
// ============================================================================
// These functions provide Buffer-free binary operations for Cloudflare Workers

/**
 * Encode a string to Uint8Array using TextEncoder
 * Workers-compatible alternative to Buffer.from(str)
 */
export function encodeString(str: string): Uint8Array {
  return new TextEncoder().encode(str);
}

/**
 * Decode a Uint8Array to string using TextDecoder
 * Workers-compatible alternative to Buffer.toString('utf-8')
 */
export function decodeString(bytes: Uint8Array): string {
  return new TextDecoder().decode(bytes);
}

/**
 * Encode Uint8Array to base64 string
 * Workers-compatible alternative to Buffer.toString('base64')
 */
export function toBase64(bytes: Uint8Array): string {
  if (bytes.length === 0) return '';

  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  let result = '';
  let i = 0;

  while (i < bytes.length) {
    const a = bytes[i++] || 0;
    const b = bytes[i++] || 0;
    const c = bytes[i++] || 0;

    const triplet = (a << 16) | (b << 8) | c;

    result +=
      (chars[(triplet >> 18) & 0x3f] ?? '') +
      (chars[(triplet >> 12) & 0x3f] ?? '') +
      (chars[(triplet >> 6) & 0x3f] ?? '') +
      (chars[triplet & 0x3f] ?? '');
  }

  // Add padding
  const padding = bytes.length % 3;
  if (padding === 1) {
    result = result.slice(0, -2) + '==';
  } else if (padding === 2) {
    result = result.slice(0, -1) + '=';
  }

  return result;
}

/**
 * Decode base64 string to Uint8Array
 * Workers-compatible alternative to Buffer.from(str, 'base64')
 */
export function fromBase64(str: string): Uint8Array {
  if (str.length === 0) return new Uint8Array(0);

  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  const lookup = new Uint8Array(128);
  for (let i = 0; i < chars.length; i++) {
    lookup[chars.charCodeAt(i)] = i;
  }

  // Remove padding and calculate output length
  let len = str.length;
  while (str[len - 1] === '=') len--;
  const outLen = (len * 6) >> 3;

  const out = new Uint8Array(outLen);
  let bits = 0;
  let value = 0;
  let outIdx = 0;

  for (let i = 0; i < len; i++) {
    value = (value << 6) | (lookup[str.charCodeAt(i)] ?? 0);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out[outIdx++] = (value >> bits) & 0xff;
    }
  }

  return out;
}

// Aliases for backward compatibility with snippet/bloom.ts naming convention
export { toBase64 as encodeBase64 };
export { fromBase64 as decodeBase64 };
