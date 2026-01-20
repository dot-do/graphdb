/**
 * Triple interface and validation for GraphDB
 *
 * Defines the core Triple structure and TypedObject discriminated union
 * for storing typed values in the graph database.
 */

import type { EntityId, Predicate, TransactionId } from './types';
import { ObjectType, isEntityId, isPredicate, isTransactionId } from './types';
import type { GeoPoint, GeoPolygon, GeoLineString } from './geo';
import { isValidGeoPoint, isValidGeoPolygon, isValidGeoLineString } from './geo';

// ============================================================================
// TypedObject Discriminated Union
// ============================================================================

/**
 * Individual TypedObject variants for each ObjectType.
 * These enable proper type narrowing when switching on the type field.
 */

export interface NullTypedObject {
  type: ObjectType.NULL;
}

export interface BoolTypedObject {
  type: ObjectType.BOOL;
  value: boolean;
}

export interface Int32TypedObject {
  type: ObjectType.INT32;
  value: bigint;
}

export interface Int64TypedObject {
  type: ObjectType.INT64;
  value: bigint;
}

export interface Float64TypedObject {
  type: ObjectType.FLOAT64;
  value: number;
}

export interface StringTypedObject {
  type: ObjectType.STRING;
  value: string;
}

export interface BinaryTypedObject {
  type: ObjectType.BINARY;
  value: Uint8Array;
}

export interface TimestampTypedObject {
  type: ObjectType.TIMESTAMP;
  value: bigint; // milliseconds since epoch
}

export interface DateTypedObject {
  type: ObjectType.DATE;
  value: number; // days since epoch
}

export interface DurationTypedObject {
  type: ObjectType.DURATION;
  value: string; // ISO 8601 duration
}

export interface RefTypedObject {
  type: ObjectType.REF;
  value: EntityId;
}

export interface RefArrayTypedObject {
  type: ObjectType.REF_ARRAY;
  value: EntityId[];
}

export interface JsonTypedObject {
  type: ObjectType.JSON;
  value: unknown;
}

export interface GeoPointTypedObject {
  type: ObjectType.GEO_POINT;
  value: GeoPoint;
}

export interface GeoPolygonTypedObject {
  type: ObjectType.GEO_POLYGON;
  value: GeoPolygon;
}

export interface GeoLineStringTypedObject {
  type: ObjectType.GEO_LINESTRING;
  value: GeoLineString;
}

export interface UrlTypedObject {
  type: ObjectType.URL;
  value: string;
}

export interface VectorTypedObject {
  type: ObjectType.VECTOR;
  value: number[];
}

/**
 * TypedObject - discriminated union of all typed object variants.
 *
 * This is the core value type used in triples, providing type safety
 * through discriminated unions. Use switch on the `type` field to narrow
 * to specific variant and access `value` with proper typing.
 *
 * @example
 * ```typescript
 * function displayValue(obj: TypedObject): string {
 *   switch (obj.type) {
 *     case ObjectType.STRING:
 *       return obj.value; // TypeScript knows value is string
 *     case ObjectType.INT64:
 *       return obj.value.toString(); // TypeScript knows value is bigint
 *     case ObjectType.GEO_POINT:
 *       return `${obj.value.lat}, ${obj.value.lng}`;
 *     default:
 *       return "unknown";
 *   }
 * }
 * ```
 */
export type TypedObject =
  | NullTypedObject
  | BoolTypedObject
  | Int32TypedObject
  | Int64TypedObject
  | Float64TypedObject
  | StringTypedObject
  | BinaryTypedObject
  | TimestampTypedObject
  | DateTypedObject
  | DurationTypedObject
  | RefTypedObject
  | RefArrayTypedObject
  | JsonTypedObject
  | GeoPointTypedObject
  | GeoPolygonTypedObject
  | GeoLineStringTypedObject
  | UrlTypedObject
  | VectorTypedObject;

// ============================================================================
// Triple Interface
// ============================================================================

/**
 * Triple - the fundamental unit of data in the graph database.
 *
 * A triple represents a single fact: subject-predicate-object
 * along with metadata for versioning (timestamp, txId).
 *
 * Triples are immutable - updates create new triples with later timestamps.
 * The latest triple for a given subject+predicate pair is the current value.
 *
 * @example
 * ```typescript
 * const triple: Triple = {
 *   subject: createEntityId("https://example.com/users/123"),
 *   predicate: createPredicate("name"),
 *   object: { type: ObjectType.STRING, value: "Alice" },
 *   timestamp: BigInt(Date.now()),
 *   txId: createTransactionId("01ARZ3NDEKTSV4RRFFQ69G5FAV")
 * };
 * ```
 */
export interface Triple {
  subject: EntityId;
  predicate: Predicate;
  object: TypedObject;
  timestamp: bigint;
  txId: TransactionId;
}

// ============================================================================
// Validation Constants
// ============================================================================

/**
 * ISO 8601 duration pattern
 * Matches patterns like: P1Y2M3D, PT1H30M, P1Y2M3DT4H5M6S
 */
const ISO_8601_DURATION_PATTERN =
  /^P(?:\d+Y)?(?:\d+M)?(?:\d+W)?(?:\d+D)?(?:T(?:\d+H)?(?:\d+M)?(?:\d+(?:\.\d+)?S)?)?$/;

/**
 * 32-bit signed integer range
 */
const INT32_MIN = BigInt(-2147483648);
const INT32_MAX = BigInt(2147483647);

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Check if a string is a valid ISO 8601 duration
 */
function isValidDuration(value: string): boolean {
  if (!value || typeof value !== 'string') {
    return false;
  }
  return ISO_8601_DURATION_PATTERN.test(value);
}

/**
 * Check if a string is a valid URL (http/https)
 */
function isValidUrl(value: string): boolean {
  if (!value || typeof value !== 'string') {
    return false;
  }
  try {
    const url = new URL(value);
    return url.protocol === 'http:' || url.protocol === 'https:';
  } catch {
    return false;
  }
}

/**
 * Validate a TypedObject has the correct value for its type.
 *
 * Checks that the value field matches the expected type:
 * - STRING: value is string
 * - INT64: value is bigint
 * - FLOAT64: value is finite number
 * - REF: value is valid EntityId
 * - etc.
 *
 * @param obj - The TypedObject to validate
 * @returns True if the object is valid for its declared type
 * @example
 * ```typescript
 * const obj = { type: ObjectType.STRING, value: "hello" };
 * isValidTypedObject(obj); // true
 *
 * const invalid = { type: ObjectType.INT64, value: "not a bigint" };
 * isValidTypedObject(invalid as any); // false
 * ```
 */
export function isValidTypedObject(obj: TypedObject): boolean {
  if (!obj || typeof obj.type !== 'number') {
    return false;
  }

  switch (obj.type) {
    case ObjectType.NULL:
      return true;

    case ObjectType.BOOL:
      return typeof obj.value === 'boolean';

    case ObjectType.INT32:
      if (typeof obj.value !== 'bigint') {
        return false;
      }
      // Check 32-bit range
      return obj.value >= INT32_MIN && obj.value <= INT32_MAX;

    case ObjectType.INT64:
      return typeof obj.value === 'bigint';

    case ObjectType.FLOAT64:
      return typeof obj.value === 'number' && Number.isFinite(obj.value);

    case ObjectType.STRING:
      return typeof obj.value === 'string';

    case ObjectType.BINARY:
      return obj.value instanceof Uint8Array;

    case ObjectType.TIMESTAMP:
      return typeof obj.value === 'bigint' && obj.value >= 0n;

    case ObjectType.DATE:
      return typeof obj.value === 'number' && Number.isFinite(obj.value);

    case ObjectType.DURATION:
      return isValidDuration(obj.value ?? '');

    case ObjectType.REF:
      return isEntityId(obj.value ?? '');

    case ObjectType.REF_ARRAY:
      if (!Array.isArray(obj.value)) {
        return false;
      }
      return obj.value.every((ref) => isEntityId(ref));

    case ObjectType.JSON:
      // JSON type must have value explicitly set (even if null)
      return obj.value !== undefined;

    case ObjectType.GEO_POINT:
      return obj.value ? isValidGeoPoint(obj.value) : false;

    case ObjectType.GEO_POLYGON:
      return obj.value ? isValidGeoPolygon(obj.value) : false;

    case ObjectType.GEO_LINESTRING:
      return obj.value ? isValidGeoLineString(obj.value) : false;

    case ObjectType.URL:
      return isValidUrl(obj.value ?? '');

    case ObjectType.VECTOR:
      if (!Array.isArray(obj.value)) {
        return false;
      }
      return obj.value.every(
        (v) => typeof v === 'number' && Number.isFinite(v)
      );

    default:
      return false;
  }
}

/**
 * Validation result for a Triple.
 *
 * Contains the validation status and any error messages.
 */
export interface TripleValidationResult {
  /** Whether the triple passed all validation checks */
  valid: boolean;
  /** Array of error messages (empty if valid) */
  errors: string[];
}

/**
 * Validate a Triple has all required fields with correct values.
 *
 * Checks:
 * - subject is a valid EntityId
 * - predicate is a valid Predicate (no colons)
 * - object is a valid TypedObject
 * - timestamp is a positive bigint
 * - txId is a valid TransactionId (ULID format)
 *
 * @param triple - The Triple to validate
 * @returns Validation result with valid boolean and error messages
 * @example
 * ```typescript
 * const result = validateTriple(triple);
 * if (!result.valid) {
 *   console.error("Triple validation failed:", result.errors);
 * }
 * ```
 */
export function validateTriple(triple: Triple): TripleValidationResult {
  const errors: string[] = [];

  // Validate subject
  if (!triple.subject) {
    errors.push('subject is required');
  } else if (!isEntityId(triple.subject)) {
    errors.push('subject must be a valid EntityId (http/https URL)');
  }

  // Validate predicate
  if (!triple.predicate) {
    errors.push('predicate is required');
  } else if (triple.predicate.includes(':')) {
    errors.push('predicate must not contain colon (no RDF prefixes)');
  } else if (!isPredicate(triple.predicate)) {
    errors.push('predicate must be a valid identifier');
  }

  // Validate object
  if (!triple.object) {
    errors.push('object is required');
  } else if (!isValidTypedObject(triple.object)) {
    errors.push('object must be a valid TypedObject');
  }

  // Validate timestamp
  if (triple.timestamp === undefined || triple.timestamp === null) {
    errors.push('timestamp is required');
  } else if (typeof triple.timestamp !== 'bigint') {
    errors.push('timestamp must be a bigint');
  } else if (triple.timestamp < 0n) {
    errors.push('timestamp must be positive');
  }

  // Validate txId
  if (!triple.txId) {
    errors.push('txId is required');
  } else if (!isTransactionId(triple.txId)) {
    errors.push('txId must be a valid TransactionId (ULID format)');
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

// ============================================================================
// Type Inference
// ============================================================================

/**
 * Infer the ObjectType from a JavaScript value.
 *
 * Maps JavaScript types to ObjectType:
 * - null/undefined -> NULL
 * - boolean -> BOOL
 * - bigint -> INT64
 * - number -> FLOAT64
 * - string -> STRING
 * - Uint8Array -> BINARY
 * - Date -> TIMESTAMP
 * - object/array -> JSON
 *
 * @param value - The value to infer the type from
 * @returns The inferred ObjectType
 * @example
 * ```typescript
 * inferObjectType("hello")      // ObjectType.STRING
 * inferObjectType(42n)          // ObjectType.INT64
 * inferObjectType(3.14)         // ObjectType.FLOAT64
 * inferObjectType({ x: 1 })     // ObjectType.JSON
 * ```
 */
export function inferObjectType(value: unknown): ObjectType {
  if (value === null || value === undefined) {
    return ObjectType.NULL;
  }

  if (typeof value === 'boolean') {
    return ObjectType.BOOL;
  }

  if (typeof value === 'bigint') {
    return ObjectType.INT64;
  }

  if (typeof value === 'number') {
    return ObjectType.FLOAT64;
  }

  if (typeof value === 'string') {
    return ObjectType.STRING;
  }

  if (value instanceof Uint8Array) {
    return ObjectType.BINARY;
  }

  if (value instanceof Date) {
    return ObjectType.TIMESTAMP;
  }

  // Object or array -> JSON
  if (typeof value === 'object') {
    return ObjectType.JSON;
  }

  // Default fallback
  return ObjectType.JSON;
}

// ============================================================================
// Factory Functions
// ============================================================================

/**
 * Create a TypedObject from a JavaScript value
 *
 * @param value The value to wrap
 * @param type Optional explicit type (inferred if not provided)
 * @returns A TypedObject wrapping the value
 */
function createTypedObject(value: unknown, type?: ObjectType): TypedObject {
  const inferredType = type ?? inferObjectType(value);

  switch (inferredType) {
    case ObjectType.NULL:
      return { type: ObjectType.NULL };

    case ObjectType.BOOL:
      return { type: ObjectType.BOOL, value: value as boolean };

    case ObjectType.INT32:
      return { type: ObjectType.INT32, value: value as bigint };

    case ObjectType.INT64:
      return { type: ObjectType.INT64, value: value as bigint };

    case ObjectType.FLOAT64:
      return { type: ObjectType.FLOAT64, value: value as number };

    case ObjectType.STRING:
      return { type: ObjectType.STRING, value: value as string };

    case ObjectType.BINARY:
      return { type: ObjectType.BINARY, value: value as Uint8Array };

    case ObjectType.TIMESTAMP:
      if (value instanceof Date) {
        return {
          type: ObjectType.TIMESTAMP,
          value: BigInt(value.getTime()),
        };
      }
      return { type: ObjectType.TIMESTAMP, value: value as bigint };

    case ObjectType.DATE:
      return { type: ObjectType.DATE, value: value as number };

    case ObjectType.DURATION:
      return { type: ObjectType.DURATION, value: value as string };

    case ObjectType.REF:
      return { type: ObjectType.REF, value: value as EntityId };

    case ObjectType.REF_ARRAY:
      return { type: ObjectType.REF_ARRAY, value: value as EntityId[] };

    case ObjectType.JSON:
      return { type: ObjectType.JSON, value: value };

    case ObjectType.GEO_POINT:
      return { type: ObjectType.GEO_POINT, value: value as GeoPoint };

    case ObjectType.GEO_POLYGON:
      return { type: ObjectType.GEO_POLYGON, value: value as GeoPolygon };

    case ObjectType.GEO_LINESTRING:
      return {
        type: ObjectType.GEO_LINESTRING,
        value: value as GeoLineString,
      };

    case ObjectType.URL:
      return { type: ObjectType.URL, value: value as string };

    case ObjectType.VECTOR:
      return { type: ObjectType.VECTOR, value: value as number[] };

    default:
      return { type: ObjectType.JSON, value: value };
  }
}

/**
 * Create a Triple from subject, predicate, value, and transaction ID.
 *
 * The value type is inferred automatically from the JavaScript value
 * using inferObjectType(). Timestamp is set to current time (Date.now()).
 *
 * @param subject - The entity ID of the subject
 * @param predicate - The predicate name (no colons)
 * @param value - The value to store (type will be inferred)
 * @param txId - The transaction ID
 * @returns A complete Triple with inferred object type
 * @example
 * ```typescript
 * const triple = createTriple(
 *   createEntityId("https://example.com/users/123"),
 *   createPredicate("name"),
 *   "Alice",
 *   createTransactionId("01ARZ3NDEKTSV4RRFFQ69G5FAV")
 * );
 * // triple.object.type === ObjectType.STRING
 * // triple.object.value === "Alice"
 * ```
 */
export function createTriple(
  subject: EntityId,
  predicate: Predicate,
  value: unknown,
  txId: TransactionId
): Triple {
  return {
    subject,
    predicate,
    object: createTypedObject(value),
    timestamp: BigInt(Date.now()),
    txId,
  };
}

// ============================================================================
// Value Extraction
// ============================================================================

/**
 * Extract the primitive JavaScript value from a TypedObject.
 *
 * Unwraps the TypedObject to return just the inner value.
 * Returns null for NULL type.
 *
 * @param obj - The TypedObject to extract the value from
 * @returns The unwrapped JavaScript value
 * @example
 * ```typescript
 * const obj = { type: ObjectType.STRING, value: "hello" };
 * extractValue(obj); // "hello"
 *
 * const nullObj = { type: ObjectType.NULL };
 * extractValue(nullObj); // null
 * ```
 */
export function extractValue(obj: TypedObject): unknown {
  switch (obj.type) {
    case ObjectType.NULL:
      return null;

    case ObjectType.BOOL:
      return obj.value;

    case ObjectType.INT32:
    case ObjectType.INT64:
      return obj.value;

    case ObjectType.FLOAT64:
      return obj.value;

    case ObjectType.STRING:
      return obj.value;

    case ObjectType.BINARY:
      return obj.value;

    case ObjectType.TIMESTAMP:
      return obj.value;

    case ObjectType.DATE:
      return obj.value;

    case ObjectType.DURATION:
      return obj.value;

    case ObjectType.REF:
      return obj.value;

    case ObjectType.REF_ARRAY:
      return obj.value;

    case ObjectType.JSON:
      return obj.value;

    case ObjectType.GEO_POINT:
      return obj.value;

    case ObjectType.GEO_POLYGON:
      return obj.value;

    case ObjectType.GEO_LINESTRING:
      return obj.value;

    case ObjectType.URL:
      return obj.value;

    case ObjectType.VECTOR:
      return obj.value;

    default:
      return null;
  }
}
