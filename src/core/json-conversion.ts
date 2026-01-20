/**
 * Safe JSON Conversion Module
 *
 * Provides runtime validation for JSON to TypedObject/Triple conversions.
 * Replaces unsafe type assertions with validated conversions.
 *
 * Key features:
 * - Validates JSON structure before conversion
 * - Handles unexpected types gracefully
 * - Provides helpful error messages with actual vs expected types
 *
 * @see CLAUDE.md for architecture details
 */

import type {
  Triple,
  TypedObject,
  NullTypedObject,
  BoolTypedObject,
  Int32TypedObject,
  Int64TypedObject,
  Float64TypedObject,
  StringTypedObject,
  BinaryTypedObject,
  TimestampTypedObject,
  DateTypedObject,
  DurationTypedObject,
  RefTypedObject,
  RefArrayTypedObject,
  JsonTypedObject,
  GeoPointTypedObject,
  GeoPolygonTypedObject,
  GeoLineStringTypedObject,
  UrlTypedObject,
  VectorTypedObject,
} from './triple.js';
import type { EntityId, Predicate, TransactionId } from './types.js';
import {
  ObjectType,
  isEntityId,
  isPredicate,
  isTransactionId,
} from './types.js';

// ============================================================================
// Error Types
// ============================================================================

/**
 * Error codes for JSON conversion failures
 */
export enum JsonConversionErrorCode {
  /** Required field is missing */
  MISSING_FIELD = 'MISSING_FIELD',
  /** Field has wrong type */
  INVALID_TYPE = 'INVALID_TYPE',
  /** Value is invalid for the specified type */
  INVALID_VALUE = 'INVALID_VALUE',
  /** Input is not a valid object */
  INVALID_INPUT = 'INVALID_INPUT',
}

/**
 * Error response format for API handlers
 */
export interface JsonConversionErrorResponse {
  type: 'error';
  code: JsonConversionErrorCode;
  message: string;
}

/**
 * Custom error class for JSON conversion failures
 */
export class JsonConversionError extends Error {
  readonly code: JsonConversionErrorCode;

  constructor(code: JsonConversionErrorCode, message: string) {
    super(message);
    this.name = 'JsonConversionError';
    this.code = code;
    if ('captureStackTrace' in Error && typeof Error.captureStackTrace === 'function') {
      Error.captureStackTrace(this, JsonConversionError);
    }
  }

  /**
   * Convert error to response format for API handlers
   */
  toResponse(): JsonConversionErrorResponse {
    return {
      type: 'error',
      code: this.code,
      message: this.message,
    };
  }
}

// ============================================================================
// Type Helpers
// ============================================================================

/**
 * Get JavaScript type name for error messages
 */
function getTypeName(value: unknown): string {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (Array.isArray(value)) return 'array';
  return typeof value;
}

/**
 * Truncate string for safe inclusion in error messages
 * Prevents overly long error messages from large inputs
 */
function truncateForError(value: string, maxLength: number = 100): string {
  if (value.length <= maxLength) {
    return value;
  }
  return value.slice(0, maxLength) + '...[truncated]';
}

/**
 * Get ObjectType name for error messages
 */
function getObjectTypeName(type: ObjectType): string {
  const names: Record<ObjectType, string> = {
    [ObjectType.NULL]: 'NULL',
    [ObjectType.BOOL]: 'BOOL',
    [ObjectType.INT32]: 'INT32',
    [ObjectType.INT64]: 'INT64',
    [ObjectType.FLOAT64]: 'FLOAT64',
    [ObjectType.STRING]: 'STRING',
    [ObjectType.BINARY]: 'BINARY',
    [ObjectType.TIMESTAMP]: 'TIMESTAMP',
    [ObjectType.DATE]: 'DATE',
    [ObjectType.DURATION]: 'DURATION',
    [ObjectType.REF]: 'REF',
    [ObjectType.REF_ARRAY]: 'REF_ARRAY',
    [ObjectType.JSON]: 'JSON',
    [ObjectType.GEO_POINT]: 'GEO_POINT',
    [ObjectType.GEO_POLYGON]: 'GEO_POLYGON',
    [ObjectType.GEO_LINESTRING]: 'GEO_LINESTRING',
    [ObjectType.URL]: 'URL',
    [ObjectType.VECTOR]: 'VECTOR',
  };
  return names[type] ?? 'UNKNOWN';
}

/**
 * Check if a value is a plain object (not null, array, etc.)
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Check if value is a valid ObjectType enum value
 */
function isValidObjectType(value: unknown): value is ObjectType {
  if (typeof value !== 'number') return false;
  return Object.values(ObjectType).includes(value);
}

// ============================================================================
// TypedObject Conversion
// ============================================================================

/**
 * Parse and validate a TypedObject from JSON
 *
 * @param input - Raw JSON object to parse
 * @returns TypedObject or JsonConversionError
 *
 * @example
 * ```typescript
 * const result = parseTypedObjectFromJson({ type: 5, value: "hello" });
 * if (result instanceof JsonConversionError) {
 *   console.error(result.message);
 *   return;
 * }
 * // result is now a validated TypedObject
 * ```
 */
export function parseTypedObjectFromJson(
  input: unknown
): TypedObject | JsonConversionError {
  // Validate input is an object
  if (!isPlainObject(input)) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_INPUT,
      `Invalid input: expected object, got ${getTypeName(input)}`
    );
  }

  // Validate type field exists
  if (!('type' in input)) {
    return new JsonConversionError(
      JsonConversionErrorCode.MISSING_FIELD,
      'Missing required field: "type"'
    );
  }

  // Validate type is a number
  if (typeof input['type'] !== 'number') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "type" field: expected number, got ${getTypeName(input['type'])}`
    );
  }

  // Validate type is a valid ObjectType
  if (!isValidObjectType(input['type'])) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "type" field: unknown ObjectType ${input['type']}`
    );
  }

  const objType = input['type'];
  const value = input['value'];

  // Convert based on type with validation
  switch (objType) {
    case ObjectType.NULL:
      return { type: ObjectType.NULL } as NullTypedObject;

    case ObjectType.BOOL:
      if (typeof value !== 'boolean') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for BOOL: expected boolean, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.BOOL, value } as BoolTypedObject;

    case ObjectType.INT32:
      return parseIntTypedObject(value, ObjectType.INT32) as
        | Int32TypedObject
        | JsonConversionError;

    case ObjectType.INT64:
      return parseIntTypedObject(value, ObjectType.INT64) as
        | Int64TypedObject
        | JsonConversionError;

    case ObjectType.FLOAT64:
      if (typeof value !== 'number') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for FLOAT64: expected number, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.FLOAT64, value } as Float64TypedObject;

    case ObjectType.STRING:
      if (typeof value !== 'string') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for STRING: expected string, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.STRING, value } as StringTypedObject;

    case ObjectType.BINARY:
      if (!Array.isArray(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for BINARY: expected array of numbers, got ${getTypeName(value)}`
        );
      }
      // Validate all elements are numbers in byte range
      for (let i = 0; i < value.length; i++) {
        const element = value[i];
        if (typeof element !== 'number' || element < 0 || element > 255) {
          return new JsonConversionError(
            JsonConversionErrorCode.INVALID_VALUE,
            `Invalid value for BINARY: element at index ${i} is not a valid byte (0-255)`
          );
        }
      }
      return {
        type: ObjectType.BINARY,
        value: new Uint8Array(value),
      } as BinaryTypedObject;

    case ObjectType.TIMESTAMP:
      return parseTimestampTypedObject(value);

    case ObjectType.DATE:
      if (typeof value !== 'number') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for DATE: expected number (days since epoch), got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.DATE, value } as DateTypedObject;

    case ObjectType.DURATION:
      if (typeof value !== 'string') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for DURATION: expected ISO 8601 duration string, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.DURATION, value } as DurationTypedObject;

    case ObjectType.REF:
      if (typeof value !== 'string') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for REF: expected string (EntityId), got ${getTypeName(value)}`
        );
      }
      // Validate EntityId format to prevent bypass via direct JSON input
      if (!isEntityId(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for REF: "${truncateForError(value)}" is not a valid EntityId (http/https URL)`
        );
      }
      return { type: ObjectType.REF, value: value as EntityId } as RefTypedObject;

    case ObjectType.REF_ARRAY:
      if (!Array.isArray(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for REF_ARRAY: expected array of strings, got ${getTypeName(value)}`
        );
      }
      for (let i = 0; i < value.length; i++) {
        if (typeof value[i] !== 'string') {
          return new JsonConversionError(
            JsonConversionErrorCode.INVALID_VALUE,
            `Invalid value for REF_ARRAY: element at index ${i} is not a string`
          );
        }
        // Validate each EntityId to prevent bypass via direct JSON input
        if (!isEntityId(value[i])) {
          return new JsonConversionError(
            JsonConversionErrorCode.INVALID_VALUE,
            `Invalid value for REF_ARRAY: element at index ${i} "${truncateForError(value[i])}" is not a valid EntityId`
          );
        }
      }
      return {
        type: ObjectType.REF_ARRAY,
        value: value as EntityId[],
      } as RefArrayTypedObject;

    case ObjectType.JSON:
      // JSON type can hold any value
      return { type: ObjectType.JSON, value } as JsonTypedObject;

    case ObjectType.GEO_POINT:
      return parseGeoPointTypedObject(value);

    case ObjectType.GEO_POLYGON:
      if (!isPlainObject(value) && !Array.isArray(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for GEO_POLYGON: expected object or array, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.GEO_POLYGON, value } as unknown as GeoPolygonTypedObject;

    case ObjectType.GEO_LINESTRING:
      if (!isPlainObject(value) && !Array.isArray(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for GEO_LINESTRING: expected object or array, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.GEO_LINESTRING, value } as unknown as GeoLineStringTypedObject;

    case ObjectType.URL:
      if (typeof value !== 'string') {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for URL: expected string, got ${getTypeName(value)}`
        );
      }
      return { type: ObjectType.URL, value } as UrlTypedObject;

    case ObjectType.VECTOR:
      if (!Array.isArray(value)) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for VECTOR: expected array of numbers, got ${getTypeName(value)}`
        );
      }
      // Validate all elements are finite numbers
      for (let i = 0; i < value.length; i++) {
        const element = value[i];
        if (typeof element !== 'number' || !Number.isFinite(element)) {
          return new JsonConversionError(
            JsonConversionErrorCode.INVALID_VALUE,
            `Invalid value for VECTOR: element at index ${i} is not a finite number`
          );
        }
      }
      return { type: ObjectType.VECTOR, value } as VectorTypedObject;

    default:
      // This shouldn't happen due to isValidObjectType check, but TypeScript needs it
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_TYPE,
        `Unknown ObjectType: ${objType}`
      );
  }
}

/**
 * Parse integer value from JSON (handles both number and string)
 */
function parseIntTypedObject(
  value: unknown,
  type: ObjectType.INT32 | ObjectType.INT64
): { type: typeof type; value: bigint } | JsonConversionError {
  const typeName = getObjectTypeName(type);

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_VALUE,
        `Invalid value for ${typeName}: number must be finite`
      );
    }
    return { type, value: BigInt(Math.trunc(value)) };
  }

  if (typeof value === 'string') {
    try {
      return { type, value: BigInt(value) };
    } catch {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_VALUE,
        `Invalid value for ${typeName}: string "${value}" is not a valid integer`
      );
    }
  }

  return new JsonConversionError(
    JsonConversionErrorCode.INVALID_VALUE,
    `Invalid value for ${typeName}: expected number or string, got ${getTypeName(value)}`
  );
}

/**
 * Parse timestamp value from JSON
 */
function parseTimestampTypedObject(
  value: unknown
): TimestampTypedObject | JsonConversionError {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value < 0) {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_VALUE,
        `Invalid value for TIMESTAMP: must be a non-negative finite number`
      );
    }
    return { type: ObjectType.TIMESTAMP, value: BigInt(Math.trunc(value)) };
  }

  if (typeof value === 'string') {
    try {
      const bigintValue = BigInt(value);
      if (bigintValue < 0n) {
        return new JsonConversionError(
          JsonConversionErrorCode.INVALID_VALUE,
          `Invalid value for TIMESTAMP: must be non-negative`
        );
      }
      return { type: ObjectType.TIMESTAMP, value: bigintValue };
    } catch {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_VALUE,
        `Invalid value for TIMESTAMP: string "${value}" is not a valid integer`
      );
    }
  }

  return new JsonConversionError(
    JsonConversionErrorCode.INVALID_VALUE,
    `Invalid value for TIMESTAMP: expected number or string, got ${getTypeName(value)}`
  );
}

/**
 * Parse GeoPoint value from JSON
 */
function parseGeoPointTypedObject(
  value: unknown
): GeoPointTypedObject | JsonConversionError {
  if (!isPlainObject(value)) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid value for GEO_POINT: expected object with lat/lng, got ${getTypeName(value)}`
    );
  }

  if (typeof value['lat'] !== 'number') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid value for GEO_POINT: "lat" must be a number, got ${getTypeName(value['lat'])}`
    );
  }

  if (typeof value['lng'] !== 'number') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid value for GEO_POINT: "lng" must be a number, got ${getTypeName(value['lng'])}`
    );
  }

  return {
    type: ObjectType.GEO_POINT,
    value: { lat: value['lat'], lng: value['lng'] },
  };
}

// ============================================================================
// Triple Conversion
// ============================================================================

/**
 * Parse and validate a Triple from JSON
 *
 * @param input - Raw JSON object to parse
 * @returns Triple or JsonConversionError
 *
 * @example
 * ```typescript
 * const result = parseTripleFromJson({
 *   subject: "https://example.com/entity/1",
 *   predicate: "name",
 *   object: { type: 5, value: "Test" },
 *   timestamp: Date.now(),
 *   txId: "01ARZ3NDEKTSV4RRFFQ69G5FAV"
 * });
 * if (result instanceof JsonConversionError) {
 *   console.error(result.message);
 *   return;
 * }
 * // result is now a validated Triple
 * ```
 */
export function parseTripleFromJson(
  input: unknown
): Triple | JsonConversionError {
  // Validate input is an object
  if (!isPlainObject(input)) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_INPUT,
      `Invalid input: expected object, got ${getTypeName(input)}`
    );
  }

  // Validate required fields exist
  const requiredFields = ['subject', 'predicate', 'object', 'timestamp', 'txId'];
  for (const field of requiredFields) {
    if (!(field in input)) {
      return new JsonConversionError(
        JsonConversionErrorCode.MISSING_FIELD,
        `Missing required field: "${field}"`
      );
    }
  }

  // Validate subject (must be valid EntityId)
  if (typeof input['subject'] !== 'string') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "subject" field: expected string, got ${getTypeName(input['subject'])}`
    );
  }
  if (!isEntityId(input['subject'])) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid "subject" field: "${truncateForError(input['subject'])}" is not a valid EntityId (http/https URL)`
    );
  }

  // Validate predicate (must be valid Predicate)
  if (typeof input['predicate'] !== 'string') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "predicate" field: expected string, got ${getTypeName(input['predicate'])}`
    );
  }
  if (!isPredicate(input['predicate'])) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid "predicate" field: "${truncateForError(input['predicate'])}" is not a valid Predicate (must be JS identifier-like, no colons)`
    );
  }

  // Validate object
  if (!isPlainObject(input['object'])) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "object" field: expected object, got ${getTypeName(input['object'])}`
    );
  }

  // Parse and validate the TypedObject
  const objectResult = parseTypedObjectFromJson(input['object']);
  if (objectResult instanceof JsonConversionError) {
    return objectResult;
  }

  // Validate timestamp
  let timestamp: bigint;
  if (typeof input['timestamp'] === 'number') {
    if (!Number.isFinite(input['timestamp']) || input['timestamp'] < 0) {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_TYPE,
        `Invalid "timestamp" field: must be a non-negative finite number`
      );
    }
    timestamp = BigInt(Math.trunc(input['timestamp']));
  } else if (typeof input['timestamp'] === 'string') {
    try {
      timestamp = BigInt(input['timestamp']);
    } catch {
      return new JsonConversionError(
        JsonConversionErrorCode.INVALID_TYPE,
        `Invalid "timestamp" field: string "${input['timestamp']}" is not a valid integer`
      );
    }
  } else {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "timestamp" field: expected number or string, got ${getTypeName(input['timestamp'])}`
    );
  }

  // Validate txId (must be valid TransactionId)
  if (typeof input['txId'] !== 'string') {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_TYPE,
      `Invalid "txId" field: expected string, got ${getTypeName(input['txId'])}`
    );
  }
  if (!isTransactionId(input['txId'])) {
    return new JsonConversionError(
      JsonConversionErrorCode.INVALID_VALUE,
      `Invalid "txId" field: "${truncateForError(input['txId'])}" is not a valid TransactionId (26-char ULID)`
    );
  }

  // At this point all branded types have been validated
  return {
    subject: input['subject'] as EntityId,
    predicate: input['predicate'] as Predicate,
    object: objectResult,
    timestamp,
    txId: input['txId'] as TransactionId,
  };
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Check if JSON represents a valid TypedObject
 */
export function isValidTypedObjectJson(input: unknown): boolean {
  const result = parseTypedObjectFromJson(input);
  return !(result instanceof JsonConversionError);
}

/**
 * Check if JSON represents a valid Triple
 */
export function isValidTripleJson(input: unknown): boolean {
  const result = parseTripleFromJson(input);
  return !(result instanceof JsonConversionError);
}
