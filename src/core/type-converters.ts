/**
 * Consolidated Type Conversion Module for GraphDB
 *
 * This module centralizes all type conversion logic for TypedObject:
 * - typedObjectToJson: Convert TypedObject to JSON-serializable format
 * - jsonToTypedObject: Convert JSON to TypedObject
 * - typedObjectToSqlValue: Convert TypedObject to SQL column values
 * - sqlValueToTypedObject: Convert SQL column values to TypedObject
 *
 * Previously, this logic was duplicated across:
 * - src/shard/shard-do.ts
 * - src/shard/crud.ts
 * - src/benchmark/benchmark-worker.ts
 * - src/query/materializer.ts
 * - src/coordinator/cdc-coordinator-do.ts
 * - src/index/triple-indexes.ts
 * - src/storage/graphcol.ts
 *
 * @see CLAUDE.md for architecture details
 */

import type {
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
import type { EntityId } from './types.js';
import { ObjectType } from './types.js';
import type { GeoPoint, GeoPolygon, GeoLineString } from './geo.js';

// ============================================================================
// JSON Conversion Types
// ============================================================================

/**
 * JSON-safe representation of a TypedObject value
 */
export interface JsonTypedObjectValue {
  type: ObjectType;
  value?: unknown;
}

/**
 * Options for JSON conversion
 */
export interface JsonConversionOptions {
  /**
   * Whether to serialize REF values with '@ref' wrapper for later expansion
   * Default: false - values are serialized as-is
   */
  wrapRefs?: boolean;
}

// ============================================================================
// SQL Row Types
// ============================================================================

/**
 * SQL row representation for TypedObject value columns
 */
export interface SqlObjectColumns {
  obj_type: number;
  obj_ref?: string | null;
  obj_string?: string | null;
  obj_int64?: number | bigint | null;
  obj_float64?: number | null;
  obj_bool?: number | null;
  obj_timestamp?: number | bigint | null;
  obj_lat?: number | null;
  obj_lng?: number | null;
  obj_binary?: Uint8Array | null;
}

/**
 * Input row for SQL to TypedObject conversion
 * This is the minimal interface required for conversion
 */
export interface SqlRowInput {
  obj_type: number;
  obj_ref?: string | null;
  obj_string?: string | null;
  obj_int64?: number | bigint | null;
  obj_float64?: number | null;
  obj_bool?: number | null;
  obj_timestamp?: number | bigint | null;
  obj_lat?: number | null;
  obj_lng?: number | null;
  obj_binary?: Uint8Array | null;
}

// ============================================================================
// TypedObject to JSON Conversion
// ============================================================================

/**
 * Convert a TypedObject to a JSON-serializable format
 *
 * Handles special cases:
 * - BigInt values are converted to strings to preserve precision
 * - Uint8Array binary data is converted to number arrays
 * - Optionally wraps REF values with '@ref' for later expansion
 *
 * @param obj - The TypedObject to convert
 * @param options - Conversion options
 * @returns JSON-safe representation
 */
export function typedObjectToJson(
  obj: TypedObject,
  options: JsonConversionOptions = {}
): JsonTypedObjectValue {
  const result: JsonTypedObjectValue = { type: obj.type };

  switch (obj.type) {
    case ObjectType.NULL:
      // No value needed for NULL type
      break;

    case ObjectType.BOOL:
      result.value = obj.value;
      break;

    case ObjectType.INT32:
    case ObjectType.INT64:
      // Convert BigInt to string to preserve precision in JSON
      result.value = obj.value?.toString();
      break;

    case ObjectType.FLOAT64:
      result.value = obj.value;
      break;

    case ObjectType.STRING:
      result.value = obj.value;
      break;

    case ObjectType.BINARY:
      // Convert Uint8Array to number array for JSON serialization
      result.value = obj.value ? Array.from(obj.value) : null;
      break;

    case ObjectType.TIMESTAMP:
      // Convert BigInt to string to preserve precision
      result.value = obj.value?.toString();
      break;

    case ObjectType.DATE:
      result.value = obj.value;
      break;

    case ObjectType.DURATION:
      result.value = obj.value;
      break;

    case ObjectType.REF:
      if (options.wrapRefs) {
        result.value = { '@ref': obj.value };
      } else {
        result.value = obj.value;
      }
      break;

    case ObjectType.REF_ARRAY:
      if (options.wrapRefs) {
        result.value = obj.value?.map((ref) => ({ '@ref': ref }));
      } else {
        result.value = obj.value;
      }
      break;

    case ObjectType.JSON:
      result.value = obj.value;
      break;

    case ObjectType.GEO_POINT:
      result.value = obj.value;
      break;

    case ObjectType.GEO_POLYGON:
      result.value = obj.value;
      break;

    case ObjectType.GEO_LINESTRING:
      result.value = obj.value;
      break;

    case ObjectType.URL:
      result.value = obj.value;
      break;

    case ObjectType.VECTOR:
      result.value = obj.value;
      break;
  }

  return result;
}

/**
 * Extract the raw value from a TypedObject for JSON serialization
 *
 * Unlike typedObjectToJson, this returns only the value without the type field.
 * Useful for materializing entity views where the type is implicit.
 *
 * @param obj - The TypedObject to extract value from
 * @param options - Conversion options
 * @returns The raw value in JSON-safe format
 */
export function extractJsonValue(
  obj: TypedObject,
  options: JsonConversionOptions = {}
): unknown {
  switch (obj.type) {
    case ObjectType.NULL:
      return null;

    case ObjectType.BOOL:
      return obj.value;

    case ObjectType.INT32:
    case ObjectType.INT64:
      // Return bigint directly - let the caller handle serialization
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
      if (options.wrapRefs) {
        return { '@ref': obj.value };
      }
      return obj.value;

    case ObjectType.REF_ARRAY:
      if (options.wrapRefs) {
        return obj.value?.map((ref) => ({ '@ref': ref }));
      }
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

// ============================================================================
// JSON to TypedObject Conversion
// ============================================================================

/**
 * Convert a JSON object to a TypedObject
 *
 * This function reconstructs the proper discriminated union based on the type field.
 * It handles:
 * - String values for BigInt types (INT32, INT64, TIMESTAMP)
 * - Number arrays for BINARY type
 * - Nested objects for GEO_POINT
 *
 * @param jsonObj - JSON object with type and optional value fields
 * @returns TypedObject with proper typing
 */
export function jsonToTypedObject(jsonObj: JsonTypedObjectValue): TypedObject {
  const objType = jsonObj.type;
  const value = jsonObj.value;

  switch (objType) {
    case ObjectType.NULL:
      return { type: ObjectType.NULL } as NullTypedObject;

    case ObjectType.BOOL:
      return { type: ObjectType.BOOL, value: value as boolean } as BoolTypedObject;

    case ObjectType.INT32:
      return {
        type: ObjectType.INT32,
        value: value !== undefined ? BigInt(value as string | number) : 0n,
      } as Int32TypedObject;

    case ObjectType.INT64:
      return {
        type: ObjectType.INT64,
        value: value !== undefined ? BigInt(value as string | number) : 0n,
      } as Int64TypedObject;

    case ObjectType.FLOAT64:
      return { type: ObjectType.FLOAT64, value: value as number } as Float64TypedObject;

    case ObjectType.STRING:
      return { type: ObjectType.STRING, value: value as string } as StringTypedObject;

    case ObjectType.BINARY:
      return {
        type: ObjectType.BINARY,
        value: Array.isArray(value) ? new Uint8Array(value) : (value as Uint8Array),
      } as BinaryTypedObject;

    case ObjectType.TIMESTAMP:
      return {
        type: ObjectType.TIMESTAMP,
        value: value !== undefined ? BigInt(value as string | number) : 0n,
      } as TimestampTypedObject;

    case ObjectType.DATE:
      return { type: ObjectType.DATE, value: value as number } as DateTypedObject;

    case ObjectType.DURATION:
      return { type: ObjectType.DURATION, value: value as string } as DurationTypedObject;

    case ObjectType.REF:
      return { type: ObjectType.REF, value: value as EntityId } as RefTypedObject;

    case ObjectType.REF_ARRAY:
      return {
        type: ObjectType.REF_ARRAY,
        value: value as EntityId[],
      } as RefArrayTypedObject;

    case ObjectType.JSON:
      return { type: ObjectType.JSON, value } as JsonTypedObject;

    case ObjectType.GEO_POINT:
      return {
        type: ObjectType.GEO_POINT,
        value: value as GeoPoint,
      } as GeoPointTypedObject;

    case ObjectType.GEO_POLYGON:
      return {
        type: ObjectType.GEO_POLYGON,
        value: value as GeoPolygon,
      } as GeoPolygonTypedObject;

    case ObjectType.GEO_LINESTRING:
      return {
        type: ObjectType.GEO_LINESTRING,
        value: value as GeoLineString,
      } as GeoLineStringTypedObject;

    case ObjectType.URL:
      return { type: ObjectType.URL, value: value as string } as UrlTypedObject;

    case ObjectType.VECTOR:
      return { type: ObjectType.VECTOR, value: value as number[] } as VectorTypedObject;

    default:
      // Fallback to NULL for unknown types
      return { type: ObjectType.NULL } as NullTypedObject;
  }
}

// ============================================================================
// TypedObject to SQL Conversion
// ============================================================================

/**
 * Convert a TypedObject to SQL column values
 *
 * Maps the TypedObject value to the appropriate SQL columns based on type:
 * - BOOL -> obj_bool (0 or 1)
 * - INT32/INT64 -> obj_int64 (number for SQLite compatibility)
 * - FLOAT64 -> obj_float64
 * - STRING/DURATION/URL -> obj_string
 * - BINARY/REF_ARRAY/JSON/GEO_POLYGON/GEO_LINESTRING -> obj_binary (encoded)
 * - TIMESTAMP -> obj_timestamp
 * - DATE -> obj_int64
 * - REF -> obj_ref
 * - GEO_POINT -> obj_lat, obj_lng
 *
 * @param obj - The TypedObject to convert
 * @returns SQL column values object
 */
export function typedObjectToSqlValue(obj: TypedObject): SqlObjectColumns {
  const result: SqlObjectColumns = {
    obj_type: obj.type,
  };

  switch (obj.type) {
    case ObjectType.NULL:
      // No value columns needed
      break;

    case ObjectType.BOOL:
      result.obj_bool = obj.value ? 1 : 0;
      break;

    case ObjectType.INT32:
    case ObjectType.INT64:
      // Convert BigInt to Number for SQLite
      // Note: This may lose precision for values > Number.MAX_SAFE_INTEGER
      result.obj_int64 = obj.value !== undefined ? Number(obj.value) : null;
      break;

    case ObjectType.FLOAT64:
      result.obj_float64 = obj.value;
      break;

    case ObjectType.STRING:
      result.obj_string = obj.value;
      break;

    case ObjectType.BINARY:
      result.obj_binary = obj.value;
      break;

    case ObjectType.TIMESTAMP:
      // Preserve bigint for SQLite (64-bit INTEGER supports full precision)
      result.obj_timestamp = obj.value ?? null;
      break;

    case ObjectType.DATE:
      // Store date as integer (days since epoch)
      result.obj_int64 = obj.value;
      break;

    case ObjectType.DURATION:
      // Store duration as string
      result.obj_string = obj.value;
      break;

    case ObjectType.REF:
      result.obj_ref = obj.value;
      break;

    case ObjectType.REF_ARRAY:
      // Store as JSON in binary column
      result.obj_binary = new TextEncoder().encode(JSON.stringify(obj.value));
      break;

    case ObjectType.JSON:
      // Store as JSON in binary column
      result.obj_binary = new TextEncoder().encode(JSON.stringify(obj.value));
      break;

    case ObjectType.GEO_POINT:
      result.obj_lat = obj.value?.lat;
      result.obj_lng = obj.value?.lng;
      break;

    case ObjectType.GEO_POLYGON:
      // Store as JSON in binary column
      result.obj_binary = new TextEncoder().encode(JSON.stringify(obj.value));
      break;

    case ObjectType.GEO_LINESTRING:
      // Store as JSON in binary column
      result.obj_binary = new TextEncoder().encode(JSON.stringify(obj.value));
      break;

    case ObjectType.URL:
      result.obj_string = obj.value;
      break;

    case ObjectType.VECTOR:
      // Store vector as JSON in binary column
      result.obj_binary = new TextEncoder().encode(JSON.stringify(obj.value));
      break;
  }

  return result;
}

// ============================================================================
// SQL to TypedObject Conversion
// ============================================================================

/**
 * Convert SQL column values to a TypedObject
 *
 * Reconstructs the TypedObject from the SQL column values.
 * Handles type coercion for:
 * - Integer columns that may be number or bigint
 * - Binary columns that need to be decoded for JSON/REF_ARRAY types
 *
 * @param row - SQL row with object columns
 * @returns TypedObject with proper typing
 */
export function sqlValueToTypedObject(row: SqlRowInput): TypedObject {
  const objType = row.obj_type as ObjectType;

  switch (objType) {
    case ObjectType.NULL:
      return { type: ObjectType.NULL } as NullTypedObject;

    case ObjectType.BOOL:
      return {
        type: ObjectType.BOOL,
        value: row.obj_bool === 1,
      } as BoolTypedObject;

    case ObjectType.INT32:
      return {
        type: ObjectType.INT32,
        value: toBigInt(row.obj_int64),
      } as Int32TypedObject;

    case ObjectType.INT64:
      return {
        type: ObjectType.INT64,
        value: toBigInt(row.obj_int64),
      } as Int64TypedObject;

    case ObjectType.FLOAT64:
      return {
        type: ObjectType.FLOAT64,
        value: row.obj_float64 ?? 0,
      } as Float64TypedObject;

    case ObjectType.STRING:
      return {
        type: ObjectType.STRING,
        value: row.obj_string ?? '',
      } as StringTypedObject;

    case ObjectType.BINARY:
      return {
        type: ObjectType.BINARY,
        value: row.obj_binary ?? new Uint8Array(),
      } as BinaryTypedObject;

    case ObjectType.TIMESTAMP:
      return {
        type: ObjectType.TIMESTAMP,
        value: toBigInt(row.obj_timestamp),
      } as TimestampTypedObject;

    case ObjectType.DATE:
      return {
        type: ObjectType.DATE,
        value: Number(row.obj_int64) || 0,
      } as DateTypedObject;

    case ObjectType.DURATION:
      return {
        type: ObjectType.DURATION,
        value: row.obj_string ?? '',
      } as DurationTypedObject;

    case ObjectType.REF:
      return {
        type: ObjectType.REF,
        value: row.obj_ref as EntityId,
      } as RefTypedObject;

    case ObjectType.REF_ARRAY: {
      const jsonStr = row.obj_binary ? new TextDecoder().decode(row.obj_binary) : '[]';
      const refs = JSON.parse(jsonStr) as string[];
      return {
        type: ObjectType.REF_ARRAY,
        value: refs.map((r) => r as EntityId),
      } as RefArrayTypedObject;
    }

    case ObjectType.JSON: {
      const jsonStr = row.obj_binary ? new TextDecoder().decode(row.obj_binary) : 'null';
      return {
        type: ObjectType.JSON,
        value: JSON.parse(jsonStr),
      } as JsonTypedObject;
    }

    case ObjectType.GEO_POINT:
      return {
        type: ObjectType.GEO_POINT,
        value: {
          lat: row.obj_lat ?? 0,
          lng: row.obj_lng ?? 0,
        },
      } as GeoPointTypedObject;

    case ObjectType.GEO_POLYGON: {
      const jsonStr = row.obj_binary ? new TextDecoder().decode(row.obj_binary) : 'null';
      return {
        type: ObjectType.GEO_POLYGON,
        value: JSON.parse(jsonStr),
      } as GeoPolygonTypedObject;
    }

    case ObjectType.GEO_LINESTRING: {
      const jsonStr = row.obj_binary ? new TextDecoder().decode(row.obj_binary) : 'null';
      return {
        type: ObjectType.GEO_LINESTRING,
        value: JSON.parse(jsonStr),
      } as GeoLineStringTypedObject;
    }

    case ObjectType.URL:
      return {
        type: ObjectType.URL,
        value: row.obj_string ?? '',
      } as UrlTypedObject;

    case ObjectType.VECTOR: {
      const jsonStr = row.obj_binary ? new TextDecoder().decode(row.obj_binary) : '[]';
      return {
        type: ObjectType.VECTOR,
        value: JSON.parse(jsonStr) as number[],
      } as VectorTypedObject;
    }

    default:
      // Fallback to NULL for unknown types
      return { type: ObjectType.NULL } as NullTypedObject;
  }
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Safely convert a value to BigInt
 * Handles number, bigint, string, null, and undefined inputs
 */
function toBigInt(value: number | bigint | string | null | undefined): bigint {
  if (value === null || value === undefined) {
    return 0n;
  }
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'string') {
    return BigInt(value);
  }
  return BigInt(value);
}

/**
 * Get the SQL column name for a given ObjectType for filtering/comparison
 * Useful for building WHERE clauses
 */
export function getValueColumn(valueType: ObjectType): string {
  switch (valueType) {
    case ObjectType.STRING:
    case ObjectType.DURATION:
    case ObjectType.URL:
      return 'obj_string';
    case ObjectType.INT32:
    case ObjectType.INT64:
    case ObjectType.DATE:
      return 'obj_int64';
    case ObjectType.FLOAT64:
      return 'obj_float64';
    case ObjectType.BOOL:
      return 'obj_bool';
    case ObjectType.TIMESTAMP:
      return 'obj_timestamp';
    case ObjectType.REF:
      return 'obj_ref';
    default:
      return 'obj_string';
  }
}

/**
 * Infer ObjectType from a JavaScript value
 * Useful for auto-detecting types when inserting data
 */
export function inferObjectTypeFromValue(value: unknown): ObjectType {
  if (value === null || value === undefined) {
    return ObjectType.NULL;
  }
  if (typeof value === 'boolean') {
    return ObjectType.BOOL;
  }
  if (typeof value === 'string') {
    return ObjectType.STRING;
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      return ObjectType.INT64;
    }
    return ObjectType.FLOAT64;
  }
  if (typeof value === 'bigint') {
    return ObjectType.INT64;
  }
  if (value instanceof Uint8Array) {
    return ObjectType.BINARY;
  }
  if (value instanceof Date) {
    return ObjectType.TIMESTAMP;
  }
  // Object or array -> JSON
  return ObjectType.JSON;
}

/**
 * Get the human-readable name for an ObjectType
 * Useful for error messages and debugging
 */
export function getObjectTypeName(type: ObjectType): string {
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
