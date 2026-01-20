/**
 * Type Converters Tests
 *
 * Tests for consolidated type conversion functions.
 * Covers all conversion paths:
 * - TypedObject <-> JSON
 * - TypedObject <-> SQL values
 *
 * TDD approach: These tests validate that the consolidated functions
 * produce the same results as the previously duplicated implementations.
 */

import { describe, it, expect } from 'vitest';
import {
  typedObjectToJson,
  jsonToTypedObject,
  typedObjectToSqlValue,
  sqlValueToTypedObject,
  extractJsonValue,
  getValueColumn,
  inferObjectTypeFromValue,
  getObjectTypeName,
  type JsonTypedObjectValue,
  type SqlRowInput,
} from '../../src/core/type-converters.js';
import { ObjectType } from '../../src/core/types.js';
import type { TypedObject } from '../../src/core/triple.js';
import type { EntityId } from '../../src/core/types.js';

describe('Type Converters', () => {
  // ============================================================================
  // typedObjectToJson Tests
  // ============================================================================

  describe('typedObjectToJson', () => {
    it('should convert NULL type', () => {
      const obj: TypedObject = { type: ObjectType.NULL };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.NULL);
      expect(result.value).toBeUndefined();
    });

    it('should convert BOOL type', () => {
      const trueObj: TypedObject = { type: ObjectType.BOOL, value: true };
      const falseObj: TypedObject = { type: ObjectType.BOOL, value: false };

      expect(typedObjectToJson(trueObj)).toEqual({ type: ObjectType.BOOL, value: true });
      expect(typedObjectToJson(falseObj)).toEqual({ type: ObjectType.BOOL, value: false });
    });

    it('should convert INT32 to string to preserve precision', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: 42n };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.INT32);
      expect(result.value).toBe('42');
    });

    it('should convert INT64 to string to preserve precision', () => {
      const obj: TypedObject = { type: ObjectType.INT64, value: 9007199254740993n };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.INT64);
      expect(result.value).toBe('9007199254740993');
    });

    it('should convert FLOAT64 type', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: 3.14159 };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.FLOAT64);
      expect(result.value).toBeCloseTo(3.14159);
    });

    it('should convert STRING type', () => {
      const obj: TypedObject = { type: ObjectType.STRING, value: 'hello world' };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.STRING);
      expect(result.value).toBe('hello world');
    });

    it('should convert BINARY to number array', () => {
      const obj: TypedObject = { type: ObjectType.BINARY, value: new Uint8Array([0, 1, 255]) };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.BINARY);
      expect(result.value).toEqual([0, 1, 255]);
    });

    it('should convert TIMESTAMP to string', () => {
      const timestamp = 1704067200000n;
      const obj: TypedObject = { type: ObjectType.TIMESTAMP, value: timestamp };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.TIMESTAMP);
      expect(result.value).toBe('1704067200000');
    });

    it('should convert DATE type', () => {
      const obj: TypedObject = { type: ObjectType.DATE, value: 19722 }; // Days since epoch
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.DATE);
      expect(result.value).toBe(19722);
    });

    it('should convert DURATION type', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P1Y2M3D' };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.DURATION);
      expect(result.value).toBe('P1Y2M3D');
    });

    it('should convert REF type without wrapping by default', () => {
      const obj: TypedObject = {
        type: ObjectType.REF,
        value: 'https://example.com/entity/1' as EntityId,
      };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.REF);
      expect(result.value).toBe('https://example.com/entity/1');
    });

    it('should convert REF type with wrapping when option is set', () => {
      const obj: TypedObject = {
        type: ObjectType.REF,
        value: 'https://example.com/entity/1' as EntityId,
      };
      const result = typedObjectToJson(obj, { wrapRefs: true });

      expect(result.type).toBe(ObjectType.REF);
      expect(result.value).toEqual({ '@ref': 'https://example.com/entity/1' });
    });

    it('should convert REF_ARRAY type', () => {
      const obj: TypedObject = {
        type: ObjectType.REF_ARRAY,
        value: [
          'https://example.com/entity/1' as EntityId,
          'https://example.com/entity/2' as EntityId,
        ],
      };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.REF_ARRAY);
      expect(result.value).toEqual([
        'https://example.com/entity/1',
        'https://example.com/entity/2',
      ]);
    });

    it('should convert REF_ARRAY with wrapping when option is set', () => {
      const obj: TypedObject = {
        type: ObjectType.REF_ARRAY,
        value: [
          'https://example.com/entity/1' as EntityId,
          'https://example.com/entity/2' as EntityId,
        ],
      };
      const result = typedObjectToJson(obj, { wrapRefs: true });

      expect(result.type).toBe(ObjectType.REF_ARRAY);
      expect(result.value).toEqual([
        { '@ref': 'https://example.com/entity/1' },
        { '@ref': 'https://example.com/entity/2' },
      ]);
    });

    it('should convert JSON type', () => {
      const obj: TypedObject = {
        type: ObjectType.JSON,
        value: { nested: { deeply: [1, 2, 3] } },
      };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.JSON);
      expect(result.value).toEqual({ nested: { deeply: [1, 2, 3] } });
    });

    it('should convert GEO_POINT type', () => {
      const obj: TypedObject = {
        type: ObjectType.GEO_POINT,
        value: { lat: 40.7128, lng: -74.006 },
      };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.GEO_POINT);
      expect(result.value).toEqual({ lat: 40.7128, lng: -74.006 });
    });

    it('should convert GEO_POLYGON type', () => {
      const polygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 1, lng: 0 },
          { lat: 1, lng: 1 },
          { lat: 0, lng: 0 },
        ],
      };
      const obj: TypedObject = { type: ObjectType.GEO_POLYGON, value: polygon };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.GEO_POLYGON);
      expect(result.value).toEqual(polygon);
    });

    it('should convert GEO_LINESTRING type', () => {
      const linestring = [
        { lat: 0, lng: 0 },
        { lat: 1, lng: 1 },
      ];
      const obj: TypedObject = { type: ObjectType.GEO_LINESTRING, value: linestring };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.GEO_LINESTRING);
      expect(result.value).toEqual(linestring);
    });

    it('should convert URL type', () => {
      const obj: TypedObject = { type: ObjectType.URL, value: 'https://example.com/path' };
      const result = typedObjectToJson(obj);

      expect(result.type).toBe(ObjectType.URL);
      expect(result.value).toBe('https://example.com/path');
    });
  });

  // ============================================================================
  // jsonToTypedObject Tests
  // ============================================================================

  describe('jsonToTypedObject', () => {
    it('should convert NULL type', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.NULL };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.NULL);
    });

    it('should convert BOOL type', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.BOOL, value: true };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.BOOL);
      expect((result as { value: boolean }).value).toBe(true);
    });

    it('should convert INT32 from number', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.INT32, value: 42 };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.INT32);
      expect((result as { value: bigint }).value).toBe(42n);
    });

    it('should convert INT64 from string', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.INT64, value: '9007199254740993' };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.INT64);
      expect((result as { value: bigint }).value).toBe(9007199254740993n);
    });

    it('should handle undefined INT64 value', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.INT64 };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.INT64);
      expect((result as { value: bigint }).value).toBe(0n);
    });

    it('should convert FLOAT64 type', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.FLOAT64, value: 3.14159 };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.FLOAT64);
      expect((result as { value: number }).value).toBeCloseTo(3.14159);
    });

    it('should convert STRING type', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.STRING, value: 'hello' };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.STRING);
      expect((result as { value: string }).value).toBe('hello');
    });

    it('should convert BINARY from array', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.BINARY, value: [0, 1, 255] };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.BINARY);
      expect((result as { value: Uint8Array }).value).toBeInstanceOf(Uint8Array);
      expect(Array.from((result as { value: Uint8Array }).value)).toEqual([0, 1, 255]);
    });

    it('should convert TIMESTAMP from string', () => {
      const json: JsonTypedObjectValue = { type: ObjectType.TIMESTAMP, value: '1704067200000' };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.TIMESTAMP);
      expect((result as { value: bigint }).value).toBe(1704067200000n);
    });

    it('should convert GEO_POINT type', () => {
      const json: JsonTypedObjectValue = {
        type: ObjectType.GEO_POINT,
        value: { lat: 40.7128, lng: -74.006 },
      };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.GEO_POINT);
      const value = (result as { value: { lat: number; lng: number } }).value;
      expect(value.lat).toBeCloseTo(40.7128);
      expect(value.lng).toBeCloseTo(-74.006);
    });

    it('should fallback to NULL for unknown types', () => {
      const json = { type: 999 as ObjectType };
      const result = jsonToTypedObject(json);

      expect(result.type).toBe(ObjectType.NULL);
    });
  });

  // ============================================================================
  // Round-trip Tests (JSON)
  // ============================================================================

  describe('JSON round-trip conversion', () => {
    it('should round-trip all types correctly', () => {
      const testCases: TypedObject[] = [
        { type: ObjectType.NULL },
        { type: ObjectType.BOOL, value: true },
        { type: ObjectType.BOOL, value: false },
        { type: ObjectType.INT32, value: 42n },
        { type: ObjectType.INT64, value: 9007199254740993n },
        { type: ObjectType.FLOAT64, value: 3.14159 },
        { type: ObjectType.STRING, value: 'test string' },
        { type: ObjectType.DATE, value: 19722 },
        { type: ObjectType.DURATION, value: 'P1D' },
        { type: ObjectType.REF, value: 'https://example.com/entity/1' as EntityId },
        { type: ObjectType.URL, value: 'https://example.com/path' },
      ];

      for (const original of testCases) {
        const json = typedObjectToJson(original);
        const restored = jsonToTypedObject(json);

        expect(restored.type).toBe(original.type);
        if (original.type !== ObjectType.NULL) {
          // For bigint types, we need to handle string conversion
          if (
            original.type === ObjectType.INT32 ||
            original.type === ObjectType.INT64 ||
            original.type === ObjectType.TIMESTAMP
          ) {
            expect((restored as { value: bigint }).value).toBe(
              (original as { value: bigint }).value
            );
          } else {
            expect((restored as { value: unknown }).value).toEqual(
              (original as { value: unknown }).value
            );
          }
        }
      }
    });
  });

  // ============================================================================
  // typedObjectToSqlValue Tests
  // ============================================================================

  describe('typedObjectToSqlValue', () => {
    it('should convert NULL type', () => {
      const obj: TypedObject = { type: ObjectType.NULL };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.NULL);
      expect(result.obj_string).toBeUndefined();
      expect(result.obj_int64).toBeUndefined();
    });

    it('should convert BOOL type', () => {
      const trueObj: TypedObject = { type: ObjectType.BOOL, value: true };
      const falseObj: TypedObject = { type: ObjectType.BOOL, value: false };

      expect(typedObjectToSqlValue(trueObj).obj_bool).toBe(1);
      expect(typedObjectToSqlValue(falseObj).obj_bool).toBe(0);
    });

    it('should convert INT32 to number', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: 42n };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.INT32);
      expect(result.obj_int64).toBe(42);
    });

    it('should convert INT64 to number', () => {
      const obj: TypedObject = { type: ObjectType.INT64, value: 12345678n };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.INT64);
      expect(result.obj_int64).toBe(12345678);
    });

    it('should convert STRING type', () => {
      const obj: TypedObject = { type: ObjectType.STRING, value: 'test' };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.STRING);
      expect(result.obj_string).toBe('test');
    });

    it('should convert BINARY type', () => {
      const obj: TypedObject = { type: ObjectType.BINARY, value: new Uint8Array([1, 2, 3]) };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.BINARY);
      expect(result.obj_binary).toEqual(new Uint8Array([1, 2, 3]));
    });

    it('should convert TIMESTAMP type', () => {
      const obj: TypedObject = { type: ObjectType.TIMESTAMP, value: 1704067200000n };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.TIMESTAMP);
      expect(result.obj_timestamp).toBe(1704067200000n);
    });

    it('should convert DATE type to int64', () => {
      const obj: TypedObject = { type: ObjectType.DATE, value: 19722 };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.DATE);
      expect(result.obj_int64).toBe(19722);
    });

    it('should convert DURATION type to string', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P1Y2M3D' };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.DURATION);
      expect(result.obj_string).toBe('P1Y2M3D');
    });

    it('should convert REF type', () => {
      const obj: TypedObject = {
        type: ObjectType.REF,
        value: 'https://example.com/entity/1' as EntityId,
      };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.REF);
      expect(result.obj_ref).toBe('https://example.com/entity/1');
    });

    it('should convert REF_ARRAY to binary JSON', () => {
      const obj: TypedObject = {
        type: ObjectType.REF_ARRAY,
        value: ['https://a.com/1' as EntityId, 'https://b.com/2' as EntityId],
      };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.REF_ARRAY);
      expect(result.obj_binary).toBeInstanceOf(Uint8Array);

      const decoded = JSON.parse(new TextDecoder().decode(result.obj_binary!));
      expect(decoded).toEqual(['https://a.com/1', 'https://b.com/2']);
    });

    it('should convert JSON type to binary', () => {
      const obj: TypedObject = { type: ObjectType.JSON, value: { foo: 'bar' } };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.JSON);
      expect(result.obj_binary).toBeInstanceOf(Uint8Array);

      const decoded = JSON.parse(new TextDecoder().decode(result.obj_binary!));
      expect(decoded).toEqual({ foo: 'bar' });
    });

    it('should convert GEO_POINT type', () => {
      const obj: TypedObject = { type: ObjectType.GEO_POINT, value: { lat: 40.7, lng: -74.0 } };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.GEO_POINT);
      expect(result.obj_lat).toBeCloseTo(40.7);
      expect(result.obj_lng).toBeCloseTo(-74.0);
    });

    it('should convert URL type to string', () => {
      const obj: TypedObject = { type: ObjectType.URL, value: 'https://example.com' };
      const result = typedObjectToSqlValue(obj);

      expect(result.obj_type).toBe(ObjectType.URL);
      expect(result.obj_string).toBe('https://example.com');
    });
  });

  // ============================================================================
  // sqlValueToTypedObject Tests
  // ============================================================================

  describe('sqlValueToTypedObject', () => {
    it('should convert NULL type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.NULL };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.NULL);
    });

    it('should convert BOOL type from 0/1', () => {
      const trueRow: SqlRowInput = { obj_type: ObjectType.BOOL, obj_bool: 1 };
      const falseRow: SqlRowInput = { obj_type: ObjectType.BOOL, obj_bool: 0 };

      expect((sqlValueToTypedObject(trueRow) as { value: boolean }).value).toBe(true);
      expect((sqlValueToTypedObject(falseRow) as { value: boolean }).value).toBe(false);
    });

    it('should convert INT32 from number', () => {
      const row: SqlRowInput = { obj_type: ObjectType.INT32, obj_int64: 42 };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.INT32);
      expect((result as { value: bigint }).value).toBe(42n);
    });

    it('should convert INT64 from bigint', () => {
      const row: SqlRowInput = { obj_type: ObjectType.INT64, obj_int64: 9007199254740993n };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.INT64);
      expect((result as { value: bigint }).value).toBe(9007199254740993n);
    });

    it('should convert FLOAT64 type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.FLOAT64, obj_float64: 3.14159 };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.FLOAT64);
      expect((result as { value: number }).value).toBeCloseTo(3.14159);
    });

    it('should convert STRING type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.STRING, obj_string: 'hello' };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.STRING);
      expect((result as { value: string }).value).toBe('hello');
    });

    it('should convert BINARY type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.BINARY, obj_binary: new Uint8Array([1, 2]) };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.BINARY);
      expect(Array.from((result as { value: Uint8Array }).value)).toEqual([1, 2]);
    });

    it('should convert TIMESTAMP from bigint', () => {
      const row: SqlRowInput = { obj_type: ObjectType.TIMESTAMP, obj_timestamp: 1704067200000n };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.TIMESTAMP);
      expect((result as { value: bigint }).value).toBe(1704067200000n);
    });

    it('should convert DATE type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.DATE, obj_int64: 19722 };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.DATE);
      expect((result as { value: number }).value).toBe(19722);
    });

    it('should convert DURATION type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.DURATION, obj_string: 'P1D' };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.DURATION);
      expect((result as { value: string }).value).toBe('P1D');
    });

    it('should convert REF type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.REF, obj_ref: 'https://example.com/1' };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.REF);
      expect((result as { value: EntityId }).value).toBe('https://example.com/1');
    });

    it('should convert REF_ARRAY from binary JSON', () => {
      const refs = ['https://a.com/1', 'https://b.com/2'];
      const row: SqlRowInput = {
        obj_type: ObjectType.REF_ARRAY,
        obj_binary: new TextEncoder().encode(JSON.stringify(refs)),
      };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.REF_ARRAY);
      expect((result as { value: EntityId[] }).value).toEqual(refs);
    });

    it('should convert JSON from binary', () => {
      const jsonValue = { foo: 'bar', nested: [1, 2, 3] };
      const row: SqlRowInput = {
        obj_type: ObjectType.JSON,
        obj_binary: new TextEncoder().encode(JSON.stringify(jsonValue)),
      };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.JSON);
      expect((result as { value: unknown }).value).toEqual(jsonValue);
    });

    it('should convert GEO_POINT type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.GEO_POINT, obj_lat: 40.7, obj_lng: -74.0 };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.GEO_POINT);
      const value = (result as { value: { lat: number; lng: number } }).value;
      expect(value.lat).toBeCloseTo(40.7);
      expect(value.lng).toBeCloseTo(-74.0);
    });

    it('should convert URL type', () => {
      const row: SqlRowInput = { obj_type: ObjectType.URL, obj_string: 'https://example.com' };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.URL);
      expect((result as { value: string }).value).toBe('https://example.com');
    });

    it('should fallback to NULL for unknown types', () => {
      const row: SqlRowInput = { obj_type: 999 };
      const result = sqlValueToTypedObject(row);

      expect(result.type).toBe(ObjectType.NULL);
    });

    it('should handle missing values gracefully', () => {
      const stringRow: SqlRowInput = { obj_type: ObjectType.STRING };
      const result = sqlValueToTypedObject(stringRow);

      expect(result.type).toBe(ObjectType.STRING);
      expect((result as { value: string }).value).toBe('');
    });
  });

  // ============================================================================
  // Round-trip Tests (SQL)
  // ============================================================================

  describe('SQL round-trip conversion', () => {
    it('should round-trip all types correctly', () => {
      const testCases: TypedObject[] = [
        { type: ObjectType.NULL },
        { type: ObjectType.BOOL, value: true },
        { type: ObjectType.INT32, value: 42n },
        { type: ObjectType.INT64, value: 12345678n },
        { type: ObjectType.FLOAT64, value: 3.14159 },
        { type: ObjectType.STRING, value: 'test' },
        { type: ObjectType.BINARY, value: new Uint8Array([1, 2, 3]) },
        { type: ObjectType.DATE, value: 19722 },
        { type: ObjectType.DURATION, value: 'P1D' },
        { type: ObjectType.REF, value: 'https://example.com/1' as EntityId },
        { type: ObjectType.URL, value: 'https://example.com' },
        { type: ObjectType.GEO_POINT, value: { lat: 40.7, lng: -74.0 } },
      ];

      for (const original of testCases) {
        const sqlValue = typedObjectToSqlValue(original);
        const restored = sqlValueToTypedObject(sqlValue);

        expect(restored.type).toBe(original.type);
      }
    });
  });

  // ============================================================================
  // extractJsonValue Tests
  // ============================================================================

  describe('extractJsonValue', () => {
    it('should extract NULL as null', () => {
      const obj: TypedObject = { type: ObjectType.NULL };
      expect(extractJsonValue(obj)).toBeNull();
    });

    it('should extract BOOL value directly', () => {
      const obj: TypedObject = { type: ObjectType.BOOL, value: true };
      expect(extractJsonValue(obj)).toBe(true);
    });

    it('should extract INT64 as bigint', () => {
      const obj: TypedObject = { type: ObjectType.INT64, value: 42n };
      expect(extractJsonValue(obj)).toBe(42n);
    });

    it('should extract REF with wrapping when option is set', () => {
      const obj: TypedObject = {
        type: ObjectType.REF,
        value: 'https://example.com/1' as EntityId,
      };

      expect(extractJsonValue(obj)).toBe('https://example.com/1');
      expect(extractJsonValue(obj, { wrapRefs: true })).toEqual({
        '@ref': 'https://example.com/1',
      });
    });
  });

  // ============================================================================
  // Helper Function Tests
  // ============================================================================

  describe('getValueColumn', () => {
    it('should return correct column for STRING types', () => {
      expect(getValueColumn(ObjectType.STRING)).toBe('obj_string');
      expect(getValueColumn(ObjectType.DURATION)).toBe('obj_string');
      expect(getValueColumn(ObjectType.URL)).toBe('obj_string');
    });

    it('should return correct column for INT types', () => {
      expect(getValueColumn(ObjectType.INT32)).toBe('obj_int64');
      expect(getValueColumn(ObjectType.INT64)).toBe('obj_int64');
      expect(getValueColumn(ObjectType.DATE)).toBe('obj_int64');
    });

    it('should return correct column for FLOAT64', () => {
      expect(getValueColumn(ObjectType.FLOAT64)).toBe('obj_float64');
    });

    it('should return correct column for BOOL', () => {
      expect(getValueColumn(ObjectType.BOOL)).toBe('obj_bool');
    });

    it('should return correct column for TIMESTAMP', () => {
      expect(getValueColumn(ObjectType.TIMESTAMP)).toBe('obj_timestamp');
    });

    it('should return correct column for REF', () => {
      expect(getValueColumn(ObjectType.REF)).toBe('obj_ref');
    });

    it('should default to obj_string for unknown types', () => {
      expect(getValueColumn(ObjectType.NULL)).toBe('obj_string');
      expect(getValueColumn(ObjectType.JSON)).toBe('obj_string');
    });
  });

  describe('inferObjectTypeFromValue', () => {
    it('should infer NULL for null/undefined', () => {
      expect(inferObjectTypeFromValue(null)).toBe(ObjectType.NULL);
      expect(inferObjectTypeFromValue(undefined)).toBe(ObjectType.NULL);
    });

    it('should infer BOOL for boolean', () => {
      expect(inferObjectTypeFromValue(true)).toBe(ObjectType.BOOL);
      expect(inferObjectTypeFromValue(false)).toBe(ObjectType.BOOL);
    });

    it('should infer STRING for string', () => {
      expect(inferObjectTypeFromValue('hello')).toBe(ObjectType.STRING);
    });

    it('should infer INT64 for integer numbers', () => {
      expect(inferObjectTypeFromValue(42)).toBe(ObjectType.INT64);
    });

    it('should infer FLOAT64 for non-integer numbers', () => {
      expect(inferObjectTypeFromValue(3.14)).toBe(ObjectType.FLOAT64);
    });

    it('should infer INT64 for bigint', () => {
      expect(inferObjectTypeFromValue(42n)).toBe(ObjectType.INT64);
    });

    it('should infer BINARY for Uint8Array', () => {
      expect(inferObjectTypeFromValue(new Uint8Array([1, 2]))).toBe(ObjectType.BINARY);
    });

    it('should infer TIMESTAMP for Date', () => {
      expect(inferObjectTypeFromValue(new Date())).toBe(ObjectType.TIMESTAMP);
    });

    it('should infer JSON for objects and arrays', () => {
      expect(inferObjectTypeFromValue({ foo: 'bar' })).toBe(ObjectType.JSON);
      expect(inferObjectTypeFromValue([1, 2, 3])).toBe(ObjectType.JSON);
    });
  });

  describe('getObjectTypeName', () => {
    it('should return correct names for all types', () => {
      expect(getObjectTypeName(ObjectType.NULL)).toBe('NULL');
      expect(getObjectTypeName(ObjectType.BOOL)).toBe('BOOL');
      expect(getObjectTypeName(ObjectType.INT32)).toBe('INT32');
      expect(getObjectTypeName(ObjectType.INT64)).toBe('INT64');
      expect(getObjectTypeName(ObjectType.FLOAT64)).toBe('FLOAT64');
      expect(getObjectTypeName(ObjectType.STRING)).toBe('STRING');
      expect(getObjectTypeName(ObjectType.BINARY)).toBe('BINARY');
      expect(getObjectTypeName(ObjectType.TIMESTAMP)).toBe('TIMESTAMP');
      expect(getObjectTypeName(ObjectType.DATE)).toBe('DATE');
      expect(getObjectTypeName(ObjectType.DURATION)).toBe('DURATION');
      expect(getObjectTypeName(ObjectType.REF)).toBe('REF');
      expect(getObjectTypeName(ObjectType.REF_ARRAY)).toBe('REF_ARRAY');
      expect(getObjectTypeName(ObjectType.JSON)).toBe('JSON');
      expect(getObjectTypeName(ObjectType.GEO_POINT)).toBe('GEO_POINT');
      expect(getObjectTypeName(ObjectType.GEO_POLYGON)).toBe('GEO_POLYGON');
      expect(getObjectTypeName(ObjectType.GEO_LINESTRING)).toBe('GEO_LINESTRING');
      expect(getObjectTypeName(ObjectType.URL)).toBe('URL');
    });

    it('should return UNKNOWN for invalid types', () => {
      expect(getObjectTypeName(999 as ObjectType)).toBe('UNKNOWN');
    });
  });
});
