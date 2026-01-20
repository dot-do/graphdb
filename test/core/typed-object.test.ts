/**
 * Type-level tests for TypedObject discriminated union
 *
 * These tests verify that TypeScript properly narrows types
 * when using switch statements on the discriminant field.
 */

import { describe, it, expect } from 'vitest';
import {
  type TypedObject,
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
  extractValue,
} from '../../src/core/triple';
import { ObjectType, createEntityId } from '../../src/core/types';
import type { GeoPoint, GeoPolygon, GeoLineString } from '../../src/core/geo';

describe('TypedObject discriminated union type narrowing', () => {
  describe('type narrowing with switch statement', () => {
    it('should narrow to NullTypedObject for NULL type', () => {
      const obj: TypedObject = { type: ObjectType.NULL };

      // Type narrowing in switch
      switch (obj.type) {
        case ObjectType.NULL: {
          // TypeScript should know obj is NullTypedObject here
          // This would fail to compile if TypedObject was not a proper discriminated union
          const narrowed: NullTypedObject = obj;
          expect(narrowed.type).toBe(ObjectType.NULL);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to BoolTypedObject and access value field for BOOL type', () => {
      const obj: TypedObject = { type: ObjectType.BOOL, value: true };

      switch (obj.type) {
        case ObjectType.BOOL: {
          // TypeScript should narrow to BoolTypedObject
          // and know that obj.value is boolean
          const narrowed: BoolTypedObject = obj;
          const value: boolean = narrowed.value;
          expect(value).toBe(true);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to Int32TypedObject and access value field for INT32 type', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: 42n };

      switch (obj.type) {
        case ObjectType.INT32: {
          const narrowed: Int32TypedObject = obj;
          const value: bigint = narrowed.value;
          expect(value).toBe(42n);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to Int64TypedObject and access value field for INT64 type', () => {
      const obj: TypedObject = {
        type: ObjectType.INT64,
        value: BigInt('9223372036854775807'),
      };

      switch (obj.type) {
        case ObjectType.INT64: {
          const narrowed: Int64TypedObject = obj;
          const value: bigint = narrowed.value;
          expect(value).toBe(BigInt('9223372036854775807'));
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to Float64TypedObject and access value field for FLOAT64 type', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: 3.14 };

      switch (obj.type) {
        case ObjectType.FLOAT64: {
          const narrowed: Float64TypedObject = obj;
          const value: number = narrowed.value;
          expect(value).toBe(3.14);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to StringTypedObject and access value field for STRING type', () => {
      const obj: TypedObject = { type: ObjectType.STRING, value: 'hello' };

      switch (obj.type) {
        case ObjectType.STRING: {
          const narrowed: StringTypedObject = obj;
          const value: string = narrowed.value;
          expect(value).toBe('hello');
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to BinaryTypedObject and access value field for BINARY type', () => {
      const binary = new Uint8Array([1, 2, 3]);
      const obj: TypedObject = { type: ObjectType.BINARY, value: binary };

      switch (obj.type) {
        case ObjectType.BINARY: {
          const narrowed: BinaryTypedObject = obj;
          const value: Uint8Array = narrowed.value;
          expect(value).toEqual(binary);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to TimestampTypedObject and access value field for TIMESTAMP type', () => {
      const timestamp = BigInt(Date.now());
      const obj: TypedObject = { type: ObjectType.TIMESTAMP, value: timestamp };

      switch (obj.type) {
        case ObjectType.TIMESTAMP: {
          const narrowed: TimestampTypedObject = obj;
          const value: bigint = narrowed.value;
          expect(value).toBe(timestamp);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to DateTypedObject and access value field for DATE type', () => {
      const obj: TypedObject = { type: ObjectType.DATE, value: 19745 };

      switch (obj.type) {
        case ObjectType.DATE: {
          const narrowed: DateTypedObject = obj;
          const value: number = narrowed.value;
          expect(value).toBe(19745);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to DurationTypedObject and access value field for DURATION type', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P1Y2M3D' };

      switch (obj.type) {
        case ObjectType.DURATION: {
          const narrowed: DurationTypedObject = obj;
          const value: string = narrowed.value;
          expect(value).toBe('P1Y2M3D');
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to RefTypedObject and access value field for REF type', () => {
      const entityId = createEntityId('https://example.com/entity/123');
      const obj: TypedObject = { type: ObjectType.REF, value: entityId };

      switch (obj.type) {
        case ObjectType.REF: {
          const narrowed: RefTypedObject = obj;
          // EntityId is a branded string type
          const value: string = narrowed.value;
          expect(value).toBe(entityId);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to RefArrayTypedObject and access value field for REF_ARRAY type', () => {
      const refArray = [
        createEntityId('https://example.com/entity/1'),
        createEntityId('https://example.com/entity/2'),
      ];
      const obj: TypedObject = { type: ObjectType.REF_ARRAY, value: refArray };

      switch (obj.type) {
        case ObjectType.REF_ARRAY: {
          const narrowed: RefArrayTypedObject = obj;
          const value: string[] = narrowed.value;
          expect(value).toEqual(refArray);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to JsonTypedObject and access value field for JSON type', () => {
      const jsonValue = { key: 'value', nested: { a: 1 } };
      const obj: TypedObject = { type: ObjectType.JSON, value: jsonValue };

      switch (obj.type) {
        case ObjectType.JSON: {
          const narrowed: JsonTypedObject = obj;
          const value: unknown = narrowed.value;
          expect(value).toEqual(jsonValue);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to GeoPointTypedObject and access value field for GEO_POINT type', () => {
      const geoPoint: GeoPoint = { lat: 37.7749, lng: -122.4194 };
      const obj: TypedObject = { type: ObjectType.GEO_POINT, value: geoPoint };

      switch (obj.type) {
        case ObjectType.GEO_POINT: {
          const narrowed: GeoPointTypedObject = obj;
          const value: GeoPoint = narrowed.value;
          expect(value).toEqual(geoPoint);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to GeoPolygonTypedObject and access value field for GEO_POLYGON type', () => {
      const geoPolygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 0, lng: 1 },
          { lat: 1, lng: 1 },
          { lat: 1, lng: 0 },
          { lat: 0, lng: 0 },
        ],
      };
      const obj: TypedObject = { type: ObjectType.GEO_POLYGON, value: geoPolygon };

      switch (obj.type) {
        case ObjectType.GEO_POLYGON: {
          const narrowed: GeoPolygonTypedObject = obj;
          const value: GeoPolygon = narrowed.value;
          expect(value).toEqual(geoPolygon);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to GeoLineStringTypedObject and access value field for GEO_LINESTRING type', () => {
      const geoLineString: GeoLineString = {
        points: [
          { lat: 0, lng: 0 },
          { lat: 1, lng: 1 },
        ],
      };
      const obj: TypedObject = {
        type: ObjectType.GEO_LINESTRING,
        value: geoLineString,
      };

      switch (obj.type) {
        case ObjectType.GEO_LINESTRING: {
          const narrowed: GeoLineStringTypedObject = obj;
          const value: GeoLineString = narrowed.value;
          expect(value).toEqual(geoLineString);
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });

    it('should narrow to UrlTypedObject and access value field for URL type', () => {
      const obj: TypedObject = {
        type: ObjectType.URL,
        value: 'https://example.com/page',
      };

      switch (obj.type) {
        case ObjectType.URL: {
          const narrowed: UrlTypedObject = obj;
          const value: string = narrowed.value;
          expect(value).toBe('https://example.com/page');
          break;
        }
        default:
          throw new Error('Should not reach default case');
      }
    });
  });

  describe('exhaustive type checking', () => {
    /**
     * Helper function to extract value from TypedObject
     * This demonstrates that the discriminated union enables exhaustive type checking
     */
    function getValueDescription(obj: TypedObject): string {
      switch (obj.type) {
        case ObjectType.NULL:
          return 'null value';
        case ObjectType.BOOL:
          return `boolean: ${obj.value}`;
        case ObjectType.INT32:
          return `int32: ${obj.value}`;
        case ObjectType.INT64:
          return `int64: ${obj.value}`;
        case ObjectType.FLOAT64:
          return `float64: ${obj.value}`;
        case ObjectType.STRING:
          return `string: ${obj.value}`;
        case ObjectType.BINARY:
          return `binary: ${obj.value.length} bytes`;
        case ObjectType.TIMESTAMP:
          return `timestamp: ${obj.value}`;
        case ObjectType.DATE:
          return `date: ${obj.value}`;
        case ObjectType.DURATION:
          return `duration: ${obj.value}`;
        case ObjectType.REF:
          return `ref: ${obj.value}`;
        case ObjectType.REF_ARRAY:
          return `ref_array: ${obj.value.length} refs`;
        case ObjectType.JSON:
          return `json: ${JSON.stringify(obj.value)}`;
        case ObjectType.GEO_POINT:
          return `geo_point: (${obj.value.lat}, ${obj.value.lng})`;
        case ObjectType.GEO_POLYGON:
          return `geo_polygon: ${obj.value.exterior.length} points`;
        case ObjectType.GEO_LINESTRING:
          return `geo_linestring: ${obj.value.points.length} points`;
        case ObjectType.URL:
          return `url: ${obj.value}`;
        default:
          // This ensures exhaustive checking - TypeScript should error if a case is missing
          const _exhaustive: never = obj;
          throw new Error(`Unexpected type: ${(_exhaustive as TypedObject).type}`);
      }
    }

    it('should handle all TypedObject variants exhaustively', () => {
      const testCases: Array<{ obj: TypedObject; expected: string }> = [
        { obj: { type: ObjectType.NULL }, expected: 'null value' },
        { obj: { type: ObjectType.BOOL, value: true }, expected: 'boolean: true' },
        { obj: { type: ObjectType.INT32, value: 42n }, expected: 'int32: 42' },
        {
          obj: { type: ObjectType.INT64, value: 9007199254740991n },
          expected: 'int64: 9007199254740991',
        },
        {
          obj: { type: ObjectType.FLOAT64, value: 3.14 },
          expected: 'float64: 3.14',
        },
        {
          obj: { type: ObjectType.STRING, value: 'hello' },
          expected: 'string: hello',
        },
        {
          obj: { type: ObjectType.BINARY, value: new Uint8Array([1, 2, 3]) },
          expected: 'binary: 3 bytes',
        },
        {
          obj: { type: ObjectType.TIMESTAMP, value: 1705320000000n },
          expected: 'timestamp: 1705320000000',
        },
        { obj: { type: ObjectType.DATE, value: 19745 }, expected: 'date: 19745' },
        {
          obj: { type: ObjectType.DURATION, value: 'P1Y2M3D' },
          expected: 'duration: P1Y2M3D',
        },
        {
          obj: {
            type: ObjectType.REF,
            value: createEntityId('https://example.com/entity/123'),
          },
          expected: 'ref: https://example.com/entity/123',
        },
        {
          obj: {
            type: ObjectType.REF_ARRAY,
            value: [
              createEntityId('https://example.com/entity/1'),
              createEntityId('https://example.com/entity/2'),
            ],
          },
          expected: 'ref_array: 2 refs',
        },
        {
          obj: { type: ObjectType.JSON, value: { key: 'value' } },
          expected: 'json: {"key":"value"}',
        },
        {
          obj: { type: ObjectType.GEO_POINT, value: { lat: 37.77, lng: -122.42 } },
          expected: 'geo_point: (37.77, -122.42)',
        },
        {
          obj: {
            type: ObjectType.GEO_POLYGON,
            value: {
              exterior: [
                { lat: 0, lng: 0 },
                { lat: 0, lng: 1 },
                { lat: 1, lng: 1 },
                { lat: 1, lng: 0 },
                { lat: 0, lng: 0 },
              ],
            },
          },
          expected: 'geo_polygon: 5 points',
        },
        {
          obj: {
            type: ObjectType.GEO_LINESTRING,
            value: {
              points: [
                { lat: 0, lng: 0 },
                { lat: 1, lng: 1 },
              ],
            },
          },
          expected: 'geo_linestring: 2 points',
        },
        {
          obj: { type: ObjectType.URL, value: 'https://example.com' },
          expected: 'url: https://example.com',
        },
      ];

      for (const { obj, expected } of testCases) {
        expect(getValueDescription(obj)).toBe(expected);
      }
    });
  });

  describe('type safety - invalid combinations should not compile', () => {
    /**
     * These tests verify that the discriminated union prevents invalid combinations
     * at compile time. The tests themselves pass if they compile, as they're
     * demonstrating that valid combinations work.
     *
     * Invalid combinations would cause TypeScript compilation errors, which is
     * the desired behavior.
     */

    it('should allow creating valid TypedObject combinations', () => {
      // All these should compile without errors
      const nullObj: TypedObject = { type: ObjectType.NULL };
      const boolObj: TypedObject = { type: ObjectType.BOOL, value: true };
      const int32Obj: TypedObject = { type: ObjectType.INT32, value: 42n };
      const int64Obj: TypedObject = { type: ObjectType.INT64, value: 42n };
      const float64Obj: TypedObject = { type: ObjectType.FLOAT64, value: 3.14 };
      const stringObj: TypedObject = { type: ObjectType.STRING, value: 'test' };
      const binaryObj: TypedObject = {
        type: ObjectType.BINARY,
        value: new Uint8Array([1, 2, 3]),
      };
      const timestampObj: TypedObject = {
        type: ObjectType.TIMESTAMP,
        value: BigInt(Date.now()),
      };
      const dateObj: TypedObject = { type: ObjectType.DATE, value: 19745 };
      const durationObj: TypedObject = {
        type: ObjectType.DURATION,
        value: 'P1Y',
      };
      const refObj: TypedObject = {
        type: ObjectType.REF,
        value: createEntityId('https://example.com/entity'),
      };
      const refArrayObj: TypedObject = {
        type: ObjectType.REF_ARRAY,
        value: [createEntityId('https://example.com/entity')],
      };
      const jsonObj: TypedObject = {
        type: ObjectType.JSON,
        value: { key: 'value' },
      };
      const geoPointObj: TypedObject = {
        type: ObjectType.GEO_POINT,
        value: { lat: 0, lng: 0 },
      };
      const geoPolygonObj: TypedObject = {
        type: ObjectType.GEO_POLYGON,
        value: {
          exterior: [
            { lat: 0, lng: 0 },
            { lat: 0, lng: 1 },
            { lat: 1, lng: 1 },
            { lat: 1, lng: 0 },
            { lat: 0, lng: 0 },
          ],
        },
      };
      const geoLineStringObj: TypedObject = {
        type: ObjectType.GEO_LINESTRING,
        value: {
          points: [
            { lat: 0, lng: 0 },
            { lat: 1, lng: 1 },
          ],
        },
      };
      const urlObj: TypedObject = {
        type: ObjectType.URL,
        value: 'https://example.com',
      };

      // All objects should be valid
      expect(nullObj).toBeDefined();
      expect(boolObj).toBeDefined();
      expect(int32Obj).toBeDefined();
      expect(int64Obj).toBeDefined();
      expect(float64Obj).toBeDefined();
      expect(stringObj).toBeDefined();
      expect(binaryObj).toBeDefined();
      expect(timestampObj).toBeDefined();
      expect(dateObj).toBeDefined();
      expect(durationObj).toBeDefined();
      expect(refObj).toBeDefined();
      expect(refArrayObj).toBeDefined();
      expect(jsonObj).toBeDefined();
      expect(geoPointObj).toBeDefined();
      expect(geoPolygonObj).toBeDefined();
      expect(geoLineStringObj).toBeDefined();
      expect(urlObj).toBeDefined();
    });

    // Note: The following would be compile-time errors if uncommented:
    // - { type: ObjectType.STRING, value: 123 } // value must be string
    // - { type: ObjectType.BOOL, value: 'true' } // value must be boolean
    // - { type: ObjectType.INT64, value: 3.14 } // value must be bigint
    // - { type: ObjectType.BOOL } // value is required for BOOL type
  });

  describe('value field access after narrowing', () => {
    it('should access value field directly after type guard', () => {
      const obj: TypedObject = { type: ObjectType.STRING, value: 'hello world' };

      if (obj.type === ObjectType.STRING) {
        // After type guard, TypeScript should know obj.value is string
        const length: number = obj.value.length;
        expect(length).toBe(11);
      }
    });

    it('should work with extractValue for all types', () => {
      // Test that extractValue returns the correct type for each variant
      const stringObj: TypedObject = { type: ObjectType.STRING, value: 'test' };
      const boolObj: TypedObject = { type: ObjectType.BOOL, value: false };
      const int64Obj: TypedObject = { type: ObjectType.INT64, value: 100n };

      expect(extractValue(stringObj)).toBe('test');
      expect(extractValue(boolObj)).toBe(false);
      expect(extractValue(int64Obj)).toBe(100n);
    });
  });
});
