/**
 * JSON Conversion Tests
 *
 * Tests for safe JSON conversion with runtime validation.
 * Addresses unchecked type assertions in JSON parsing/conversion code.
 *
 * Key scenarios:
 * - Validate JSON structure before conversion
 * - Handle unexpected types gracefully
 * - Provide helpful error messages
 */

import { describe, it, expect } from 'vitest';
import {
  parseTripleFromJson,
  parseTypedObjectFromJson,
  JsonConversionError,
  JsonConversionErrorCode,
  isValidTripleJson,
  isValidTypedObjectJson,
} from '../../src/core/json-conversion.js';
import { ObjectType } from '../../src/core/types.js';

describe('JSON Conversion', () => {
  describe('parseTypedObjectFromJson', () => {
    describe('should validate JSON structure before conversion', () => {
      it('should require type field', () => {
        const result = parseTypedObjectFromJson({});

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('type');
        }
      });

      it('should require type to be a number', () => {
        const result = parseTypedObjectFromJson({ type: 'STRING' });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('type');
        }
      });

      it('should reject invalid type enum values', () => {
        const result = parseTypedObjectFromJson({ type: 999 });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('unknown ObjectType');
        }
      });

      it('should accept valid NULL type', () => {
        const result = parseTypedObjectFromJson({ type: ObjectType.NULL });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.NULL);
        }
      });
    });

    describe('should handle unexpected types gracefully', () => {
      it('should reject non-boolean value for BOOL type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.BOOL,
          value: 'true',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('BOOL');
          expect(result.message).toContain('boolean');
        }
      });

      it('should reject non-number value for INT32 type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.INT32,
          value: 'not a number',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('INT32');
        }
      });

      it('should reject non-number value for FLOAT64 type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.FLOAT64,
          value: 'not a number',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('FLOAT64');
          expect(result.message).toContain('number');
        }
      });

      it('should reject non-string value for STRING type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.STRING,
          value: 123,
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('STRING');
          expect(result.message).toContain('string');
        }
      });

      it('should reject non-array value for BINARY type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.BINARY,
          value: 'not an array',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('BINARY');
          expect(result.message).toContain('array');
        }
      });

      it('should reject missing lat/lng for GEO_POINT type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.GEO_POINT,
          value: { lat: 40.7128 }, // missing lng
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('GEO_POINT');
        }
      });

      it('should reject non-array value for REF_ARRAY type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.REF_ARRAY,
          value: 'not an array',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('REF_ARRAY');
          expect(result.message).toContain('array');
        }
      });

      it('should handle null input gracefully', () => {
        const result = parseTypedObjectFromJson(null as unknown);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_INPUT);
        }
      });

      it('should handle undefined input gracefully', () => {
        const result = parseTypedObjectFromJson(undefined as unknown);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_INPUT);
        }
      });

      it('should handle array input gracefully', () => {
        const result = parseTypedObjectFromJson([1, 2, 3] as unknown);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_INPUT);
        }
      });
    });

    describe('should provide helpful error messages', () => {
      it('should include expected type in error message', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.STRING,
          value: 123,
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.message).toMatch(/expected.*string/i);
          expect(result.message).toMatch(/got.*number/i);
        }
      });

      it('should include field name in missing field error', () => {
        const result = parseTypedObjectFromJson({ value: 'test' });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.message).toContain('type');
        }
      });

      it('should include actual type in error for wrong type', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.BOOL,
          value: 42,
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.message).toContain('number');
        }
      });
    });

    describe('should successfully convert valid typed objects', () => {
      it('should convert valid BOOL', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.BOOL,
          value: true,
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.BOOL);
          expect(result.value).toBe(true);
        }
      });

      it('should convert valid INT32 from number', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.INT32,
          value: 42,
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.INT32);
          expect(result.value).toBe(42n);
        }
      });

      it('should convert valid INT64 from string', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.INT64,
          value: '9007199254740993', // Larger than Number.MAX_SAFE_INTEGER
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.INT64);
          expect(result.value).toBe(9007199254740993n);
        }
      });

      it('should convert valid FLOAT64', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.FLOAT64,
          value: 3.14159,
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.FLOAT64);
          expect(result.value).toBeCloseTo(3.14159);
        }
      });

      it('should convert valid STRING', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.STRING,
          value: 'hello world',
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.STRING);
          expect(result.value).toBe('hello world');
        }
      });

      it('should convert valid BINARY from number array', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.BINARY,
          value: [0, 1, 2, 255],
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.BINARY);
          expect(result.value).toBeInstanceOf(Uint8Array);
          expect(Array.from(result.value as Uint8Array)).toEqual([0, 1, 2, 255]);
        }
      });

      it('should convert valid TIMESTAMP', () => {
        const timestamp = Date.now();
        const result = parseTypedObjectFromJson({
          type: ObjectType.TIMESTAMP,
          value: timestamp,
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.TIMESTAMP);
          expect(result.value).toBe(BigInt(timestamp));
        }
      });

      it('should convert valid GEO_POINT', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.GEO_POINT,
          value: { lat: 40.7128, lng: -74.006 },
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.GEO_POINT);
          const value = result.value as { lat: number; lng: number };
          expect(value.lat).toBeCloseTo(40.7128);
          expect(value.lng).toBeCloseTo(-74.006);
        }
      });

      it('should convert valid REF', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.REF,
          value: 'https://example.com/entity/1',
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.REF);
          expect(result.value).toBe('https://example.com/entity/1');
        }
      });

      it('should convert valid REF_ARRAY', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.REF_ARRAY,
          value: ['https://example.com/entity/1', 'https://example.com/entity/2'],
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.REF_ARRAY);
          expect(result.value).toEqual([
            'https://example.com/entity/1',
            'https://example.com/entity/2',
          ]);
        }
      });

      it('should convert valid JSON type', () => {
        const jsonValue = { nested: { deeply: { value: [1, 2, 3] } } };
        const result = parseTypedObjectFromJson({
          type: ObjectType.JSON,
          value: jsonValue,
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.JSON);
          expect(result.value).toEqual(jsonValue);
        }
      });

      it('should convert valid URL', () => {
        const result = parseTypedObjectFromJson({
          type: ObjectType.URL,
          value: 'https://example.com/path?query=value',
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.type).toBe(ObjectType.URL);
          expect(result.value).toBe('https://example.com/path?query=value');
        }
      });
    });
  });

  describe('parseTripleFromJson', () => {
    const validTripleJson = {
      subject: 'https://example.com/entity/1',
      predicate: 'name',
      object: { type: ObjectType.STRING, value: 'Test Entity' },
      timestamp: Date.now(),
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
    };

    describe('should validate JSON structure before conversion', () => {
      it('should require subject field', () => {
        const { subject, ...without } = validTripleJson;
        const result = parseTripleFromJson(without);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('subject');
        }
      });

      it('should require predicate field', () => {
        const { predicate, ...without } = validTripleJson;
        const result = parseTripleFromJson(without);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('predicate');
        }
      });

      it('should require object field', () => {
        const { object, ...without } = validTripleJson;
        const result = parseTripleFromJson(without);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('object');
        }
      });

      it('should require timestamp field', () => {
        const { timestamp, ...without } = validTripleJson;
        const result = parseTripleFromJson(without);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('timestamp');
        }
      });

      it('should require txId field', () => {
        const { txId, ...without } = validTripleJson;
        const result = parseTripleFromJson(without);

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.MISSING_FIELD);
          expect(result.message).toContain('txId');
        }
      });
    });

    describe('should handle unexpected types gracefully', () => {
      it('should reject non-string subject', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          subject: 123,
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('subject');
        }
      });

      it('should reject non-string predicate', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          predicate: { field: 'name' },
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('predicate');
        }
      });

      it('should reject non-object object field', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          object: 'not an object',
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('object');
        }
      });

      it('should reject non-number/string timestamp', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          timestamp: { time: 123 },
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('timestamp');
        }
      });

      it('should reject non-string txId', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          txId: 12345,
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
          expect(result.message).toContain('txId');
        }
      });

      it('should propagate TypedObject conversion errors', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          object: { type: ObjectType.BOOL, value: 'not a boolean' },
        });

        expect(result).toBeInstanceOf(JsonConversionError);
        if (result instanceof JsonConversionError) {
          expect(result.code).toBe(JsonConversionErrorCode.INVALID_VALUE);
          expect(result.message).toContain('BOOL');
        }
      });
    });

    describe('should successfully convert valid triples', () => {
      it('should convert a valid triple', () => {
        const result = parseTripleFromJson(validTripleJson);

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.subject).toBe(validTripleJson.subject);
          expect(result.predicate).toBe(validTripleJson.predicate);
          expect(result.object.type).toBe(ObjectType.STRING);
          expect(result.timestamp).toBe(BigInt(validTripleJson.timestamp));
          expect(result.txId).toBe(validTripleJson.txId);
        }
      });

      it('should accept string timestamp', () => {
        const result = parseTripleFromJson({
          ...validTripleJson,
          timestamp: '1704067200000',
        });

        expect(result).not.toBeInstanceOf(JsonConversionError);
        if (!(result instanceof JsonConversionError)) {
          expect(result.timestamp).toBe(1704067200000n);
        }
      });
    });
  });

  describe('isValidTripleJson', () => {
    it('should return true for valid triple JSON', () => {
      expect(
        isValidTripleJson({
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          object: { type: ObjectType.STRING, value: 'test' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        })
      ).toBe(true);
    });

    it('should return false for missing fields', () => {
      expect(isValidTripleJson({ subject: 'test' })).toBe(false);
    });

    it('should return false for invalid object', () => {
      expect(
        isValidTripleJson({
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          object: { type: ObjectType.BOOL, value: 'not a boolean' },
          timestamp: Date.now(),
          txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        })
      ).toBe(false);
    });
  });

  describe('isValidTypedObjectJson', () => {
    it('should return true for valid typed object JSON', () => {
      expect(isValidTypedObjectJson({ type: ObjectType.NULL })).toBe(true);
      expect(isValidTypedObjectJson({ type: ObjectType.BOOL, value: true })).toBe(
        true
      );
      expect(
        isValidTypedObjectJson({ type: ObjectType.STRING, value: 'hello' })
      ).toBe(true);
    });

    it('should return false for invalid typed object JSON', () => {
      expect(isValidTypedObjectJson({})).toBe(false);
      expect(isValidTypedObjectJson({ type: 'STRING' })).toBe(false);
      expect(
        isValidTypedObjectJson({ type: ObjectType.BOOL, value: 'not boolean' })
      ).toBe(false);
    });
  });

  describe('JsonConversionError', () => {
    it('should have proper error properties', () => {
      const error = new JsonConversionError(
        JsonConversionErrorCode.INVALID_TYPE,
        'Test error message'
      );

      expect(error).toBeInstanceOf(Error);
      expect(error.name).toBe('JsonConversionError');
      expect(error.code).toBe(JsonConversionErrorCode.INVALID_TYPE);
      expect(error.message).toBe('Test error message');
    });

    it('should be serializable to JSON response', () => {
      const error = new JsonConversionError(
        JsonConversionErrorCode.MISSING_FIELD,
        'Field "subject" is required'
      );

      const response = error.toResponse();
      expect(response).toEqual({
        type: 'error',
        code: JsonConversionErrorCode.MISSING_FIELD,
        message: 'Field "subject" is required',
      });
    });
  });
});
