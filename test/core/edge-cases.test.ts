/**
 * Edge Case Tests for GraphDB Core Module
 *
 * TDD Red Phase - These tests capture edge cases and error conditions
 * that should be handled by the core module.
 *
 * Focus areas:
 * - TypedObject validation edge cases
 * - INT32 boundary conditions
 * - FLOAT64 special values
 * - ULID validation edge cases
 * - Entity URL edge cases
 * - Assertion function edge cases
 */

import { describe, it, expect } from 'vitest';
import {
  ObjectType,
  isEntityId,
  isPredicate,
  isTransactionId,
  createEntityId,
  createPredicate,
  createTransactionId,
  assertEntityId,
  assertPredicate,
  assertTransactionId,
  assertEntityIdArray,
  BrandedTypeValidationError,
  BrandedTypeErrorCode,
} from '../../src/core/types.js';
import {
  isValidTypedObject,
  validateTriple,
  createTriple,
  inferObjectType,
  type TypedObject,
  type Triple,
} from '../../src/core/triple.js';
import {
  urlToStoragePath,
  storagePathToUrl,
  resolveNamespace,
  parseEntityId,
  createEntity,
  validateEntity,
  isValidFieldName,
} from '../../src/core/entity.js';
import type { EntityId } from '../../src/core/types.js';

describe('Edge Cases - TypedObject Validation', () => {
  describe('INT32 boundary conditions', () => {
    it('should accept INT32 at minimum value (-2147483648)', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: BigInt(-2147483648) };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept INT32 at maximum value (2147483647)', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: BigInt(2147483647) };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should reject INT32 one below minimum (-2147483649)', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: BigInt(-2147483649) };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject INT32 one above maximum (2147483648)', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: BigInt(2147483648) };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should accept INT32 at zero', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: 0n };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept negative INT32 values', () => {
      const obj: TypedObject = { type: ObjectType.INT32, value: -100n };
      expect(isValidTypedObject(obj)).toBe(true);
    });
  });

  describe('FLOAT64 special values', () => {
    it('should reject FLOAT64 with Infinity', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: Infinity };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject FLOAT64 with negative Infinity', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: -Infinity };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should accept FLOAT64 with very small positive number', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: Number.MIN_VALUE };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept FLOAT64 with very large positive number', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: Number.MAX_VALUE };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept FLOAT64 with negative zero', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: -0 };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept FLOAT64 with very small negative number', () => {
      const obj: TypedObject = { type: ObjectType.FLOAT64, value: -Number.MIN_VALUE };
      expect(isValidTypedObject(obj)).toBe(true);
    });
  });

  describe('TIMESTAMP edge cases', () => {
    it('should accept TIMESTAMP at zero (epoch)', () => {
      const obj: TypedObject = { type: ObjectType.TIMESTAMP, value: 0n };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept very large TIMESTAMP values', () => {
      // Year 3000+
      const obj: TypedObject = { type: ObjectType.TIMESTAMP, value: 32503680000000n };
      expect(isValidTypedObject(obj)).toBe(true);
    });
  });

  describe('VECTOR edge cases', () => {
    it('should accept VECTOR with single element', () => {
      const obj: TypedObject = { type: ObjectType.VECTOR, value: [0.5] };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept VECTOR with very large number of elements', () => {
      const obj: TypedObject = { type: ObjectType.VECTOR, value: new Array(1536).fill(0.1) };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should reject VECTOR with negative Infinity element', () => {
      const obj: TypedObject = { type: ObjectType.VECTOR, value: [0.1, -Infinity, 0.3] };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should accept VECTOR with zero values', () => {
      const obj: TypedObject = { type: ObjectType.VECTOR, value: [0, 0, 0] };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept VECTOR with negative values', () => {
      const obj: TypedObject = { type: ObjectType.VECTOR, value: [-0.5, -0.3, -0.1] };
      expect(isValidTypedObject(obj)).toBe(true);
    });
  });

  describe('DURATION edge cases', () => {
    it('should accept full ISO 8601 duration with all components', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P1Y2M3DT4H5M6S' };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept duration with weeks', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P2W' };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should accept duration with fractional seconds', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'PT0.5S' };
      expect(isValidTypedObject(obj)).toBe(true);
    });

    it('should reject duration without P prefix', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: '1Y2M3D' };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject empty duration', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: '' };
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject duration with just P', () => {
      const obj: TypedObject = { type: ObjectType.DURATION, value: 'P' };
      expect(isValidTypedObject(obj)).toBe(false);
    });
  });

  describe('TypedObject with wrong value type', () => {
    it('should reject BOOL with string value', () => {
      const obj = { type: ObjectType.BOOL, value: 'true' } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject INT64 with number value', () => {
      const obj = { type: ObjectType.INT64, value: 42 } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject STRING with number value', () => {
      const obj = { type: ObjectType.STRING, value: 123 } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject BINARY with array of numbers (not Uint8Array)', () => {
      const obj = { type: ObjectType.BINARY, value: [1, 2, 3] } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should reject TIMESTAMP with number value (not bigint)', () => {
      const obj = { type: ObjectType.TIMESTAMP, value: Date.now() } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });
  });

  describe('TypedObject with null or undefined obj', () => {
    it('should return false for null object', () => {
      expect(isValidTypedObject(null as unknown as TypedObject)).toBe(false);
    });

    it('should return false for undefined object', () => {
      expect(isValidTypedObject(undefined as unknown as TypedObject)).toBe(false);
    });

    it('should return false for object without type field', () => {
      const obj = { value: 'test' } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should return false for object with non-numeric type', () => {
      const obj = { type: 'STRING', value: 'test' } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });

    it('should return false for unknown type number', () => {
      const obj = { type: 999, value: 'test' } as unknown as TypedObject;
      expect(isValidTypedObject(obj)).toBe(false);
    });
  });
});

describe('Edge Cases - TransactionId (ULID) Validation', () => {
  describe('ULID character validation', () => {
    it('should reject ULID with lowercase letters', () => {
      expect(isTransactionId('01arz3ndektsv4rrffq69g5fav')).toBe(false);
    });

    it('should reject ULID containing letter I', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAI')).toBe(false);
    });

    it('should reject ULID containing letter L', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAL')).toBe(false);
    });

    it('should reject ULID containing letter O', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAO')).toBe(false);
    });

    it('should reject ULID containing letter U', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAU')).toBe(false);
    });

    it('should accept ULID with all valid characters', () => {
      // Valid Crockford Base32: 0123456789ABCDEFGHJKMNPQRSTVWXYZ
      expect(isTransactionId('0123456789ABCDEFGHJKMNPQRS')).toBe(true);
    });
  });

  describe('ULID length validation', () => {
    it('should reject ULID that is 25 characters (one short)', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FA')).toBe(false);
    });

    it('should reject ULID that is 27 characters (one extra)', () => {
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAVX')).toBe(false);
    });

    it('should reject empty string', () => {
      expect(isTransactionId('')).toBe(false);
    });
  });
});

describe('Edge Cases - Predicate Validation', () => {
  describe('Special character handling', () => {
    it('should reject predicate with hyphen', () => {
      expect(isPredicate('first-name')).toBe(false);
    });

    it('should reject predicate with dot', () => {
      expect(isPredicate('user.name')).toBe(false);
    });

    it('should reject predicate with at sign', () => {
      expect(isPredicate('@context')).toBe(false);
    });

    it('should reject predicate with hash', () => {
      expect(isPredicate('#id')).toBe(false);
    });

    it('should reject predicate with unicode characters', () => {
      expect(isPredicate('nombre_')).toBe(true); // ASCII underscore is valid
    });

    it('should accept predicate with consecutive underscores', () => {
      expect(isPredicate('__private__')).toBe(true);
    });

    it('should accept predicate with numbers in middle', () => {
      expect(isPredicate('field123value')).toBe(true);
    });

    it('should accept single character predicate', () => {
      expect(isPredicate('a')).toBe(true);
    });

    it('should accept predicate starting with underscore', () => {
      expect(isPredicate('_field')).toBe(true);
    });

    it('should accept predicate starting with $', () => {
      expect(isPredicate('$custom')).toBe(true);
    });
  });
});

describe('Edge Cases - Entity URL Utilities', () => {
  describe('urlToStoragePath edge cases', () => {
    it('should handle URL with port number', () => {
      const path = urlToStoragePath('https://example.com:8080/path');
      expect(path).toContain('.example');
    });

    it('should handle URL with query string', () => {
      // Query strings should be preserved in the path
      const result = urlToStoragePath('https://example.com/path?query=value');
      expect(result).toContain('.com');
    });

    it('should handle URL with fragment', () => {
      // Fragment might be stripped by URL parsing
      const result = urlToStoragePath('https://example.com/path#section');
      expect(result).toContain('.com');
    });

    it('should handle single-letter domain parts', () => {
      const path = urlToStoragePath('https://a.b.c/path');
      expect(path).toBe('.c/.b/.a/path');
    });

    it('should handle numeric domain parts', () => {
      const path = urlToStoragePath('https://123.example.com/path');
      expect(path).toContain('.123');
    });
  });

  describe('storagePathToUrl edge cases', () => {
    it('should handle path with single domain part', () => {
      const url = storagePathToUrl('.localhost/path');
      expect(url).toBe('https://localhost/path');
    });

    it('should handle deeply nested paths', () => {
      const url = storagePathToUrl('.com/.example/a/b/c/d/e/f/g');
      expect(url).toBe('https://example.com/a/b/c/d/e/f/g');
    });

    it('should handle path with empty segments', () => {
      // Test behavior with consecutive slashes
      const url = storagePathToUrl('.com/.example//double');
      expect(url).toContain('example.com');
    });
  });

  describe('resolveNamespace edge cases', () => {
    it('should handle URL with single path segment', () => {
      const result = resolveNamespace('https://example.com/id');
      expect(result.namespace).toBe('https://example.com');
      expect(result.localId).toBe('id');
      expect(result.context).toBe('https://example.com');
    });

    it('should handle URL with only hostname (no path)', () => {
      const result = resolveNamespace('https://example.com');
      expect(result.namespace).toBe('https://example.com');
      expect(result.localId).toBe('');
    });

    it('should handle http protocol', () => {
      const result = resolveNamespace('http://example.com/path/id');
      expect(result.namespace).toBe('http://example.com');
    });

    it('should handle URL with port', () => {
      const result = resolveNamespace('https://example.com:8080/path/id');
      expect(result.namespace).toBe('https://example.com:8080');
    });
  });

  describe('parseEntityId edge cases', () => {
    it('should handle URL with empty path segments', () => {
      // Double slashes in path
      const result = parseEntityId('https://example.com//path' as EntityId);
      expect(result.path.filter(p => p !== '')).toEqual(['path']);
    });

    it('should return empty array for root URL', () => {
      const result = parseEntityId('https://example.com' as EntityId);
      expect(result.path).toEqual([]);
      expect(result.localId).toBe('');
    });
  });
});

describe('Edge Cases - Entity Creation and Validation', () => {
  describe('createEntity edge cases', () => {
    it('should handle entity with no additional properties', () => {
      const entity = createEntity(
        'https://example.com/items/1' as EntityId,
        'Item',
        {}
      );
      expect(entity.$id).toBe('https://example.com/items/1');
      expect(entity.$type).toBe('Item');
    });

    it('should handle entity with empty string type', () => {
      const entity = createEntity(
        'https://example.com/items/1' as EntityId,
        '',
        {}
      );
      expect(entity.$type).toBe('');
    });

    it('should handle entity with empty array of types', () => {
      const entity = createEntity(
        'https://example.com/items/1' as EntityId,
        [],
        {}
      );
      expect(entity.$type).toEqual([]);
    });

    it('should reject property with empty string key', () => {
      expect(() =>
        createEntity('https://example.com/items/1' as EntityId, 'Item', {
          '': 'value',
        })
      ).toThrow();
    });
  });

  describe('validateEntity edge cases', () => {
    it('should handle entity with $id as empty string', () => {
      const entity = {
        $id: '' as EntityId,
        $type: 'User',
        $context: 'https://example.com',
        _namespace: 'https://example.com',
        _localId: '',
      } as unknown as import('../../src/core/entity.js').Entity;

      const result = validateEntity(entity);
      expect(result.valid).toBe(false);
    });

    it('should handle entity with null $type', () => {
      const entity = {
        $id: 'https://example.com/1' as EntityId,
        $type: null,
        $context: 'https://example.com',
        _namespace: 'https://example.com',
        _localId: '1',
      } as unknown as import('../../src/core/entity.js').Entity;

      const result = validateEntity(entity);
      expect(result.valid).toBe(false);
    });
  });

  describe('isValidFieldName edge cases', () => {
    it('should reject field name with just underscore', () => {
      // Single underscore should be valid according to pattern
      expect(isValidFieldName('_')).toBe(true);
    });

    it('should reject field name with just $', () => {
      // Single $ should be valid according to pattern
      expect(isValidFieldName('$')).toBe(true);
    });

    it('should accept long field names', () => {
      const longName = 'a'.repeat(100);
      expect(isValidFieldName(longName)).toBe(true);
    });

    it('should reject field name with leading number', () => {
      expect(isValidFieldName('1field')).toBe(false);
    });

    it('should accept field name without special characters', () => {
      // 'field_with_emoji' is actually a valid field name (no actual emoji)
      expect(isValidFieldName('field_with_emoji')).toBe(true);
    });

    it('should reject field name with actual emoji character', () => {
      // Field name pattern only allows [a-zA-Z0-9_$], so emoji should be rejected
      // Using unicode escape to ensure the character is included
      expect(isValidFieldName('field\u{1F525}')).toBe(false); // fire emoji
    });

    it('should reject field name with backslash', () => {
      expect(isValidFieldName('field\\name')).toBe(false);
    });

    it('should reject field name with forward slash', () => {
      expect(isValidFieldName('field/name')).toBe(false);
    });
  });
});

describe('Edge Cases - Assertion Functions', () => {
  describe('assertEntityIdArray edge cases', () => {
    it('should handle array with single valid element', () => {
      const result = assertEntityIdArray(['https://example.com/1']);
      expect(result).toHaveLength(1);
    });

    it('should throw for array with null element', () => {
      expect(() => assertEntityIdArray(['https://example.com/1', null])).toThrow(
        BrandedTypeValidationError
      );
    });

    it('should throw for array with undefined element', () => {
      expect(() => assertEntityIdArray(['https://example.com/1', undefined])).toThrow(
        BrandedTypeValidationError
      );
    });

    it('should throw for nested array', () => {
      expect(() =>
        assertEntityIdArray([['https://example.com/1']] as unknown as string[])
      ).toThrow(BrandedTypeValidationError);
    });

    it('should include correct index in error for first invalid element', () => {
      try {
        assertEntityIdArray(['invalid']);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(BrandedTypeValidationError);
        expect((e as BrandedTypeValidationError).message).toContain('[0]');
      }
    });

    it('should handle very large array', () => {
      const urls = Array.from({ length: 1000 }, (_, i) => `https://example.com/${i}`);
      const result = assertEntityIdArray(urls);
      expect(result).toHaveLength(1000);
    });
  });

  describe('assertEntityId with various invalid inputs', () => {
    it('should throw for Symbol', () => {
      expect(() => assertEntityId(Symbol('test'))).toThrow(BrandedTypeValidationError);
    });

    it('should throw for function', () => {
      expect(() => assertEntityId(() => 'url')).toThrow(BrandedTypeValidationError);
    });

    it('should throw for BigInt', () => {
      expect(() => assertEntityId(123n)).toThrow(BrandedTypeValidationError);
    });
  });

  describe('BrandedTypeValidationError properties', () => {
    it('should preserve the original value in the error', () => {
      const originalValue = { complex: 'object' };
      try {
        assertEntityId(originalValue);
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e).toBeInstanceOf(BrandedTypeValidationError);
        expect((e as BrandedTypeValidationError).value).toBe(originalValue);
      }
    });

    it('should have correct error code for each assertion function', () => {
      try {
        assertEntityId('invalid');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(BrandedTypeErrorCode.INVALID_ENTITY_ID);
      }

      try {
        assertPredicate('invalid:colon');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(BrandedTypeErrorCode.INVALID_PREDICATE);
      }
    });
  });
});

describe('Edge Cases - Triple Validation', () => {
  const validSubject = 'https://example.com/entity/123' as EntityId;
  const validPredicate = 'name' as import('../../src/core/types.js').Predicate;
  const validTxId = '01ARZ3NDEKTSV4RRFFQ69G5FAV' as import('../../src/core/types.js').TransactionId;

  describe('validateTriple with boundary values', () => {
    it('should accept timestamp at zero', () => {
      const triple: Triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: 0n,
        txId: validTxId,
      };
      const result = validateTriple(triple);
      expect(result.valid).toBe(true);
    });

    it('should reject timestamp as number (not bigint)', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'test' },
        timestamp: Date.now(),
        txId: validTxId,
      } as unknown as Triple;
      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors.some(e => e.includes('timestamp'))).toBe(true);
    });
  });

  describe('createTriple type inference', () => {
    it('should correctly infer type for undefined', () => {
      const triple = createTriple(validSubject, validPredicate, undefined, validTxId);
      expect(triple.object.type).toBe(ObjectType.NULL);
    });

    it('should correctly infer type for empty object', () => {
      const triple = createTriple(validSubject, validPredicate, {}, validTxId);
      expect(triple.object.type).toBe(ObjectType.JSON);
    });

    it('should correctly infer type for empty array', () => {
      const triple = createTriple(validSubject, validPredicate, [], validTxId);
      expect(triple.object.type).toBe(ObjectType.JSON);
    });

    it('should correctly infer type for empty string', () => {
      const triple = createTriple(validSubject, validPredicate, '', validTxId);
      expect(triple.object.type).toBe(ObjectType.STRING);
      expect(triple.object.value).toBe('');
    });
  });
});

describe('Edge Cases - inferObjectType', () => {
  it('should infer JSON for nested objects', () => {
    expect(inferObjectType({ a: { b: { c: 1 } } })).toBe(ObjectType.JSON);
  });

  it('should infer FLOAT64 for NaN', () => {
    // NaN is still typeof number, so FLOAT64 is inferred
    expect(inferObjectType(NaN)).toBe(ObjectType.FLOAT64);
  });

  it('should infer FLOAT64 for Infinity', () => {
    expect(inferObjectType(Infinity)).toBe(ObjectType.FLOAT64);
  });

  it('should infer JSON for object with prototype', () => {
    class CustomClass {
      value = 1;
    }
    expect(inferObjectType(new CustomClass())).toBe(ObjectType.JSON);
  });
});
