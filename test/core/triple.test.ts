import { describe, it, expect } from 'vitest';
import {
  type Triple,
  type TypedObject,
  isValidTypedObject,
  validateTriple,
  createTriple,
  inferObjectType,
  extractValue,
} from '../../src/core/triple';
import {
  ObjectType,
  createEntityId,
  createPredicate,
  createTransactionId,
} from '../../src/core/types';
import type { GeoPoint, GeoPolygon, GeoLineString } from '../../src/core/geo';

describe('TypedObject validation', () => {
  describe('isValidTypedObject', () => {
    describe('NULL type', () => {
      it('should return true for NULL type with no values set', () => {
        const obj: TypedObject = { type: ObjectType.NULL };
        expect(isValidTypedObject(obj)).toBe(true);
      });
    });

    describe('BOOL type', () => {
      it('should return true for BOOL type with boolean value', () => {
        const obj: TypedObject = { type: ObjectType.BOOL, value: true };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for BOOL type with false value', () => {
        const obj: TypedObject = { type: ObjectType.BOOL, value: false };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for BOOL type without value', () => {
        const obj: TypedObject = { type: ObjectType.BOOL };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('INT32 type', () => {
      it('should return true for INT32 type with bigint value', () => {
        const obj: TypedObject = { type: ObjectType.INT32, value: 42n };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for INT32 type without value', () => {
        const obj: TypedObject = { type: ObjectType.INT32 };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for INT32 type with value out of 32-bit range', () => {
        const obj: TypedObject = {
          type: ObjectType.INT32,
          value: BigInt(2 ** 32),
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('INT64 type', () => {
      it('should return true for INT64 type with bigint value', () => {
        const obj: TypedObject = {
          type: ObjectType.INT64,
          value: BigInt('9223372036854775807'),
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for INT64 type without value', () => {
        const obj: TypedObject = { type: ObjectType.INT64 };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('FLOAT64 type', () => {
      it('should return true for FLOAT64 type with number value', () => {
        const obj: TypedObject = { type: ObjectType.FLOAT64, value: 3.14 };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for FLOAT64 type with zero', () => {
        const obj: TypedObject = { type: ObjectType.FLOAT64, value: 0 };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for FLOAT64 type without value', () => {
        const obj: TypedObject = { type: ObjectType.FLOAT64 };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for FLOAT64 type with NaN', () => {
        const obj: TypedObject = { type: ObjectType.FLOAT64, value: NaN };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('STRING type', () => {
      it('should return true for STRING type with string value', () => {
        const obj: TypedObject = {
          type: ObjectType.STRING,
          value: 'hello',
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for STRING type with empty string', () => {
        const obj: TypedObject = { type: ObjectType.STRING, value: '' };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for STRING type without value', () => {
        const obj: TypedObject = { type: ObjectType.STRING };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('BINARY type', () => {
      it('should return true for BINARY type with Uint8Array', () => {
        const obj: TypedObject = {
          type: ObjectType.BINARY,
          value: new Uint8Array([1, 2, 3]),
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for BINARY type with empty Uint8Array', () => {
        const obj: TypedObject = {
          type: ObjectType.BINARY,
          value: new Uint8Array([]),
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for BINARY type without value', () => {
        const obj: TypedObject = { type: ObjectType.BINARY };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('TIMESTAMP type', () => {
      it('should return true for TIMESTAMP type with positive bigint', () => {
        const obj: TypedObject = {
          type: ObjectType.TIMESTAMP,
          value: BigInt(Date.now()),
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for TIMESTAMP type without value', () => {
        const obj: TypedObject = { type: ObjectType.TIMESTAMP };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for TIMESTAMP type with negative value', () => {
        const obj: TypedObject = {
          type: ObjectType.TIMESTAMP,
          value: -1n,
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('DATE type', () => {
      it('should return true for DATE type with days since epoch', () => {
        const obj: TypedObject = { type: ObjectType.DATE, value: 19745 };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for DATE type without value', () => {
        const obj: TypedObject = { type: ObjectType.DATE };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('DURATION type', () => {
      it('should return true for DURATION type with ISO 8601 duration', () => {
        const obj: TypedObject = {
          type: ObjectType.DURATION,
          value: 'P1Y2M3D',
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for DURATION type with time duration', () => {
        const obj: TypedObject = {
          type: ObjectType.DURATION,
          value: 'PT1H30M',
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for DURATION type without value', () => {
        const obj: TypedObject = { type: ObjectType.DURATION };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for DURATION type with invalid format', () => {
        const obj: TypedObject = {
          type: ObjectType.DURATION,
          value: 'invalid',
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('REF type', () => {
      it('should return true for REF type with valid EntityId', () => {
        const obj: TypedObject = {
          type: ObjectType.REF,
          value: createEntityId('https://example.com/entity/123'),
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for REF type without value', () => {
        const obj: TypedObject = { type: ObjectType.REF };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for REF type with invalid EntityId format', () => {
        const obj: TypedObject = {
          type: ObjectType.REF,
          value: 'not-a-url' as any,
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('REF_ARRAY type', () => {
      it('should return true for REF_ARRAY type with array of valid EntityIds', () => {
        const obj: TypedObject = {
          type: ObjectType.REF_ARRAY,
          value: [
            createEntityId('https://example.com/entity/1'),
            createEntityId('https://example.com/entity/2'),
          ],
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for REF_ARRAY type with empty array', () => {
        const obj: TypedObject = { type: ObjectType.REF_ARRAY, value: [] };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for REF_ARRAY type without value', () => {
        const obj: TypedObject = { type: ObjectType.REF_ARRAY };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for REF_ARRAY type with invalid EntityId in array', () => {
        const obj: TypedObject = {
          type: ObjectType.REF_ARRAY,
          value: [
            createEntityId('https://example.com/entity/1'),
            'not-a-url' as any,
          ],
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('JSON type', () => {
      it('should return true for JSON type with object', () => {
        const obj: TypedObject = {
          type: ObjectType.JSON,
          value: { key: 'value' },
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for JSON type with array', () => {
        const obj: TypedObject = {
          type: ObjectType.JSON,
          value: [1, 2, 3],
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for JSON type with null', () => {
        const obj: TypedObject = { type: ObjectType.JSON, value: null };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for JSON type without value (undefined)', () => {
        const obj: TypedObject = { type: ObjectType.JSON };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('GEO_POINT type', () => {
      it('should return true for GEO_POINT type with valid coordinates', () => {
        const obj: TypedObject = {
          type: ObjectType.GEO_POINT,
          value: { lat: 37.7749, lng: -122.4194 },
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for GEO_POINT type without value', () => {
        const obj: TypedObject = { type: ObjectType.GEO_POINT };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for GEO_POINT type with invalid coordinates', () => {
        const obj: TypedObject = {
          type: ObjectType.GEO_POINT,
          value: { lat: 100, lng: 0 },
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('GEO_POLYGON type', () => {
      it('should return true for GEO_POLYGON type with valid polygon', () => {
        const obj: TypedObject = {
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
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for GEO_POLYGON type without value', () => {
        const obj: TypedObject = { type: ObjectType.GEO_POLYGON };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('GEO_LINESTRING type', () => {
      it('should return true for GEO_LINESTRING type with valid line', () => {
        const obj: TypedObject = {
          type: ObjectType.GEO_LINESTRING,
          value: {
            points: [
              { lat: 0, lng: 0 },
              { lat: 1, lng: 1 },
            ],
          },
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for GEO_LINESTRING type without value', () => {
        const obj: TypedObject = { type: ObjectType.GEO_LINESTRING };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('URL type', () => {
      it('should return true for URL type with valid URL', () => {
        const obj: TypedObject = {
          type: ObjectType.URL,
          value: 'https://example.com/page',
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for URL type without value', () => {
        const obj: TypedObject = { type: ObjectType.URL };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for URL type with invalid URL', () => {
        const obj: TypedObject = { type: ObjectType.URL, value: 'not-a-url' };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });

    describe('VECTOR type', () => {
      it('should return true for VECTOR type with valid number array', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: [0.1, 0.2, 0.3, 0.4],
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return true for VECTOR type with empty array', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: [],
        };
        expect(isValidTypedObject(obj)).toBe(true);
      });

      it('should return false for VECTOR type without value', () => {
        const obj: TypedObject = { type: ObjectType.VECTOR };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for VECTOR type with non-array value', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: 'not-an-array' as any,
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for VECTOR type with non-number elements', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: ['a', 'b', 'c'] as any,
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for VECTOR type with NaN elements', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: [0.1, NaN, 0.3],
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });

      it('should return false for VECTOR type with Infinity elements', () => {
        const obj: TypedObject = {
          type: ObjectType.VECTOR,
          value: [0.1, Infinity, 0.3],
        };
        expect(isValidTypedObject(obj)).toBe(false);
      });
    });
  });
});

describe('Triple validation', () => {
  describe('validateTriple', () => {
    const validSubject = createEntityId('https://example.com/entity/123');
    const validPredicate = createPredicate('name');
    const validTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
    const validTimestamp = BigInt(Date.now());

    it('should return valid for complete triple', () => {
      const triple: Triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: validTxId,
      };

      const result = validateTriple(triple);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return invalid if subject is missing', () => {
      const triple = {
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain('subject is required');
    });

    it('should return invalid if subject is not a valid EntityId', () => {
      const triple = {
        subject: 'not-a-url' as any,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('subject'))).toBe(true);
    });

    it('should return invalid if predicate is missing', () => {
      const triple = {
        subject: validSubject,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain('predicate is required');
    });

    it('should return invalid if predicate contains colon', () => {
      const triple = {
        subject: validSubject,
        predicate: 'schema:name' as any,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(
        result.errors.some((e) => e.includes('predicate') && e.includes('colon'))
      ).toBe(true);
    });

    it('should return invalid if object is missing', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain('object is required');
    });

    it('should return invalid if object is invalid TypedObject', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING }, // missing value
        timestamp: validTimestamp,
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('object'))).toBe(true);
    });

    it('should return invalid if timestamp is missing', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        txId: validTxId,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain('timestamp is required');
    });

    it('should return invalid if timestamp is not a positive bigint', () => {
      const triple: Triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: -1n,
        txId: validTxId,
      };

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(
        result.errors.some((e) => e.includes('timestamp') && e.includes('positive'))
      ).toBe(true);
    });

    it('should return invalid if txId is missing', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors).toContain('txId is required');
    });

    it('should return invalid if txId is not a valid TransactionId', () => {
      const triple = {
        subject: validSubject,
        predicate: validPredicate,
        object: { type: ObjectType.STRING, value: 'John' },
        timestamp: validTimestamp,
        txId: 'invalid-tx-id' as any,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors.some((e) => e.includes('txId'))).toBe(true);
    });

    it('should return multiple errors for triple with multiple issues', () => {
      const triple = {
        subject: 'not-a-url' as any,
        predicate: 'schema:name' as any,
        object: { type: ObjectType.STRING },
        timestamp: -1n,
        txId: 'invalid' as any,
      } as Triple;

      const result = validateTriple(triple);
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(1);
    });
  });
});

describe('createTriple', () => {
  const validSubject = createEntityId('https://example.com/entity/123');
  const validPredicate = createPredicate('name');
  const validTxId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');

  it('should create triple from string value', () => {
    const triple = createTriple(validSubject, validPredicate, 'John', validTxId);

    expect(triple.subject).toBe(validSubject);
    expect(triple.predicate).toBe(validPredicate);
    expect(triple.object.type).toBe(ObjectType.STRING);
    expect(triple.object.value).toBe('John');
    expect(triple.txId).toBe(validTxId);
    expect(typeof triple.timestamp).toBe('bigint');
    expect(triple.timestamp > 0n).toBe(true);
  });

  it('should create triple from boolean value', () => {
    const triple = createTriple(validSubject, validPredicate, true, validTxId);

    expect(triple.object.type).toBe(ObjectType.BOOL);
    expect(triple.object.value).toBe(true);
  });

  it('should create triple from number value (float)', () => {
    const triple = createTriple(validSubject, validPredicate, 3.14, validTxId);

    expect(triple.object.type).toBe(ObjectType.FLOAT64);
    expect(triple.object.value).toBe(3.14);
  });

  it('should create triple from integer number value', () => {
    const triple = createTriple(validSubject, validPredicate, 42, validTxId);

    // Integer numbers should be stored as INT64 (or INT32 if small enough)
    expect([ObjectType.INT32, ObjectType.INT64, ObjectType.FLOAT64]).toContain(
      triple.object.type
    );
  });

  it('should create triple from bigint value', () => {
    const triple = createTriple(
      validSubject,
      validPredicate,
      BigInt('9223372036854775807'),
      validTxId
    );

    expect(triple.object.type).toBe(ObjectType.INT64);
    expect(triple.object.value).toBe(BigInt('9223372036854775807'));
  });

  it('should create triple from Date value', () => {
    const date = new Date('2024-01-15');
    const triple = createTriple(validSubject, validPredicate, date, validTxId);

    expect(triple.object.type).toBe(ObjectType.TIMESTAMP);
    expect(triple.object.value).toBe(BigInt(date.getTime()));
  });

  it('should create triple from Uint8Array value', () => {
    const binary = new Uint8Array([1, 2, 3]);
    const triple = createTriple(validSubject, validPredicate, binary, validTxId);

    expect(triple.object.type).toBe(ObjectType.BINARY);
    expect(triple.object.value).toEqual(binary);
  });

  it('should create triple from null value', () => {
    const triple = createTriple(validSubject, validPredicate, null, validTxId);

    expect(triple.object.type).toBe(ObjectType.NULL);
  });

  it('should create triple from object value (JSON)', () => {
    const obj = { key: 'value', nested: { a: 1 } };
    const triple = createTriple(validSubject, validPredicate, obj, validTxId);

    expect(triple.object.type).toBe(ObjectType.JSON);
    expect(triple.object.value).toEqual(obj);
  });

  it('should create triple from array value (JSON)', () => {
    const arr = [1, 2, 3];
    const triple = createTriple(validSubject, validPredicate, arr, validTxId);

    expect(triple.object.type).toBe(ObjectType.JSON);
    expect(triple.object.value).toEqual(arr);
  });
});

describe('inferObjectType', () => {
  it('should infer NULL for null', () => {
    expect(inferObjectType(null)).toBe(ObjectType.NULL);
  });

  it('should infer NULL for undefined', () => {
    expect(inferObjectType(undefined)).toBe(ObjectType.NULL);
  });

  it('should infer BOOL for boolean', () => {
    expect(inferObjectType(true)).toBe(ObjectType.BOOL);
    expect(inferObjectType(false)).toBe(ObjectType.BOOL);
  });

  it('should infer FLOAT64 for number', () => {
    expect(inferObjectType(3.14)).toBe(ObjectType.FLOAT64);
    expect(inferObjectType(42)).toBe(ObjectType.FLOAT64);
    expect(inferObjectType(0)).toBe(ObjectType.FLOAT64);
    expect(inferObjectType(-123.456)).toBe(ObjectType.FLOAT64);
  });

  it('should infer INT64 for bigint', () => {
    expect(inferObjectType(42n)).toBe(ObjectType.INT64);
    expect(inferObjectType(BigInt('9223372036854775807'))).toBe(ObjectType.INT64);
  });

  it('should infer STRING for string', () => {
    expect(inferObjectType('hello')).toBe(ObjectType.STRING);
    expect(inferObjectType('')).toBe(ObjectType.STRING);
  });

  it('should infer BINARY for Uint8Array', () => {
    expect(inferObjectType(new Uint8Array([1, 2, 3]))).toBe(ObjectType.BINARY);
    expect(inferObjectType(new Uint8Array())).toBe(ObjectType.BINARY);
  });

  it('should infer TIMESTAMP for Date', () => {
    expect(inferObjectType(new Date())).toBe(ObjectType.TIMESTAMP);
    expect(inferObjectType(new Date('2024-01-01'))).toBe(ObjectType.TIMESTAMP);
  });

  it('should infer JSON for object', () => {
    expect(inferObjectType({ key: 'value' })).toBe(ObjectType.JSON);
    expect(inferObjectType({})).toBe(ObjectType.JSON);
  });

  it('should infer JSON for array', () => {
    expect(inferObjectType([1, 2, 3])).toBe(ObjectType.JSON);
    expect(inferObjectType([])).toBe(ObjectType.JSON);
  });
});

describe('extractValue', () => {
  it('should extract null for NULL type', () => {
    const obj: TypedObject = { type: ObjectType.NULL };
    expect(extractValue(obj)).toBe(null);
  });

  it('should extract boolean for BOOL type', () => {
    expect(extractValue({ type: ObjectType.BOOL, value: true })).toBe(true);
    expect(extractValue({ type: ObjectType.BOOL, value: false })).toBe(false);
  });

  it('should extract bigint for INT32 type', () => {
    const obj: TypedObject = { type: ObjectType.INT32, value: 42n };
    expect(extractValue(obj)).toBe(42n);
  });

  it('should extract bigint for INT64 type', () => {
    const obj: TypedObject = {
      type: ObjectType.INT64,
      value: BigInt('9223372036854775807'),
    };
    expect(extractValue(obj)).toBe(BigInt('9223372036854775807'));
  });

  it('should extract number for FLOAT64 type', () => {
    const obj: TypedObject = { type: ObjectType.FLOAT64, value: 3.14 };
    expect(extractValue(obj)).toBe(3.14);
  });

  it('should extract string for STRING type', () => {
    const obj: TypedObject = { type: ObjectType.STRING, value: 'hello' };
    expect(extractValue(obj)).toBe('hello');
  });

  it('should extract Uint8Array for BINARY type', () => {
    const binary = new Uint8Array([1, 2, 3]);
    const obj: TypedObject = { type: ObjectType.BINARY, value: binary };
    expect(extractValue(obj)).toEqual(binary);
  });

  it('should extract bigint for TIMESTAMP type', () => {
    const timestamp = BigInt(Date.now());
    const obj: TypedObject = {
      type: ObjectType.TIMESTAMP,
      value: timestamp,
    };
    expect(extractValue(obj)).toBe(timestamp);
  });

  it('should extract number for DATE type', () => {
    const obj: TypedObject = { type: ObjectType.DATE, value: 19745 };
    expect(extractValue(obj)).toBe(19745);
  });

  it('should extract string for DURATION type', () => {
    const obj: TypedObject = {
      type: ObjectType.DURATION,
      value: 'P1Y2M3D',
    };
    expect(extractValue(obj)).toBe('P1Y2M3D');
  });

  it('should extract EntityId for REF type', () => {
    const entityId = createEntityId('https://example.com/entity/123');
    const obj: TypedObject = { type: ObjectType.REF, value: entityId };
    expect(extractValue(obj)).toBe(entityId);
  });

  it('should extract array of EntityIds for REF_ARRAY type', () => {
    const value = [
      createEntityId('https://example.com/entity/1'),
      createEntityId('https://example.com/entity/2'),
    ];
    const obj: TypedObject = { type: ObjectType.REF_ARRAY, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract value for JSON type', () => {
    const value = { key: 'value', nested: { a: 1 } };
    const obj: TypedObject = { type: ObjectType.JSON, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract GeoPoint for GEO_POINT type', () => {
    const value: GeoPoint = { lat: 37.7749, lng: -122.4194 };
    const obj: TypedObject = { type: ObjectType.GEO_POINT, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract GeoPolygon for GEO_POLYGON type', () => {
    const value: GeoPolygon = {
      exterior: [
        { lat: 0, lng: 0 },
        { lat: 0, lng: 1 },
        { lat: 1, lng: 1 },
        { lat: 1, lng: 0 },
        { lat: 0, lng: 0 },
      ],
    };
    const obj: TypedObject = { type: ObjectType.GEO_POLYGON, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract GeoLineString for GEO_LINESTRING type', () => {
    const value: GeoLineString = {
      points: [
        { lat: 0, lng: 0 },
        { lat: 1, lng: 1 },
      ],
    };
    const obj: TypedObject = { type: ObjectType.GEO_LINESTRING, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract string for URL type', () => {
    const obj: TypedObject = {
      type: ObjectType.URL,
      value: 'https://example.com/page',
    };
    expect(extractValue(obj)).toBe('https://example.com/page');
  });

  it('should extract number array for VECTOR type', () => {
    const value = [0.1, 0.2, 0.3, 0.4];
    const obj: TypedObject = { type: ObjectType.VECTOR, value };
    expect(extractValue(obj)).toEqual(value);
  });

  it('should extract empty array for VECTOR type with empty array', () => {
    const obj: TypedObject = { type: ObjectType.VECTOR, value: [] };
    expect(extractValue(obj)).toEqual([]);
  });
});
