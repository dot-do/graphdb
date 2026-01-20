/**
 * Row Parser Tests - Type-Safe SQL Row Parsing
 *
 * Tests for type-safe parsing of SQL query results instead of
 * relying on `any` types and direct casts.
 *
 * @see src/shard/row-parsers.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  parseTripleRow,
  parseChunkRow,
  type TripleRow,
  type ChunkRow,
  RowParseError,
} from '../../src/shard/row-parsers.js';
import { rowToTriple } from '../../src/shard/crud.js';
import { ObjectType } from '../../src/core/types.js';

describe('Row Parsers', () => {
  describe('parseTripleRow', () => {
    it('should parse triple row with all fields typed', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_ref: null,
        obj_string: 'Test Name',
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: null,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.id).toBe(1);
      expect(row.subject).toBe('https://example.com/entity/1');
      expect(row.predicate).toBe('name');
      expect(row.objType).toBe(ObjectType.STRING);
      expect(row.objString).toBe('Test Name');
      expect(row.timestamp).toBe(BigInt(1700000000000));
      expect(row.txId).toBe('01ARZ3NDEKTSV4RRFFQ69G5FAV');
    });

    it('should parse triple row with bigint timestamp', () => {
      const rawRow = {
        id: 2,
        subject: 'https://example.com/entity/2',
        predicate: 'createdAt',
        obj_type: ObjectType.TIMESTAMP,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: BigInt(1700000000000),
        obj_lat: null,
        obj_lng: null,
        obj_binary: null,
        timestamp: BigInt(1700000000000),
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objTimestamp).toBe(BigInt(1700000000000));
      expect(row.timestamp).toBe(BigInt(1700000000000));
    });

    it('should parse triple row with INT64 value', () => {
      const rawRow = {
        id: 3,
        subject: 'https://example.com/entity/3',
        predicate: 'count',
        obj_type: ObjectType.INT64,
        obj_ref: null,
        obj_string: null,
        obj_int64: BigInt(9007199254740993), // Beyond MAX_SAFE_INTEGER
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: null,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objInt64).toBe(BigInt(9007199254740993));
    });

    it('should parse triple row with GEO_POINT value', () => {
      const rawRow = {
        id: 4,
        subject: 'https://example.com/entity/4',
        predicate: 'location',
        obj_type: ObjectType.GEO_POINT,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: 37.7749,
        obj_lng: -122.4194,
        obj_binary: null,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objLat).toBe(37.7749);
      expect(row.objLng).toBe(-122.4194);
    });

    it('should parse triple row with BINARY value', () => {
      const binaryData = new Uint8Array([1, 2, 3, 4, 5]);
      const rawRow = {
        id: 5,
        subject: 'https://example.com/entity/5',
        predicate: 'data',
        obj_type: ObjectType.BINARY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: binaryData,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objBinary).toEqual(binaryData);
    });

    it('should parse triple row with ArrayBuffer binary value', () => {
      const buffer = new ArrayBuffer(5);
      const view = new Uint8Array(buffer);
      view.set([1, 2, 3, 4, 5]);

      const rawRow = {
        id: 6,
        subject: 'https://example.com/entity/6',
        predicate: 'data',
        obj_type: ObjectType.BINARY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: buffer,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objBinary).toBeInstanceOf(Uint8Array);
      expect(row.objBinary).toEqual(new Uint8Array([1, 2, 3, 4, 5]));
    });

    it('should reject malformed rows - missing subject', () => {
      const rawRow = {
        id: 1,
        // subject missing
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('subject');
    });

    it('should reject malformed rows - missing predicate', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        // predicate missing
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('predicate');
    });

    it('should reject malformed rows - missing obj_type', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        // obj_type missing
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('obj_type');
    });

    it('should reject malformed rows - missing timestamp', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        // timestamp missing
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('timestamp');
    });

    it('should reject malformed rows - missing tx_id', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        timestamp: 1700000000000,
        // tx_id missing
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('tx_id');
    });

    it('should reject malformed rows - invalid obj_type (not a number)', () => {
      const rawRow = {
        id: 1,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: 'invalid', // should be number
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('obj_type');
    });

    it('should reject non-object input', () => {
      expect(parseTripleRow(null)).toBeInstanceOf(RowParseError);
      expect(parseTripleRow(undefined)).toBeInstanceOf(RowParseError);
      expect(parseTripleRow('string')).toBeInstanceOf(RowParseError);
      expect(parseTripleRow(123)).toBeInstanceOf(RowParseError);
      expect(parseTripleRow([])).toBeInstanceOf(RowParseError);
    });

    it('should handle null values correctly for optional fields', () => {
      const rawRow = {
        id: 7,
        subject: 'https://example.com/entity/7',
        predicate: 'deleted',
        obj_type: ObjectType.NULL,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: null,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const result = parseTripleRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as TripleRow;
      expect(row.objRef).toBeNull();
      expect(row.objString).toBeNull();
      expect(row.objInt64).toBeNull();
      expect(row.objFloat64).toBeNull();
      expect(row.objBool).toBeNull();
      expect(row.objTimestamp).toBeNull();
      expect(row.objLat).toBeNull();
      expect(row.objLng).toBeNull();
      expect(row.objBinary).toBeNull();
    });

    it('should handle BOOL type correctly (0 and 1)', () => {
      const rawRowTrue = {
        id: 8,
        subject: 'https://example.com/entity/8',
        predicate: 'active',
        obj_type: ObjectType.BOOL,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: 1,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: null,
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const rawRowFalse = {
        ...rawRowTrue,
        id: 9,
        obj_bool: 0,
      };

      const resultTrue = parseTripleRow(rawRowTrue);
      const resultFalse = parseTripleRow(rawRowFalse);

      expect(resultTrue).not.toBeInstanceOf(Error);
      expect(resultFalse).not.toBeInstanceOf(Error);
      expect((resultTrue as TripleRow).objBool).toBe(1);
      expect((resultFalse as TripleRow).objBool).toBe(0);
    });

    it('should handle optional id field', () => {
      const rawRowWithId = {
        id: 42,
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const rawRowWithoutId = {
        subject: 'https://example.com/entity/1',
        predicate: 'name',
        obj_type: ObjectType.STRING,
        obj_string: 'Test',
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const resultWithId = parseTripleRow(rawRowWithId);
      const resultWithoutId = parseTripleRow(rawRowWithoutId);

      expect(resultWithId).not.toBeInstanceOf(Error);
      expect(resultWithoutId).not.toBeInstanceOf(Error);
      expect((resultWithId as TripleRow).id).toBe(42);
      expect((resultWithoutId as TripleRow).id).toBeUndefined();
    });
  });

  describe('parseChunkRow', () => {
    it('should parse chunk row with metadata', () => {
      const blobData = new Uint8Array([1, 2, 3, 4, 5]);
      const rawRow = {
        id: 'chunk_abc123_xyz789',
        namespace: 'https://example.com/ns',
        triple_count: 1000,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: blobData,
        size_bytes: 1024,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as ChunkRow;
      expect(row.id).toBe('chunk_abc123_xyz789');
      expect(row.namespace).toBe('https://example.com/ns');
      expect(row.tripleCount).toBe(1000);
      expect(row.minTimestamp).toBe(1700000000000);
      expect(row.maxTimestamp).toBe(1700001000000);
      expect(row.data).toEqual(blobData);
      expect(row.sizeBytes).toBe(1024);
      expect(row.createdAt).toBe(1700002000000);
    });

    it('should parse chunk row with ArrayBuffer data', () => {
      const buffer = new ArrayBuffer(5);
      const view = new Uint8Array(buffer);
      view.set([10, 20, 30, 40, 50]);

      const rawRow = {
        id: 'chunk_def456',
        namespace: 'https://example.com/ns',
        triple_count: 500,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: buffer,
        size_bytes: 512,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as ChunkRow;
      expect(row.data).toBeInstanceOf(Uint8Array);
      expect(row.data).toEqual(new Uint8Array([10, 20, 30, 40, 50]));
    });

    it('should reject malformed chunk rows - missing id', () => {
      const rawRow = {
        // id missing
        namespace: 'https://example.com/ns',
        triple_count: 1000,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: new Uint8Array([1, 2, 3]),
        size_bytes: 1024,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('id');
    });

    it('should reject malformed chunk rows - missing namespace', () => {
      const rawRow = {
        id: 'chunk_abc123',
        // namespace missing
        triple_count: 1000,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: new Uint8Array([1, 2, 3]),
        size_bytes: 1024,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('namespace');
    });

    it('should reject malformed chunk rows - missing triple_count', () => {
      const rawRow = {
        id: 'chunk_abc123',
        namespace: 'https://example.com/ns',
        // triple_count missing
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: new Uint8Array([1, 2, 3]),
        size_bytes: 1024,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('triple_count');
    });

    it('should reject malformed chunk rows - missing data', () => {
      const rawRow = {
        id: 'chunk_abc123',
        namespace: 'https://example.com/ns',
        triple_count: 1000,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        // data missing
        size_bytes: 1024,
        created_at: 1700002000000,
      };

      const result = parseChunkRow(rawRow);

      expect(result).toBeInstanceOf(RowParseError);
      expect((result as RowParseError).message).toContain('data');
    });

    it('should reject non-object input', () => {
      expect(parseChunkRow(null)).toBeInstanceOf(RowParseError);
      expect(parseChunkRow(undefined)).toBeInstanceOf(RowParseError);
      expect(parseChunkRow('string')).toBeInstanceOf(RowParseError);
      expect(parseChunkRow(123)).toBeInstanceOf(RowParseError);
      expect(parseChunkRow([])).toBeInstanceOf(RowParseError);
    });

    it('should validate column count - reject extra unknown columns with strict mode', () => {
      const rawRow = {
        id: 'chunk_abc123',
        namespace: 'https://example.com/ns',
        triple_count: 1000,
        min_timestamp: 1700000000000,
        max_timestamp: 1700001000000,
        data: new Uint8Array([1, 2, 3]),
        size_bytes: 1024,
        created_at: 1700002000000,
        unknown_column: 'extra data',
      };

      // Default behavior should allow extra columns (forward compatibility)
      const result = parseChunkRow(rawRow);
      expect(result).not.toBeInstanceOf(Error);

      // Strict mode should reject extra columns
      const strictResult = parseChunkRow(rawRow, { strict: true });
      expect(strictResult).toBeInstanceOf(RowParseError);
      expect((strictResult as RowParseError).message).toContain('unknown_column');
    });

    it('should handle bigint timestamps from SQLite', () => {
      const rawRow = {
        id: 'chunk_abc123',
        namespace: 'https://example.com/ns',
        triple_count: 1000,
        min_timestamp: BigInt(1700000000000),
        max_timestamp: BigInt(1700001000000),
        data: new Uint8Array([1, 2, 3]),
        size_bytes: 1024,
        created_at: BigInt(1700002000000),
      };

      const result = parseChunkRow(rawRow);

      expect(result).not.toBeInstanceOf(Error);
      const row = result as ChunkRow;
      // Should normalize to numbers for consistency
      expect(row.minTimestamp).toBe(1700000000000);
      expect(row.maxTimestamp).toBe(1700001000000);
      expect(row.createdAt).toBe(1700002000000);
    });
  });

  describe('Branded Type Validation', () => {
    describe('parseTripleRow - branded types', () => {
      it('should reject invalid subject (not a valid URL)', () => {
        const rawRow = {
          subject: 'not-a-valid-url',
          predicate: 'name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('subject');
        expect((result as RowParseError).message).toContain('http');
      });

      it('should reject subject with non-http/https protocol', () => {
        const rawRow = {
          subject: 'ftp://example.com/entity/1',
          predicate: 'name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('subject');
      });

      it('should reject invalid predicate (contains colon)', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'schema:name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('predicate');
        expect((result as RowParseError).message).toContain('colon');
      });

      it('should reject invalid predicate (contains whitespace)', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'invalid name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('predicate');
      });

      it('should reject invalid tx_id (not ULID format)', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: 'not-a-valid-ulid',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('tx_id');
        expect((result as RowParseError).message).toContain('ULID');
      });

      it('should reject tx_id that is wrong length', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'name',
          obj_type: ObjectType.STRING,
          obj_string: 'Test',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FA', // 25 chars, needs 26
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('tx_id');
      });

      it('should reject invalid obj_ref (not a valid URL) when present', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'references',
          obj_type: ObjectType.REF,
          obj_ref: 'not-a-valid-url',
          obj_string: null,
          obj_int64: null,
          obj_float64: null,
          obj_bool: null,
          obj_timestamp: null,
          obj_lat: null,
          obj_lng: null,
          obj_binary: null,
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('obj_ref');
      });

      it('should accept valid branded types', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: 'references',
          obj_type: ObjectType.REF,
          obj_ref: 'https://example.com/entity/2',
          obj_string: null,
          obj_int64: null,
          obj_float64: null,
          obj_bool: null,
          obj_timestamp: null,
          obj_lat: null,
          obj_lng: null,
          obj_binary: null,
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).not.toBeInstanceOf(RowParseError);
        const row = result as TripleRow;
        expect(row.subject).toBe('https://example.com/entity/1');
        expect(row.predicate).toBe('references');
        expect(row.objRef).toBe('https://example.com/entity/2');
        expect(row.txId).toBe('01ARZ3NDEKTSV4RRFFQ69G5FAV');
      });

      it('should accept $ prefixed predicates', () => {
        const rawRow = {
          subject: 'https://example.com/entity/1',
          predicate: '$type',
          obj_type: ObjectType.STRING,
          obj_string: 'Person',
          timestamp: 1700000000000,
          tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
        };

        const result = parseTripleRow(rawRow);

        expect(result).not.toBeInstanceOf(RowParseError);
        expect((result as TripleRow).predicate).toBe('$type');
      });
    });

    describe('parseChunkRow - branded types', () => {
      it('should reject invalid namespace (not a valid URL)', () => {
        const rawRow = {
          id: 'chunk_abc123',
          namespace: 'not-a-valid-url',
          triple_count: 1000,
          min_timestamp: 1700000000000,
          max_timestamp: 1700001000000,
          data: new Uint8Array([1, 2, 3]),
          size_bytes: 1024,
          created_at: 1700002000000,
        };

        const result = parseChunkRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('namespace');
        expect((result as RowParseError).message).toContain('http');
      });

      it('should reject namespace with non-http/https protocol', () => {
        const rawRow = {
          id: 'chunk_abc123',
          namespace: 'ftp://example.com/ns',
          triple_count: 1000,
          min_timestamp: 1700000000000,
          max_timestamp: 1700001000000,
          data: new Uint8Array([1, 2, 3]),
          size_bytes: 1024,
          created_at: 1700002000000,
        };

        const result = parseChunkRow(rawRow);

        expect(result).toBeInstanceOf(RowParseError);
        expect((result as RowParseError).message).toContain('namespace');
      });

      it('should accept valid namespace URL', () => {
        const rawRow = {
          id: 'chunk_abc123',
          namespace: 'https://example.com/crm/',
          triple_count: 1000,
          min_timestamp: 1700000000000,
          max_timestamp: 1700001000000,
          data: new Uint8Array([1, 2, 3]),
          size_bytes: 1024,
          created_at: 1700002000000,
        };

        const result = parseChunkRow(rawRow);

        expect(result).not.toBeInstanceOf(RowParseError);
        const row = result as ChunkRow;
        expect(row.namespace).toBe('https://example.com/crm/');
      });

      it('should accept http namespace URL', () => {
        const rawRow = {
          id: 'chunk_abc123',
          namespace: 'http://localhost:8080/dev/',
          triple_count: 1000,
          min_timestamp: 1700000000000,
          max_timestamp: 1700001000000,
          data: new Uint8Array([1, 2, 3]),
          size_bytes: 1024,
          created_at: 1700002000000,
        };

        const result = parseChunkRow(rawRow);

        expect(result).not.toBeInstanceOf(RowParseError);
        const row = result as ChunkRow;
        expect(row.namespace).toBe('http://localhost:8080/dev/');
      });
    });
  });

  describe('rowToTriple - REF_ARRAY validation', () => {
    it('should reject REF_ARRAY with non-array JSON data', () => {
      // Create a row that would pass initial parsing but has invalid REF_ARRAY data
      const invalidJson = JSON.stringify({ notAnArray: true });
      const rawRow = {
        subject: 'https://example.com/entity/1',
        predicate: 'friends',
        obj_type: ObjectType.REF_ARRAY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: new TextEncoder().encode(invalidJson),
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      expect(() => rowToTriple(rawRow)).toThrow(RowParseError);
      expect(() => rowToTriple(rawRow)).toThrow('must be an array');
    });

    it('should reject REF_ARRAY with invalid EntityId items', () => {
      // Array with non-URL strings
      const invalidRefs = JSON.stringify(['not-a-url', 'also-not-a-url']);
      const rawRow = {
        subject: 'https://example.com/entity/1',
        predicate: 'friends',
        obj_type: ObjectType.REF_ARRAY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: new TextEncoder().encode(invalidRefs),
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      expect(() => rowToTriple(rawRow)).toThrow(RowParseError);
      expect(() => rowToTriple(rawRow)).toThrow('must be valid EntityIds');
    });

    it('should reject REF_ARRAY with non-string items', () => {
      // Array with number instead of string
      const invalidRefs = JSON.stringify([123, 'https://example.com/entity/2']);
      const rawRow = {
        subject: 'https://example.com/entity/1',
        predicate: 'friends',
        obj_type: ObjectType.REF_ARRAY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: new TextEncoder().encode(invalidRefs),
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      expect(() => rowToTriple(rawRow)).toThrow(RowParseError);
    });

    it('should accept valid REF_ARRAY with valid EntityIds', () => {
      const validRefs = JSON.stringify([
        'https://example.com/entity/2',
        'https://example.com/entity/3',
      ]);
      const rawRow = {
        subject: 'https://example.com/entity/1',
        predicate: 'friends',
        obj_type: ObjectType.REF_ARRAY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: new TextEncoder().encode(validRefs),
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const triple = rowToTriple(rawRow);
      expect(triple.object.type).toBe(ObjectType.REF_ARRAY);
      expect(triple.object.value).toEqual([
        'https://example.com/entity/2',
        'https://example.com/entity/3',
      ]);
    });

    it('should accept empty REF_ARRAY', () => {
      const emptyRefs = JSON.stringify([]);
      const rawRow = {
        subject: 'https://example.com/entity/1',
        predicate: 'friends',
        obj_type: ObjectType.REF_ARRAY,
        obj_ref: null,
        obj_string: null,
        obj_int64: null,
        obj_float64: null,
        obj_bool: null,
        obj_timestamp: null,
        obj_lat: null,
        obj_lng: null,
        obj_binary: new TextEncoder().encode(emptyRefs),
        timestamp: 1700000000000,
        tx_id: '01ARZ3NDEKTSV4RRFFQ69G5FAV',
      };

      const triple = rowToTriple(rawRow);
      expect(triple.object.type).toBe(ObjectType.REF_ARRAY);
      expect(triple.object.value).toEqual([]);
    });
  });

  describe('RowParseError', () => {
    it('should have name property set to RowParseError', () => {
      const error = new RowParseError('test message');
      expect(error.name).toBe('RowParseError');
    });

    it('should include field name in error', () => {
      const error = new RowParseError('Missing required field', 'subject');
      expect(error.field).toBe('subject');
      expect(error.message).toContain('subject');
    });

    it('should include original value in error for debugging', () => {
      const originalRow = { foo: 'bar' };
      const error = new RowParseError('Invalid row', undefined, originalRow);
      expect(error.originalValue).toBe(originalRow);
    });
  });
});
