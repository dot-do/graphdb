import { describe, it, expect } from 'vitest';
import {
  ObjectType,
  type EntityId,
  type Predicate,
  type Namespace,
  type TransactionId,
  isEntityId,
  createEntityId,
  isPredicate,
  createPredicate,
  isNamespace,
  createNamespace,
  isTransactionId,
  createTransactionId,
} from '../../src/core/types';
import {
  type GeoPoint,
  type GeoPolygon,
  type GeoLineString,
  isValidGeoPoint,
  isValidGeoPolygon,
  isValidGeoLineString,
  encodeGeohash,
  decodeGeohash,
} from '../../src/core/geo';

describe('ObjectType enum', () => {
  it('should have NULL = 0', () => {
    expect(ObjectType.NULL).toBe(0);
  });

  it('should have BOOL = 1', () => {
    expect(ObjectType.BOOL).toBe(1);
  });

  it('should have INT32 = 2', () => {
    expect(ObjectType.INT32).toBe(2);
  });

  it('should have INT64 = 3', () => {
    expect(ObjectType.INT64).toBe(3);
  });

  it('should have FLOAT64 = 4', () => {
    expect(ObjectType.FLOAT64).toBe(4);
  });

  it('should have STRING = 5', () => {
    expect(ObjectType.STRING).toBe(5);
  });

  it('should have BINARY = 6', () => {
    expect(ObjectType.BINARY).toBe(6);
  });

  it('should have TIMESTAMP = 7', () => {
    expect(ObjectType.TIMESTAMP).toBe(7);
  });

  it('should have DATE = 8', () => {
    expect(ObjectType.DATE).toBe(8);
  });

  it('should have DURATION = 9', () => {
    expect(ObjectType.DURATION).toBe(9);
  });

  it('should have REF = 10', () => {
    expect(ObjectType.REF).toBe(10);
  });

  it('should have REF_ARRAY = 11', () => {
    expect(ObjectType.REF_ARRAY).toBe(11);
  });

  it('should have JSON = 12', () => {
    expect(ObjectType.JSON).toBe(12);
  });

  it('should have GEO_POINT = 13', () => {
    expect(ObjectType.GEO_POINT).toBe(13);
  });

  it('should have GEO_POLYGON = 14', () => {
    expect(ObjectType.GEO_POLYGON).toBe(14);
  });

  it('should have GEO_LINESTRING = 15', () => {
    expect(ObjectType.GEO_LINESTRING).toBe(15);
  });

  it('should have URL = 16', () => {
    expect(ObjectType.URL).toBe(16);
  });

  it('should have VECTOR = 17', () => {
    expect(ObjectType.VECTOR).toBe(17);
  });

  it('should have exactly 18 enum values (0-17)', () => {
    // Count numeric enum values (exclude reverse mappings)
    const numericValues = Object.values(ObjectType).filter(
      (v) => typeof v === 'number'
    );
    expect(numericValues.length).toBe(18);
    expect(Math.min(...numericValues)).toBe(0);
    expect(Math.max(...numericValues)).toBe(17);
  });
});

describe('EntityId branded type', () => {
  describe('isEntityId', () => {
    it('should return true for valid URL strings', () => {
      expect(isEntityId('https://example.com/entity/123')).toBe(true);
      expect(isEntityId('http://example.com/entity/123')).toBe(true);
      expect(isEntityId('https://example.com')).toBe(true);
    });

    it('should return false for non-URL strings', () => {
      expect(isEntityId('not-a-url')).toBe(false);
      expect(isEntityId('entity/123')).toBe(false);
      expect(isEntityId('')).toBe(false);
      expect(isEntityId('ftp://example.com')).toBe(false); // Only http/https
    });

    it('should return false for malformed URLs', () => {
      expect(isEntityId('https://')).toBe(false);
      expect(isEntityId('https://.')).toBe(false);
      expect(isEntityId('://example.com')).toBe(false);
    });
  });

  describe('createEntityId', () => {
    it('should create EntityId from valid URL', () => {
      const entityId = createEntityId('https://example.com/entity/123');
      expect(entityId).toBe('https://example.com/entity/123');
      // TypeScript should recognize this as EntityId type
      const _check: EntityId = entityId;
    });

    it('should throw for invalid URL', () => {
      expect(() => createEntityId('not-a-url')).toThrow();
      expect(() => createEntityId('')).toThrow();
    });

    it('should throw for non-http(s) protocols', () => {
      expect(() => createEntityId('ftp://example.com')).toThrow();
      expect(() => createEntityId('file:///path')).toThrow();
    });
  });
});

describe('Predicate branded type', () => {
  describe('isPredicate', () => {
    it('should return true for valid predicate names', () => {
      expect(isPredicate('name')).toBe(true);
      expect(isPredicate('firstName')).toBe(true);
      expect(isPredicate('first_name')).toBe(true);
      expect(isPredicate('age')).toBe(true);
      expect(isPredicate('$id')).toBe(true);
      expect(isPredicate('$type')).toBe(true);
    });

    it('should return false for predicates containing colons (NO RDF prefixes)', () => {
      expect(isPredicate('schema:name')).toBe(false);
      expect(isPredicate('rdf:type')).toBe(false);
      expect(isPredicate('foaf:knows')).toBe(false);
      expect(isPredicate('prefix:suffix:extra')).toBe(false);
    });

    it('should return false for empty strings', () => {
      expect(isPredicate('')).toBe(false);
    });

    it('should return false for strings with invalid characters', () => {
      expect(isPredicate('name with space')).toBe(false);
      expect(isPredicate('name\ttab')).toBe(false);
      expect(isPredicate('name\nnewline')).toBe(false);
    });

    it('should allow underscores and camelCase', () => {
      expect(isPredicate('first_name')).toBe(true);
      expect(isPredicate('firstName')).toBe(true);
      expect(isPredicate('UPPER_CASE')).toBe(true);
      expect(isPredicate('MixedCase123')).toBe(true);
    });
  });

  describe('createPredicate', () => {
    it('should create Predicate from valid name', () => {
      const predicate = createPredicate('name');
      expect(predicate).toBe('name');
      // TypeScript should recognize this as Predicate type
      const _check: Predicate = predicate;
    });

    it('should throw for predicate with colons', () => {
      expect(() => createPredicate('schema:name')).toThrow();
      expect(() => createPredicate('rdf:type')).toThrow();
    });

    it('should throw for empty predicate', () => {
      expect(() => createPredicate('')).toThrow();
    });

    it('should throw for predicate with spaces', () => {
      expect(() => createPredicate('invalid name')).toThrow();
    });
  });
});

describe('Namespace branded type', () => {
  describe('isNamespace', () => {
    it('should return true for valid namespace URLs', () => {
      expect(isNamespace('https://example.com/ns/')).toBe(true);
      expect(isNamespace('https://schema.org/')).toBe(true);
    });

    it('should return false for non-URL strings', () => {
      expect(isNamespace('not-a-url')).toBe(false);
      expect(isNamespace('')).toBe(false);
    });
  });

  describe('createNamespace', () => {
    it('should create Namespace from valid URL', () => {
      const ns = createNamespace('https://example.com/ns/');
      expect(ns).toBe('https://example.com/ns/');
      const _check: Namespace = ns;
    });

    it('should throw for invalid URL', () => {
      expect(() => createNamespace('not-a-url')).toThrow();
    });
  });
});

describe('TransactionId branded type', () => {
  describe('isTransactionId', () => {
    it('should return true for valid transaction IDs (ULIDs)', () => {
      // ULID format: 26 character string, Crockford base32
      expect(isTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV')).toBe(true);
      expect(isTransactionId('01H9YG5VF9QW8TNXS0QBAJKZ3Y')).toBe(true);
    });

    it('should return false for invalid transaction IDs', () => {
      expect(isTransactionId('')).toBe(false);
      expect(isTransactionId('too-short')).toBe(false);
      expect(isTransactionId('invalid-chars-!!!')).toBe(false);
    });

    it('should return false for UUIDs (wrong format)', () => {
      expect(isTransactionId('550e8400-e29b-41d4-a716-446655440000')).toBe(
        false
      );
    });
  });

  describe('createTransactionId', () => {
    it('should create TransactionId from valid ULID', () => {
      const txId = createTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
      expect(txId).toBe('01ARZ3NDEKTSV4RRFFQ69G5FAV');
      const _check: TransactionId = txId;
    });

    it('should throw for invalid transaction ID', () => {
      expect(() => createTransactionId('invalid')).toThrow();
      expect(() => createTransactionId('')).toThrow();
    });
  });
});

describe('GeoPoint', () => {
  describe('isValidGeoPoint', () => {
    it('should return true for valid coordinates', () => {
      expect(isValidGeoPoint({ lat: 0, lng: 0 })).toBe(true);
      expect(isValidGeoPoint({ lat: 90, lng: 180 })).toBe(true);
      expect(isValidGeoPoint({ lat: -90, lng: -180 })).toBe(true);
      expect(isValidGeoPoint({ lat: 37.7749, lng: -122.4194 })).toBe(true); // San Francisco
      expect(isValidGeoPoint({ lat: 51.5074, lng: -0.1278 })).toBe(true); // London
    });

    it('should return false for latitude out of range (-90 to 90)', () => {
      expect(isValidGeoPoint({ lat: 91, lng: 0 })).toBe(false);
      expect(isValidGeoPoint({ lat: -91, lng: 0 })).toBe(false);
      expect(isValidGeoPoint({ lat: 100, lng: 0 })).toBe(false);
      expect(isValidGeoPoint({ lat: -100, lng: 0 })).toBe(false);
    });

    it('should return false for longitude out of range (-180 to 180)', () => {
      expect(isValidGeoPoint({ lat: 0, lng: 181 })).toBe(false);
      expect(isValidGeoPoint({ lat: 0, lng: -181 })).toBe(false);
      expect(isValidGeoPoint({ lat: 0, lng: 200 })).toBe(false);
      expect(isValidGeoPoint({ lat: 0, lng: -200 })).toBe(false);
    });

    it('should return false for NaN values', () => {
      expect(isValidGeoPoint({ lat: NaN, lng: 0 })).toBe(false);
      expect(isValidGeoPoint({ lat: 0, lng: NaN })).toBe(false);
      expect(isValidGeoPoint({ lat: NaN, lng: NaN })).toBe(false);
    });

    it('should return false for Infinity values', () => {
      expect(isValidGeoPoint({ lat: Infinity, lng: 0 })).toBe(false);
      expect(isValidGeoPoint({ lat: 0, lng: -Infinity })).toBe(false);
    });
  });
});

describe('GeoPolygon', () => {
  describe('isValidGeoPolygon', () => {
    it('should return true for valid polygon with closed exterior ring', () => {
      const polygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 0, lng: 1 },
          { lat: 1, lng: 1 },
          { lat: 1, lng: 0 },
          { lat: 0, lng: 0 }, // closed ring
        ],
      };
      expect(isValidGeoPolygon(polygon)).toBe(true);
    });

    it('should return true for valid polygon with holes', () => {
      const polygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 0, lng: 10 },
          { lat: 10, lng: 10 },
          { lat: 10, lng: 0 },
          { lat: 0, lng: 0 },
        ],
        holes: [
          [
            { lat: 2, lng: 2 },
            { lat: 2, lng: 3 },
            { lat: 3, lng: 3 },
            { lat: 3, lng: 2 },
            { lat: 2, lng: 2 },
          ],
        ],
      };
      expect(isValidGeoPolygon(polygon)).toBe(true);
    });

    it('should return false for polygon with fewer than 4 points', () => {
      const polygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 1, lng: 1 },
          { lat: 0, lng: 0 },
        ],
      };
      expect(isValidGeoPolygon(polygon)).toBe(false);
    });

    it('should return false for unclosed polygon ring', () => {
      const polygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 0, lng: 1 },
          { lat: 1, lng: 1 },
          { lat: 1, lng: 0 },
          // not closed - missing { lat: 0, lng: 0 }
        ],
      };
      expect(isValidGeoPolygon(polygon)).toBe(false);
    });

    it('should return false if exterior contains invalid point', () => {
      const polygon: GeoPolygon = {
        exterior: [
          { lat: 0, lng: 0 },
          { lat: 0, lng: 1 },
          { lat: 100, lng: 1 }, // invalid latitude
          { lat: 1, lng: 0 },
          { lat: 0, lng: 0 },
        ],
      };
      expect(isValidGeoPolygon(polygon)).toBe(false);
    });
  });
});

describe('GeoLineString', () => {
  describe('isValidGeoLineString', () => {
    it('should return true for valid line string with 2+ points', () => {
      const line: GeoLineString = {
        points: [
          { lat: 0, lng: 0 },
          { lat: 1, lng: 1 },
        ],
      };
      expect(isValidGeoLineString(line)).toBe(true);
    });

    it('should return true for multi-point line string', () => {
      const line: GeoLineString = {
        points: [
          { lat: 0, lng: 0 },
          { lat: 1, lng: 1 },
          { lat: 2, lng: 0 },
          { lat: 3, lng: 1 },
        ],
      };
      expect(isValidGeoLineString(line)).toBe(true);
    });

    it('should return false for line string with fewer than 2 points', () => {
      const line: GeoLineString = {
        points: [{ lat: 0, lng: 0 }],
      };
      expect(isValidGeoLineString(line)).toBe(false);
    });

    it('should return false for empty line string', () => {
      const line: GeoLineString = {
        points: [],
      };
      expect(isValidGeoLineString(line)).toBe(false);
    });

    it('should return false if any point is invalid', () => {
      const line: GeoLineString = {
        points: [
          { lat: 0, lng: 0 },
          { lat: 100, lng: 0 }, // invalid latitude
        ],
      };
      expect(isValidGeoLineString(line)).toBe(false);
    });
  });
});

describe('Geohash encoding/decoding', () => {
  describe('encodeGeohash', () => {
    it('should encode known coordinates correctly', () => {
      // San Francisco: 37.7749, -122.4194
      // Known geohash: 9q8yy (5 chars precision)
      const hash = encodeGeohash(37.7749, -122.4194, 5);
      expect(hash).toBe('9q8yy');
    });

    it('should encode with default precision of 9', () => {
      const hash = encodeGeohash(37.7749, -122.4194);
      expect(hash.length).toBe(9);
    });

    it('should encode origin (0, 0) correctly', () => {
      const hash = encodeGeohash(0, 0, 5);
      expect(hash).toBe('s0000');
    });

    it('should handle different precisions', () => {
      const hash3 = encodeGeohash(37.7749, -122.4194, 3);
      const hash5 = encodeGeohash(37.7749, -122.4194, 5);
      const hash7 = encodeGeohash(37.7749, -122.4194, 7);

      expect(hash3.length).toBe(3);
      expect(hash5.length).toBe(5);
      expect(hash7.length).toBe(7);

      // Longer hash should start with shorter hash
      expect(hash5.startsWith(hash3)).toBe(true);
      expect(hash7.startsWith(hash5)).toBe(true);
    });

    it('should throw for invalid coordinates', () => {
      expect(() => encodeGeohash(100, 0)).toThrow();
      expect(() => encodeGeohash(0, 200)).toThrow();
    });
  });

  describe('decodeGeohash', () => {
    it('should decode known geohash correctly', () => {
      // 9q8yy -> approximately San Francisco
      const point = decodeGeohash('9q8yy');
      expect(point.lat).toBeCloseTo(37.77, 1);
      expect(point.lng).toBeCloseTo(-122.42, 1);
    });

    it('should decode origin geohash correctly', () => {
      const point = decodeGeohash('s0000');
      expect(point.lat).toBeCloseTo(0, 0);
      expect(point.lng).toBeCloseTo(0, 0);
    });

    it('should return valid GeoPoint', () => {
      const point = decodeGeohash('9q8yy');
      expect(isValidGeoPoint(point)).toBe(true);
    });

    it('should throw for empty geohash', () => {
      expect(() => decodeGeohash('')).toThrow();
    });

    it('should throw for invalid geohash characters', () => {
      expect(() => decodeGeohash('invalid!')).toThrow();
      expect(() => decodeGeohash('abcio')).toThrow(); // 'i', 'o' are not valid geohash chars
    });
  });

  describe('round-trip encoding/decoding', () => {
    it('should round-trip coordinates with acceptable precision loss', () => {
      const testCases = [
        { lat: 37.7749, lng: -122.4194, name: 'San Francisco' },
        { lat: 51.5074, lng: -0.1278, name: 'London' },
        { lat: 35.6762, lng: 139.6503, name: 'Tokyo' },
        { lat: -33.8688, lng: 151.2093, name: 'Sydney' },
        { lat: 0, lng: 0, name: 'Origin' },
        { lat: 90, lng: 0, name: 'North Pole' },
        { lat: -90, lng: 0, name: 'South Pole' },
      ];

      for (const { lat, lng, name } of testCases) {
        const hash = encodeGeohash(lat, lng, 9);
        const decoded = decodeGeohash(hash);

        // With 9 character precision, should be within ~5 meters
        expect(decoded.lat).toBeCloseTo(lat, 4);
        expect(decoded.lng).toBeCloseTo(lng, 4);
      }
    });

    it('should have higher precision with longer geohash', () => {
      const lat = 37.7749295;
      const lng = -122.4194155;

      const hash5 = encodeGeohash(lat, lng, 5);
      const hash9 = encodeGeohash(lat, lng, 9);
      const hash12 = encodeGeohash(lat, lng, 12);

      const decoded5 = decodeGeohash(hash5);
      const decoded9 = decodeGeohash(hash9);
      const decoded12 = decodeGeohash(hash12);

      // Error should decrease with longer geohash
      const error5 = Math.abs(decoded5.lat - lat) + Math.abs(decoded5.lng - lng);
      const error9 = Math.abs(decoded9.lat - lat) + Math.abs(decoded9.lng - lng);
      const error12 =
        Math.abs(decoded12.lat - lat) + Math.abs(decoded12.lng - lng);

      expect(error9).toBeLessThan(error5);
      expect(error12).toBeLessThan(error9);
    });
  });
});
