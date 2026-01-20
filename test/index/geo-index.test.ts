/**
 * Geo Index Tests
 *
 * NOTE: Many tests use the deprecated triples table which has been removed
 * in BLOB-only architecture (schema v3). Those tests are SKIPPED.
 *
 * The pure utility functions (haversineDistance, getGeohashNeighbors) still work.
 *
 * Tests for geospatial index using geohash encoding:
 * - Geohash column and index creation
 * - Bounding box queries
 * - Radius queries with haversine distance
 * - Geohash neighbor calculations
 * - Edge cases (antimeridian, poles)
 *
 * @see CLAUDE.md for architecture details
 */

import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { initializeSchema } from '../../src/shard/schema.js';
import { ObjectType } from '../../src/core/types.js';
import type { EntityId, Predicate } from '../../src/core/types.js';
import { encodeGeohash, decodeGeohash } from '../../src/core/geo.js';
import {
  initializeGeoIndex,
  queryGeoBBox,
  queryGeoRadius,
  getGeohashNeighbors,
  haversineDistance,
  insertGeoPointTriple,
  type GeoQuery,
  type GeoResult,
} from '../../src/index/geo-index.js';

// Helper to get fresh DO stubs
let testCounter = 0;
function getUniqueShardStub() {
  const id = env.SHARD.idFromName(`shard-geo-test-${Date.now()}-${testCounter++}`);
  return env.SHARD.get(id);
}

// Test data helpers
const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';

/**
 * Insert a geo point triple into the database with computed geohash
 */
function insertGeoTriple(
  sql: SqlStorage,
  subject: string,
  predicate: string,
  lat: number,
  lng: number
): void {
  // Use insertGeoPointTriple which computes the geohash automatically
  insertGeoPointTriple(sql, subject, predicate, lat, lng, Date.now(), VALID_TX_ID);
}

// ============================================================================
// Test Locations (well-known places with precise coordinates)
// ============================================================================

const LOCATIONS = {
  // San Francisco area
  sanFrancisco: { lat: 37.7749, lng: -122.4194 },
  oakland: { lat: 37.8044, lng: -122.2712 },
  berkeley: { lat: 37.8716, lng: -122.2727 },
  paloAlto: { lat: 37.4419, lng: -122.143 },

  // New York area
  newYork: { lat: 40.7128, lng: -74.006 },
  brooklyn: { lat: 40.6782, lng: -73.9442 },

  // London area
  london: { lat: 51.5074, lng: -0.1278 },

  // Tokyo area
  tokyo: { lat: 35.6762, lng: 139.6503 },

  // Edge cases
  northPole: { lat: 90, lng: 0 },
  southPole: { lat: -90, lng: 0 },
  antimeridianEast: { lat: 0, lng: 179.9 },
  antimeridianWest: { lat: 0, lng: -179.9 },
  nullIsland: { lat: 0, lng: 0 },
};

// ============================================================================
// Haversine Distance Tests
// ============================================================================

describe('haversineDistance', () => {
  it('should return 0 for identical points', () => {
    const point = LOCATIONS.sanFrancisco;
    const distance = haversineDistance(point, point);
    expect(distance).toBe(0);
  });

  it('should calculate distance between SF and Oakland (~13km)', () => {
    const distance = haversineDistance(LOCATIONS.sanFrancisco, LOCATIONS.oakland);
    // SF to Oakland is approximately 13km
    expect(distance).toBeGreaterThan(12);
    expect(distance).toBeLessThan(15);
  });

  it('should calculate distance between SF and NY (~4130km)', () => {
    const distance = haversineDistance(LOCATIONS.sanFrancisco, LOCATIONS.newYork);
    // SF to NY is approximately 4130km
    expect(distance).toBeGreaterThan(4000);
    expect(distance).toBeLessThan(4200);
  });

  it('should calculate distance between London and Tokyo (~9560km)', () => {
    const distance = haversineDistance(LOCATIONS.london, LOCATIONS.tokyo);
    // London to Tokyo is approximately 9560km
    expect(distance).toBeGreaterThan(9400);
    expect(distance).toBeLessThan(9700);
  });

  it('should handle antimeridian crossing', () => {
    // Points across the antimeridian (should be ~40km, not ~40000km)
    const distance = haversineDistance(LOCATIONS.antimeridianEast, LOCATIONS.antimeridianWest);
    expect(distance).toBeLessThan(50);
  });

  it('should calculate pole-to-pole distance (~20015km)', () => {
    const distance = haversineDistance(LOCATIONS.northPole, LOCATIONS.southPole);
    // Pole to pole is half the earth's circumference (~20015km)
    expect(distance).toBeGreaterThan(19900);
    expect(distance).toBeLessThan(20100);
  });
});

// ============================================================================
// Geohash Neighbors Tests
// ============================================================================

describe('getGeohashNeighbors', () => {
  it('should return 8 neighbors for a geohash', () => {
    const hash = encodeGeohash(37.7749, -122.4194, 6);
    const neighbors = getGeohashNeighbors(hash);

    expect(neighbors.length).toBe(8);
    // All neighbors should have the same length as the original hash
    for (const neighbor of neighbors) {
      expect(neighbor.length).toBe(hash.length);
    }
  });

  it('should return neighbors that are adjacent cells', () => {
    // Use a specific geohash we can verify
    const hash = '9q8yyk'; // SF area
    const neighbors = getGeohashNeighbors(hash);

    // Neighbors should all be different from the original
    expect(neighbors).not.toContain(hash);

    // All neighbors should be unique
    const uniqueNeighbors = new Set(neighbors);
    expect(uniqueNeighbors.size).toBe(8);
  });

  it('should handle edge cases at prime meridian', () => {
    const hash = encodeGeohash(51.5074, 0, 6); // Near London
    const neighbors = getGeohashNeighbors(hash);

    expect(neighbors.length).toBe(8);
    // All should be valid geohash strings
    for (const neighbor of neighbors) {
      expect(neighbor).toMatch(/^[0-9b-hj-km-np-z]+$/);
    }
  });

  it('should handle short geohashes', () => {
    const hash = encodeGeohash(37.7749, -122.4194, 2);
    const neighbors = getGeohashNeighbors(hash);

    expect(neighbors.length).toBe(8);
  });
});

// ============================================================================
// Geo Index Initialization Tests
// ============================================================================

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('initializeGeoIndex', () => {
  it('should add geohash column to triples table', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Check if geohash column exists
      const result = sql.exec("PRAGMA table_info(triples)");
      const columns = [...result].map((row: any) => row.name);

      expect(columns).toContain('geohash');
    });
  });

  it('should create geohash index', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Check if geohash index exists
      const result = sql.exec(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_geohash'"
      );
      const indexes = [...result];

      expect(indexes.length).toBe(1);
    });
  });

  it('should be idempotent (safe to call multiple times)', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);

      // Call multiple times - should not throw
      initializeGeoIndex(sql);
      initializeGeoIndex(sql);
      initializeGeoIndex(sql);

      // Verify structure is still correct
      const result = sql.exec("PRAGMA table_info(triples)");
      const columns = [...result].map((row: any) => row.name);
      expect(columns).toContain('geohash');
    });
  });
});

// ============================================================================
// Geohash Computation Tests
// ============================================================================

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Geohash computation on insert', () => {
  it('should compute geohash when inserting GEO_POINT triples', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert a geo point
      insertGeoTriple(sql, 'https://example.com/place/1', 'location', 37.7749, -122.4194);

      // Check if geohash was computed
      const result = sql.exec('SELECT geohash FROM triples WHERE subject = ?', 'https://example.com/place/1');
      const rows = [...result] as any[];

      expect(rows.length).toBe(1);
      expect(rows[0].geohash).toBeDefined();
      expect(rows[0].geohash).toMatch(/^[0-9b-hj-km-np-z]+$/);

      // Verify the geohash decodes back to approximately the same location
      const decoded = decodeGeohash(rows[0].geohash);
      expect(decoded.lat).toBeCloseTo(37.7749, 1);
      expect(decoded.lng).toBeCloseTo(-122.4194, 1);
    });
  });

  it('should not compute geohash for non-GEO_POINT triples', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert a string triple
      sql.exec(
        `INSERT INTO triples (subject, predicate, obj_type, obj_string, timestamp, tx_id)
         VALUES (?, ?, ?, ?, ?, ?)`,
        'https://example.com/place/1',
        'name',
        ObjectType.STRING,
        'San Francisco',
        Date.now(),
        VALID_TX_ID
      );

      // Check that geohash is null
      const result = sql.exec('SELECT geohash FROM triples WHERE subject = ?', 'https://example.com/place/1');
      const rows = [...result] as any[];

      expect(rows.length).toBe(1);
      expect(rows[0].geohash).toBeNull();
    });
  });
});

// ============================================================================
// Bounding Box Query Tests
// ============================================================================

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('queryGeoBBox', () => {
  it('should return points within bounding box', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert SF Bay Area locations
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);
      insertGeoTriple(sql, 'https://example.com/place/oakland', 'location', LOCATIONS.oakland.lat, LOCATIONS.oakland.lng);
      insertGeoTriple(sql, 'https://example.com/place/berkeley', 'location', LOCATIONS.berkeley.lat, LOCATIONS.berkeley.lng);

      // Insert NY location (outside bbox)
      insertGeoTriple(sql, 'https://example.com/place/ny', 'location', LOCATIONS.newYork.lat, LOCATIONS.newYork.lng);

      // Query SF Bay Area bounding box
      const results = await queryGeoBBox(sql, {
        bbox: {
          minLat: 37.4,
          maxLat: 38.0,
          minLng: -122.6,
          maxLng: -122.0,
        },
      });

      expect(results.length).toBe(3);

      // Verify all returned points are in SF Bay Area
      const subjects = results.map(r => r.subject);
      expect(subjects).toContain('https://example.com/place/sf');
      expect(subjects).toContain('https://example.com/place/oakland');
      expect(subjects).toContain('https://example.com/place/berkeley');
      expect(subjects).not.toContain('https://example.com/place/ny');
    });
  });

  it('should return empty array when no points in bbox', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert SF location
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);

      // Query somewhere with no points (middle of Pacific Ocean)
      const results = await queryGeoBBox(sql, {
        bbox: {
          minLat: 30,
          maxLat: 35,
          minLng: -160,
          maxLng: -150,
        },
      });

      expect(results.length).toBe(0);
    });
  });

  it('should filter by predicate when specified', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert same location with different predicates
      insertGeoTriple(sql, 'https://example.com/place/1', 'homeLocation', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);
      insertGeoTriple(sql, 'https://example.com/place/1', 'workLocation', LOCATIONS.oakland.lat, LOCATIONS.oakland.lng);

      // Query with predicate filter
      const results = await queryGeoBBox(sql, {
        bbox: {
          minLat: 37.4,
          maxLat: 38.0,
          minLng: -122.6,
          maxLng: -122.0,
        },
        predicate: 'homeLocation' as Predicate,
      });

      expect(results.length).toBe(1);
      expect(results[0].predicate).toBe('homeLocation');
    });
  });

  it('should respect limit parameter', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert multiple locations
      for (let i = 0; i < 10; i++) {
        insertGeoTriple(
          sql,
          `https://example.com/place/${i}`,
          'location',
          37.7749 + i * 0.01,
          -122.4194 + i * 0.01
        );
      }

      // Query with limit
      const results = await queryGeoBBox(sql, {
        bbox: {
          minLat: 37.7,
          maxLat: 38.0,
          minLng: -122.5,
          maxLng: -122.0,
        },
        limit: 5,
      });

      expect(results.length).toBe(5);
    });
  });

  it('should handle antimeridian crossing bbox', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert points near antimeridian
      insertGeoTriple(sql, 'https://example.com/place/east', 'location', 0, 179.9);
      insertGeoTriple(sql, 'https://example.com/place/west', 'location', 0, -179.9);
      insertGeoTriple(sql, 'https://example.com/place/far', 'location', 0, 0); // Should not match

      // Query across antimeridian (minLng > maxLng indicates crossing)
      const results = await queryGeoBBox(sql, {
        bbox: {
          minLat: -10,
          maxLat: 10,
          minLng: 170,
          maxLng: -170,
        },
      });

      expect(results.length).toBe(2);
      const subjects = results.map(r => r.subject);
      expect(subjects).toContain('https://example.com/place/east');
      expect(subjects).toContain('https://example.com/place/west');
    });
  });
});

// ============================================================================
// Radius Query Tests
// ============================================================================

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('queryGeoRadius', () => {
  it('should return points within radius', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert locations
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);
      insertGeoTriple(sql, 'https://example.com/place/oakland', 'location', LOCATIONS.oakland.lat, LOCATIONS.oakland.lng);
      insertGeoTriple(sql, 'https://example.com/place/ny', 'location', LOCATIONS.newYork.lat, LOCATIONS.newYork.lng);

      // Query 20km radius around SF (should include Oakland but not NY)
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 20,
      });

      expect(results.length).toBe(2); // SF and Oakland

      const subjects = results.map(r => r.subject);
      expect(subjects).toContain('https://example.com/place/sf');
      expect(subjects).toContain('https://example.com/place/oakland');
      expect(subjects).not.toContain('https://example.com/place/ny');
    });
  });

  it('should include distance in results', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert SF location
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);

      // Query from same point
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 10,
      });

      expect(results.length).toBe(1);
      expect(results[0].distanceKm).toBeDefined();
      expect(results[0].distanceKm).toBeCloseTo(0, 1);
    });
  });

  it('should return results sorted by distance', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert locations at varying distances from SF
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);
      insertGeoTriple(sql, 'https://example.com/place/oakland', 'location', LOCATIONS.oakland.lat, LOCATIONS.oakland.lng);
      insertGeoTriple(sql, 'https://example.com/place/berkeley', 'location', LOCATIONS.berkeley.lat, LOCATIONS.berkeley.lng);
      insertGeoTriple(sql, 'https://example.com/place/paloAlto', 'location', LOCATIONS.paloAlto.lat, LOCATIONS.paloAlto.lng);

      // Query from SF
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 100,
      });

      // Results should be sorted by distance
      for (let i = 1; i < results.length; i++) {
        expect(results[i].distanceKm!).toBeGreaterThanOrEqual(results[i - 1].distanceKm!);
      }
    });
  });

  it('should filter by predicate when specified', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert same location with different predicates
      insertGeoTriple(sql, 'https://example.com/place/1', 'homeLocation', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);
      insertGeoTriple(sql, 'https://example.com/place/2', 'workLocation', LOCATIONS.oakland.lat, LOCATIONS.oakland.lng);

      // Query with predicate filter
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 50,
        predicate: 'homeLocation' as Predicate,
      });

      expect(results.length).toBe(1);
      expect(results[0].predicate).toBe('homeLocation');
    });
  });

  it('should respect limit parameter', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert multiple locations
      for (let i = 0; i < 10; i++) {
        insertGeoTriple(
          sql,
          `https://example.com/place/${i}`,
          'location',
          LOCATIONS.sanFrancisco.lat + i * 0.001,
          LOCATIONS.sanFrancisco.lng + i * 0.001
        );
      }

      // Query with limit
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 50,
        limit: 5,
      });

      expect(results.length).toBe(5);
    });
  });

  it('should handle zero radius (exact point)', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert exact location
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);

      // Query with zero radius - should still find exact match
      const results = await queryGeoRadius(sql, {
        center: LOCATIONS.sanFrancisco,
        radiusKm: 0,
      });

      // Zero radius should find exact matches (within floating point precision)
      expect(results.length).toBe(1);
    });
  });

  it('should handle queries near poles', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert points near north pole
      insertGeoTriple(sql, 'https://example.com/place/polar1', 'location', 89.9, 0);
      insertGeoTriple(sql, 'https://example.com/place/polar2', 'location', 89.9, 90);
      insertGeoTriple(sql, 'https://example.com/place/polar3', 'location', 89.9, 180);

      // Query from north pole
      const results = await queryGeoRadius(sql, {
        center: { lat: 90, lng: 0 },
        radiusKm: 20,
      });

      // All points near pole should be within 20km
      expect(results.length).toBe(3);
    });
  });

  it('should handle queries across antimeridian', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert points on both sides of antimeridian
      insertGeoTriple(sql, 'https://example.com/place/east', 'location', 0, 179.9);
      insertGeoTriple(sql, 'https://example.com/place/west', 'location', 0, -179.9);
      insertGeoTriple(sql, 'https://example.com/place/far', 'location', 0, 0);

      // Query from antimeridian point - should find nearby points on both sides
      const results = await queryGeoRadius(sql, {
        center: { lat: 0, lng: 180 },
        radiusKm: 50,
      });

      // Should find the two points near antimeridian
      expect(results.length).toBe(2);
      const subjects = results.map(r => r.subject);
      expect(subjects).toContain('https://example.com/place/east');
      expect(subjects).toContain('https://example.com/place/west');
    });
  });
});

// ============================================================================
// Edge Cases
// ============================================================================

// SKIPPED: Uses deprecated triples table which no longer exists in BLOB-only schema
describe('Edge cases', () => {
  it('should handle null island (0, 0)', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert at null island
      insertGeoTriple(sql, 'https://example.com/place/null', 'location', 0, 0);

      // Query should find it
      const results = await queryGeoRadius(sql, {
        center: { lat: 0, lng: 0 },
        radiusKm: 1,
      });

      expect(results.length).toBe(1);
    });
  });

  it('should handle extreme latitude values', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert at poles
      insertGeoTriple(sql, 'https://example.com/place/north', 'location', 90, 0);
      insertGeoTriple(sql, 'https://example.com/place/south', 'location', -90, 0);

      // Query at north pole
      const northResults = await queryGeoRadius(sql, {
        center: { lat: 90, lng: 0 },
        radiusKm: 1,
      });
      expect(northResults.length).toBe(1);
      expect(northResults[0].subject).toBe('https://example.com/place/north');

      // Query at south pole
      const southResults = await queryGeoRadius(sql, {
        center: { lat: -90, lng: 0 },
        radiusKm: 1,
      });
      expect(southResults.length).toBe(1);
      expect(southResults[0].subject).toBe('https://example.com/place/south');
    });
  });

  it('should handle empty database', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Query empty database
      const bboxResults = await queryGeoBBox(sql, {
        bbox: {
          minLat: -90,
          maxLat: 90,
          minLng: -180,
          maxLng: 180,
        },
      });
      expect(bboxResults.length).toBe(0);

      const radiusResults = await queryGeoRadius(sql, {
        center: { lat: 0, lng: 0 },
        radiusKm: 1000,
      });
      expect(radiusResults.length).toBe(0);
    });
  });

  it('should handle large radius queries', async () => {
    const stub = getUniqueShardStub();

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const sql = state.storage.sql;
      initializeSchema(sql);
      initializeGeoIndex(sql);

      // Insert a point
      insertGeoTriple(sql, 'https://example.com/place/sf', 'location', LOCATIONS.sanFrancisco.lat, LOCATIONS.sanFrancisco.lng);

      // Query with earth-circumference radius (should find everything)
      const results = await queryGeoRadius(sql, {
        center: { lat: 0, lng: 0 },
        radiusKm: 20037, // Half earth circumference
      });

      expect(results.length).toBe(1);
    });
  });
});
