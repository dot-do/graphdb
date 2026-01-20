/**
 * Geospatial Index for GraphDB
 *
 * Provides geospatial indexing using geohash encoding for efficient
 * spatial queries on GEO_POINT data:
 * - Bounding box queries
 * - Radius queries with haversine distance
 * - Geohash-based index for prefix queries
 *
 * @see CLAUDE.md for architecture details
 * @see src/core/geo.ts for geohash encoding/decoding
 */

import type { EntityId, Predicate } from '../core/types';
import { ObjectType } from '../core/types';
import type { GeoPoint } from '../core/geo';
import { encodeGeohash } from '../core/geo';
import { querySql } from '../shard/sql-utils.js';

// ============================================================================
// Constants
// ============================================================================

/** Earth's radius in kilometers */
const EARTH_RADIUS_KM = 6371;

/** Default geohash precision (8 chars = ~19m x 19m cell) */
const DEFAULT_GEOHASH_PRECISION = 8;

/** Geohash Base32 characters */
const GEOHASH_CHARS = '0123456789bcdefghjkmnpqrstuvwxyz';

// ============================================================================
// Schema SQL
// ============================================================================

/**
 * SQL to add geohash column and index for spatial queries
 *
 * This is a migration that adds:
 * - geohash TEXT column to triples table
 * - idx_geohash index for prefix queries (only for GEO_POINT type)
 */
export const GEO_INDEX_SCHEMA = `
-- Add geohash column for spatial indexing (if not exists)
-- Note: SQLite doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN,
-- so we check in code before running this

-- Index on geohash for prefix queries (only for GEO_POINT type)
CREATE INDEX IF NOT EXISTS idx_geohash ON triples(geohash) WHERE obj_type = 13;
`;

// ============================================================================
// Query Interfaces
// ============================================================================

/**
 * Geospatial query parameters
 */
export interface GeoQuery {
  /** Bounding box query */
  bbox?: {
    minLat: number;
    maxLat: number;
    minLng: number;
    maxLng: number;
  };

  /** Center point for radius query */
  center?: GeoPoint;

  /** Radius in kilometers for radius query */
  radiusKm?: number;

  /** Optional predicate filter */
  predicate?: Predicate;

  /** Maximum number of results to return */
  limit?: number;
}

/**
 * Geospatial query result
 */
export interface GeoResult {
  /** The subject entity ID */
  subject: EntityId;

  /** The predicate of the triple */
  predicate: Predicate;

  /** The geographic point */
  point: GeoPoint;

  /** Distance from query center in km (only for radius queries) */
  distanceKm?: number;
}

// ============================================================================
// Index Initialization
// ============================================================================

/**
 * Initialize the geo index on a SQLite storage instance
 *
 * Adds the geohash column to the triples table and creates the index.
 * This is idempotent - safe to call multiple times.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 */
export function initializeGeoIndex(sql: SqlStorage): void {
  // Check if geohash column already exists
  const columns = querySql<TableInfoRow>(sql, 'PRAGMA table_info(triples)');
  const columnNames = columns.map((row) => row.name);

  if (!columnNames.includes('geohash')) {
    // Add geohash column
    sql.exec('ALTER TABLE triples ADD COLUMN geohash TEXT');
  }

  // Create index (IF NOT EXISTS handles idempotency)
  sql.exec('CREATE INDEX IF NOT EXISTS idx_geohash ON triples(geohash) WHERE obj_type = 13');

  // Backfill geohash for existing GEO_POINT triples that don't have it
  backfillGeohash(sql);
}

/**
 * Backfill geohash values for existing GEO_POINT triples
 *
 * @param sql - SqlStorage instance
 */
function backfillGeohash(sql: SqlStorage): void {
  // Get all GEO_POINT triples without geohash
  const result = sql.exec(
    'SELECT id, obj_lat, obj_lng FROM triples WHERE obj_type = ? AND geohash IS NULL',
    ObjectType.GEO_POINT
  );

  for (const row of result) {
    const { id, obj_lat, obj_lng } = row as { id: number; obj_lat: number; obj_lng: number };
    if (obj_lat !== null && obj_lng !== null) {
      const geohash = encodeGeohash(obj_lat, obj_lng, DEFAULT_GEOHASH_PRECISION);
      sql.exec('UPDATE triples SET geohash = ? WHERE id = ?', geohash, id);
    }
  }
}

/**
 * Compute and set geohash for a newly inserted GEO_POINT triple
 *
 * Call this after inserting a GEO_POINT triple to compute its geohash.
 * This is typically called by a trigger, but can be called manually.
 *
 * @param sql - SqlStorage instance
 * @param tripleId - The ID of the triple to update
 * @param lat - Latitude
 * @param lng - Longitude
 */
export function computeGeohashForTriple(sql: SqlStorage, tripleId: number, lat: number, lng: number): void {
  const geohash = encodeGeohash(lat, lng, DEFAULT_GEOHASH_PRECISION);
  sql.exec('UPDATE triples SET geohash = ? WHERE id = ?', geohash, tripleId);
}

// ============================================================================
// Haversine Distance
// ============================================================================

/**
 * Calculate the haversine distance between two geographic points
 *
 * Uses the haversine formula to calculate the great-circle distance
 * between two points on a sphere given their longitudes and latitudes.
 *
 * @param p1 - First point
 * @param p2 - Second point
 * @returns Distance in kilometers
 */
export function haversineDistance(p1: GeoPoint, p2: GeoPoint): number {
  // Convert to radians
  const lat1 = (p1.lat * Math.PI) / 180;
  const lat2 = (p2.lat * Math.PI) / 180;
  const dLat = ((p2.lat - p1.lat) * Math.PI) / 180;
  const dLng = ((p2.lng - p1.lng) * Math.PI) / 180;

  // Haversine formula
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) * Math.sin(dLng / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return EARTH_RADIUS_KM * c;
}

// ============================================================================
// Geohash Neighbors
// ============================================================================

/**
 * Get the 8 neighboring geohash cells for a given geohash
 *
 * Returns neighbors in this order:
 * [N, NE, E, SE, S, SW, W, NW]
 *
 * @param hash - The center geohash
 * @returns Array of 8 neighboring geohash strings
 */
export function getGeohashNeighbors(hash: string): string[] {
  // Decode the center cell bounds
  const bounds = getGeohashBounds(hash);

  // Calculate cell dimensions
  const latDelta = bounds.maxLat - bounds.minLat;
  const lngDelta = bounds.maxLng - bounds.minLng;

  // Center of the cell
  const centerLat = (bounds.minLat + bounds.maxLat) / 2;
  const centerLng = (bounds.minLng + bounds.maxLng) / 2;

  // Calculate neighbor centers and encode them
  const neighbors: string[] = [];
  const directions = [
    [1, 0], // N
    [1, 1], // NE
    [0, 1], // E
    [-1, 1], // SE
    [-1, 0], // S
    [-1, -1], // SW
    [0, -1], // W
    [1, -1], // NW
  ];

  for (const [latDir, lngDir] of directions) {
    let neighborLat = centerLat + latDir! * latDelta;
    let neighborLng = centerLng + lngDir! * lngDelta;

    // Clamp latitude
    neighborLat = Math.max(-90, Math.min(90, neighborLat));

    // Wrap longitude
    if (neighborLng > 180) neighborLng -= 360;
    if (neighborLng < -180) neighborLng += 360;

    neighbors.push(encodeGeohash(neighborLat, neighborLng, hash.length));
  }

  return neighbors;
}

/**
 * Get the bounding box for a geohash cell
 *
 * @param hash - The geohash string
 * @returns Bounding box with minLat, maxLat, minLng, maxLng
 */
function getGeohashBounds(hash: string): {
  minLat: number;
  maxLat: number;
  minLng: number;
  maxLng: number;
} {
  let latMin = -90;
  let latMax = 90;
  let lngMin = -180;
  let lngMax = 180;
  let isLng = true;

  const lowerHash = hash.toLowerCase();

  for (const char of lowerHash) {
    const value = GEOHASH_CHARS.indexOf(char);
    if (value === -1) continue;

    for (let bit = 4; bit >= 0; bit--) {
      const bitValue = (value >> bit) & 1;

      if (isLng) {
        const mid = (lngMin + lngMax) / 2;
        if (bitValue === 1) {
          lngMin = mid;
        } else {
          lngMax = mid;
        }
      } else {
        const mid = (latMin + latMax) / 2;
        if (bitValue === 1) {
          latMin = mid;
        } else {
          latMax = mid;
        }
      }

      isLng = !isLng;
    }
  }

  return { minLat: latMin, maxLat: latMax, minLng: lngMin, maxLng: lngMax };
}

/**
 * Get all geohash prefixes that cover a bounding box
 *
 * @param bbox - Bounding box
 * @param precision - Geohash precision to use
 * @returns Array of geohash prefixes that cover the bbox
 * @internal Reserved for future geohash-based spatial optimization
 */
export function getGeohashesForBBox(
  bbox: { minLat: number; maxLat: number; minLng: number; maxLng: number },
  precision: number
): string[] {
  const hashes = new Set<string>();

  // Calculate step size based on precision
  // Each geohash character roughly halves the cell size 2.5 times
  // Precision 1: ~5000km x 5000km
  // Precision 2: ~1250km x 625km
  // Precision 3: ~156km x 156km
  // Precision 4: ~39km x 20km
  // Precision 5: ~5km x 5km
  // Precision 6: ~1.2km x 0.6km
  // Precision 7: ~150m x 150m
  // Precision 8: ~19m x 19m
  const latStep = 180 / Math.pow(2, Math.ceil((precision * 5) / 2));
  const lngStep = 360 / Math.pow(2, Math.floor((precision * 5) / 2));

  // Handle antimeridian crossing (minLng > maxLng)
  if (bbox.minLng > bbox.maxLng) {
    // Split into two bboxes: [minLng, 180] and [-180, maxLng]
    for (let lat = bbox.minLat; lat <= bbox.maxLat; lat += latStep) {
      // Eastern side
      for (let lng = bbox.minLng; lng <= 180; lng += lngStep) {
        const clampedLat = Math.max(-90, Math.min(90, lat));
        const clampedLng = Math.max(-180, Math.min(180, lng));
        hashes.add(encodeGeohash(clampedLat, clampedLng, precision));
      }
      // Western side
      for (let lng = -180; lng <= bbox.maxLng; lng += lngStep) {
        const clampedLat = Math.max(-90, Math.min(90, lat));
        const clampedLng = Math.max(-180, Math.min(180, lng));
        hashes.add(encodeGeohash(clampedLat, clampedLng, precision));
      }
    }
  } else {
    // Normal case
    for (let lat = bbox.minLat; lat <= bbox.maxLat; lat += latStep) {
      for (let lng = bbox.minLng; lng <= bbox.maxLng; lng += lngStep) {
        const clampedLat = Math.max(-90, Math.min(90, lat));
        const clampedLng = Math.max(-180, Math.min(180, lng));
        hashes.add(encodeGeohash(clampedLat, clampedLng, precision));
      }
    }
  }

  return [...hashes];
}

// ============================================================================
// Query Functions
// ============================================================================

/**
 * Database row type for geo queries
 */
interface GeoRow extends Record<string, unknown> {
  id: number;
  subject: string;
  predicate: string;
  obj_lat: number;
  obj_lng: number;
  geohash: string | null;
}

/**
 * Row type for PRAGMA table_info result
 */
interface TableInfoRow extends Record<string, unknown> {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

/**
 * Query by bounding box
 *
 * Returns all GEO_POINT triples within the specified bounding box.
 * Uses geohash index for efficient filtering, then verifies exact bounds.
 *
 * @param sql - SqlStorage instance
 * @param query - Geo query with bbox
 * @returns Array of GeoResults
 */
export async function queryGeoBBox(sql: SqlStorage, query: GeoQuery): Promise<GeoResult[]> {
  if (!query.bbox) {
    return [];
  }

  const { minLat, maxLat, minLng, maxLng } = query.bbox;
  const isAntimeridianCrossing = minLng > maxLng;

  // Build SQL query
  let sqlQuery = `SELECT id, subject, predicate, obj_lat, obj_lng, geohash
                  FROM triples
                  WHERE obj_type = ?`;

  const params: (string | number)[] = [ObjectType.GEO_POINT];

  // Add predicate filter if specified
  if (query.predicate) {
    sqlQuery += ` AND predicate = ?`;
    params.push(query.predicate);
  }

  // Add latitude bounds (always straightforward)
  sqlQuery += ` AND obj_lat >= ? AND obj_lat <= ?`;
  params.push(minLat, maxLat);

  // Add longitude bounds (handle antimeridian crossing)
  if (isAntimeridianCrossing) {
    sqlQuery += ` AND (obj_lng >= ? OR obj_lng <= ?)`;
    params.push(minLng, maxLng);
  } else {
    sqlQuery += ` AND obj_lng >= ? AND obj_lng <= ?`;
    params.push(minLng, maxLng);
  }

  // Add limit
  if (query.limit) {
    sqlQuery += ` LIMIT ?`;
    params.push(query.limit);
  }

  // Execute query
  const rows = querySql<GeoRow>(sql, sqlQuery, ...params);

  // Convert to GeoResults
  return rows.map((row) => ({
    subject: row.subject as EntityId,
    predicate: row.predicate as Predicate,
    point: { lat: row.obj_lat, lng: row.obj_lng },
  }));
}

/**
 * Query by radius
 *
 * Returns all GEO_POINT triples within the specified radius of the center point.
 * Uses geohash index for efficient candidate filtering, then calculates exact
 * haversine distance for each candidate.
 *
 * Results are sorted by distance (nearest first).
 *
 * @param sql - SqlStorage instance
 * @param query - Geo query with center and radiusKm
 * @returns Array of GeoResults with distance
 */
export async function queryGeoRadius(sql: SqlStorage, query: GeoQuery): Promise<GeoResult[]> {
  if (!query.center || query.radiusKm === undefined) {
    return [];
  }

  const { center, radiusKm } = query;

  // Calculate bounding box for the radius (approximate)
  // This is used for initial filtering before haversine calculation
  const latDelta = (radiusKm / EARTH_RADIUS_KM) * (180 / Math.PI);

  // Avoid division by zero at poles and handle very large radii
  const cosLat = Math.cos((center.lat * Math.PI) / 180);
  const lngDelta = cosLat > 0.001 ? (radiusKm / EARTH_RADIUS_KM) * (180 / Math.PI) / cosLat : 360;

  // Handle edge cases near poles
  let minLat = center.lat - latDelta;
  let maxLat = center.lat + latDelta;
  let minLng = center.lng - lngDelta;
  let maxLng = center.lng + lngDelta;

  // Clamp latitude
  minLat = Math.max(-90, minLat);
  maxLat = Math.min(90, maxLat);

  // Check if we need to query all longitudes (very large radius or pole proximity)
  const coversAllLongitudes = lngDelta >= 180 || maxLat >= 90 || minLat <= -90;

  if (!coversAllLongitudes) {
    // Handle longitude wrap-around
    if (minLng < -180) minLng += 360;
    if (maxLng > 180) maxLng -= 360;
  }

  // Build SQL query with bounding box filter
  let sqlQuery = `SELECT id, subject, predicate, obj_lat, obj_lng, geohash
                  FROM triples
                  WHERE obj_type = ?`;

  const params: (string | number)[] = [ObjectType.GEO_POINT];

  // Add predicate filter if specified
  if (query.predicate) {
    sqlQuery += ` AND predicate = ?`;
    params.push(query.predicate);
  }

  // Add latitude bounds
  sqlQuery += ` AND obj_lat >= ? AND obj_lat <= ?`;
  params.push(minLat, maxLat);

  // Add longitude bounds (handle wrap-around and large radii)
  if (coversAllLongitudes) {
    // No longitude filter needed - query covers all longitudes
  } else if (minLng > maxLng) {
    // Antimeridian crossing
    sqlQuery += ` AND (obj_lng >= ? OR obj_lng <= ?)`;
    params.push(minLng, maxLng);
  } else {
    sqlQuery += ` AND obj_lng >= ? AND obj_lng <= ?`;
    params.push(minLng, maxLng);
  }

  // Execute query
  const rows = querySql<GeoRow>(sql, sqlQuery, ...params);

  // Filter by exact haversine distance and calculate distances
  const results: GeoResult[] = [];

  for (const row of rows) {
    const point: GeoPoint = { lat: row.obj_lat, lng: row.obj_lng };
    const distance = haversineDistance(center, point);

    // Special case: zero radius should still find exact matches
    // (within floating point precision)
    if (radiusKm === 0) {
      if (distance < 0.001) {
        // ~1 meter precision
        results.push({
          subject: row.subject as EntityId,
          predicate: row.predicate as Predicate,
          point,
          distanceKm: distance,
        });
      }
    } else if (distance <= radiusKm) {
      results.push({
        subject: row.subject as EntityId,
        predicate: row.predicate as Predicate,
        point,
        distanceKm: distance,
      });
    }
  }

  // Sort by distance
  results.sort((a, b) => (a.distanceKm ?? 0) - (b.distanceKm ?? 0));

  // Apply limit
  if (query.limit && results.length > query.limit) {
    return results.slice(0, query.limit);
  }

  return results;
}

/**
 * Insert a GEO_POINT triple and compute its geohash
 *
 * This is a convenience function that inserts a GEO_POINT triple
 * and immediately computes its geohash. Use this instead of raw INSERT
 * to ensure the geohash is populated.
 *
 * @param sql - SqlStorage instance
 * @param subject - Subject entity ID
 * @param predicate - Predicate name
 * @param lat - Latitude
 * @param lng - Longitude
 * @param timestamp - Triple timestamp
 * @param txId - Transaction ID
 */
export function insertGeoPointTriple(
  sql: SqlStorage,
  subject: string,
  predicate: string,
  lat: number,
  lng: number,
  timestamp: number,
  txId: string
): void {
  const geohash = encodeGeohash(lat, lng, DEFAULT_GEOHASH_PRECISION);

  sql.exec(
    `INSERT INTO triples (subject, predicate, obj_type, obj_lat, obj_lng, geohash, timestamp, tx_id)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
    subject,
    predicate,
    ObjectType.GEO_POINT,
    lat,
    lng,
    geohash,
    timestamp,
    txId
  );
}
