/**
 * Geospatial types and utilities for GraphDB
 *
 * Includes GeoPoint, GeoPolygon, GeoLineString types
 * and geohash encoding/decoding functions.
 */

// ============================================================================
// Geospatial Types
// ============================================================================

/**
 * A geographic point with latitude and longitude
 * - lat: -90 to 90 (degrees)
 * - lng: -180 to 180 (degrees)
 */
export interface GeoPoint {
  lat: number; // -90 to 90
  lng: number; // -180 to 180
}

/**
 * A geographic polygon with an exterior ring and optional holes
 * - exterior: closed ring of points (first and last point must be the same)
 * - holes: optional array of closed rings representing holes in the polygon
 */
export interface GeoPolygon {
  exterior: GeoPoint[];
  holes?: GeoPoint[][];
}

/**
 * A geographic line string (sequence of connected points)
 * - points: at least 2 points
 */
export interface GeoLineString {
  points: GeoPoint[];
}

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Check if a GeoPoint has valid coordinates
 * - lat must be between -90 and 90
 * - lng must be between -180 and 180
 * - Values must be finite numbers (not NaN or Infinity)
 */
export function isValidGeoPoint(point: GeoPoint): boolean {
  if (typeof point?.lat !== 'number' || typeof point?.lng !== 'number') {
    return false;
  }

  // Check for NaN and Infinity
  if (!Number.isFinite(point.lat) || !Number.isFinite(point.lng)) {
    return false;
  }

  // Check lat bounds: -90 to 90
  if (point.lat < -90 || point.lat > 90) {
    return false;
  }

  // Check lng bounds: -180 to 180
  if (point.lng < -180 || point.lng > 180) {
    return false;
  }

  return true;
}

/**
 * Check if a ring is closed (first and last points are the same)
 */
function isClosedRing(ring: GeoPoint[]): boolean {
  if (ring.length < 4) {
    return false;
  }
  const first = ring[0];
  const last = ring[ring.length - 1];
  if (first === undefined || last === undefined) {
    return false;
  }
  return first.lat === last.lat && first.lng === last.lng;
}

/**
 * Check if a GeoPolygon is valid
 * - exterior must have at least 4 points (minimum for a closed ring)
 * - exterior must be closed (first and last point same)
 * - all points must be valid
 * - holes (if present) must also be valid closed rings
 */
export function isValidGeoPolygon(polygon: GeoPolygon): boolean {
  if (!polygon?.exterior || !Array.isArray(polygon.exterior)) {
    return false;
  }

  // Exterior must have at least 4 points for a valid closed ring
  if (polygon.exterior.length < 4) {
    return false;
  }

  // Exterior must be closed
  if (!isClosedRing(polygon.exterior)) {
    return false;
  }

  // All exterior points must be valid
  for (const point of polygon.exterior) {
    if (!isValidGeoPoint(point)) {
      return false;
    }
  }

  // Validate holes if present
  if (polygon.holes) {
    for (const hole of polygon.holes) {
      if (hole.length < 4 || !isClosedRing(hole)) {
        return false;
      }
      for (const point of hole) {
        if (!isValidGeoPoint(point)) {
          return false;
        }
      }
    }
  }

  return true;
}

/**
 * Check if a GeoLineString is valid
 * - must have at least 2 points
 * - all points must be valid
 */
export function isValidGeoLineString(line: GeoLineString): boolean {
  if (!line?.points || !Array.isArray(line.points)) {
    return false;
  }

  // Must have at least 2 points
  if (line.points.length < 2) {
    return false;
  }

  // All points must be valid
  for (const point of line.points) {
    if (!isValidGeoPoint(point)) {
      return false;
    }
  }

  return true;
}

// ============================================================================
// Geohash Encoding/Decoding
// ============================================================================

/**
 * Geohash Base32 characters (Crockford variant without i, l, o)
 * Standard geohash uses: 0123456789bcdefghjkmnpqrstuvwxyz
 */
const GEOHASH_CHARS = '0123456789bcdefghjkmnpqrstuvwxyz';

/**
 * Lookup table for decoding geohash characters to their index
 */
const GEOHASH_DECODE: Record<string, number> = {};
for (let i = 0; i < GEOHASH_CHARS.length; i++) {
  const char = GEOHASH_CHARS[i];
  if (char !== undefined) {
    GEOHASH_DECODE[char] = i;
  }
}

/**
 * Encode latitude/longitude to a geohash string
 *
 * @param lat Latitude (-90 to 90)
 * @param lng Longitude (-180 to 180)
 * @param precision Number of characters in the geohash (default: 9)
 * @returns Geohash string
 * @throws Error if coordinates are invalid
 */
export function encodeGeohash(
  lat: number,
  lng: number,
  precision: number = 9
): string {
  // Validate inputs
  if (!Number.isFinite(lat) || lat < -90 || lat > 90) {
    throw new Error(`Invalid latitude: ${lat}. Must be between -90 and 90.`);
  }
  if (!Number.isFinite(lng) || lng < -180 || lng > 180) {
    throw new Error(
      `Invalid longitude: ${lng}. Must be between -180 and 180.`
    );
  }

  let latMin = -90;
  let latMax = 90;
  let lngMin = -180;
  let lngMax = 180;

  let hash = '';
  let bit = 0;
  let ch = 0;
  let isLng = true; // Alternate between longitude and latitude, starting with longitude

  while (hash.length < precision) {
    if (isLng) {
      const mid = (lngMin + lngMax) / 2;
      if (lng >= mid) {
        ch = (ch << 1) | 1;
        lngMin = mid;
      } else {
        ch = ch << 1;
        lngMax = mid;
      }
    } else {
      const mid = (latMin + latMax) / 2;
      if (lat >= mid) {
        ch = (ch << 1) | 1;
        latMin = mid;
      } else {
        ch = ch << 1;
        latMax = mid;
      }
    }

    isLng = !isLng;
    bit++;

    if (bit === 5) {
      hash += GEOHASH_CHARS[ch];
      bit = 0;
      ch = 0;
    }
  }

  return hash;
}

/**
 * Decode a geohash string to latitude/longitude
 *
 * @param hash Geohash string to decode
 * @returns GeoPoint with lat and lng (center of the geohash cell)
 * @throws Error if the geohash is invalid
 */
export function decodeGeohash(hash: string): GeoPoint {
  if (!hash || typeof hash !== 'string' || hash.length === 0) {
    throw new Error('Invalid geohash: empty string');
  }

  // Validate all characters are valid geohash chars
  const lowerHash = hash.toLowerCase();
  for (const char of lowerHash) {
    if (GEOHASH_DECODE[char] === undefined) {
      throw new Error(
        `Invalid geohash character: '${char}'. Valid characters: ${GEOHASH_CHARS}`
      );
    }
  }

  let latMin = -90;
  let latMax = 90;
  let lngMin = -180;
  let lngMax = 180;
  let isLng = true;

  for (const char of lowerHash) {
    const value = GEOHASH_DECODE[char] ?? 0;

    // Process 5 bits
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

  // Return the center of the bounding box
  return {
    lat: (latMin + latMax) / 2,
    lng: (lngMin + lngMax) / 2,
  };
}

// ============================================================================
// Geohash Neighbor Functions
// ============================================================================

/**
 * Neighbor lookup tables for geohash
 * Each entry maps a BASE32 character (by index) to its neighbor character in that direction
 * Standard geohash neighbor encoding from https://github.com/davetroy/geohash-js
 */
const NEIGHBORS: Record<string, Record<string, string>> = {
  n: { even: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy', odd: 'bc01fg45238967deuvhjyznpkmstqrwx' },
  s: { even: '14365h7k9dcfesgujnmqp0r2twvyx8zb', odd: '238967debc01fg45hjnpkmstqrwxuvyz' },
  e: { even: 'bc01fg45238967deuvhjyznpkmstqrwx', odd: 'p0r21436x8zb9dcf5h7kjnmqesgutwvy' },
  w: { even: '238967debc01fg45hjnpkmstqrwxuvyz', odd: '14365h7k9dcfesgujnmqp0r2twvyx8zb' },
};

const BORDERS: Record<string, Record<string, string>> = {
  n: { even: 'prxz', odd: 'bcfguvyz' },
  s: { even: '028b', odd: '0145hjnp' },
  e: { even: 'bcfguvyz', odd: 'prxz' },
  w: { even: '0145hjnp', odd: '028b' },
};

/**
 * Get adjacent geohash in specified direction
 *
 * @param geohash - The geohash to find the neighbor of
 * @param direction - Direction: 'n' (north), 's' (south), 'e' (east), 'w' (west)
 * @returns Adjacent geohash in the specified direction, or empty string at map edge
 */
function getAdjacent(geohash: string, direction: string): string {
  if (geohash.length === 0) return '';

  const lastChar = geohash.charAt(geohash.length - 1);
  const parent = geohash.substring(0, geohash.length - 1);
  const type = geohash.length % 2 === 0 ? 'even' : 'odd';

  // Check if we need to recurse to parent cell
  if (BORDERS[direction]![type]!.indexOf(lastChar) !== -1 && parent.length > 0) {
    const newParent = getAdjacent(parent, direction);
    if (newParent === '') return ''; // Edge of map
    return newParent + GEOHASH_CHARS[NEIGHBORS[direction]![type]!.indexOf(lastChar)];
  }

  return parent + GEOHASH_CHARS[NEIGHBORS[direction]![type]!.indexOf(lastChar)];
}

/**
 * Get neighboring geohash cells (for radius queries)
 * Returns the center cell plus all 8 adjacent cells
 *
 * @param geohash - The center geohash
 * @returns Array of geohashes: center + 8 neighbors (N, S, E, W, NE, NW, SE, SW)
 *
 * @example
 * ```typescript
 * const neighbors = getGeohashNeighbors("u4pruydq");
 * // Returns array with center + 8 adjacent cells
 * ```
 */
export function getGeohashNeighbors(geohash: string): string[] {
  if (geohash.length === 0) return [];

  const neighbors: string[] = [geohash];

  // Get direct neighbors (N, S, E, W)
  const n = getAdjacent(geohash, 'n');
  const s = getAdjacent(geohash, 's');
  const e = getAdjacent(geohash, 'e');
  const w = getAdjacent(geohash, 'w');

  if (n) neighbors.push(n);
  if (s) neighbors.push(s);
  if (e) neighbors.push(e);
  if (w) neighbors.push(w);

  // Get diagonal neighbors (NE, NW, SE, SW)
  if (n) {
    const ne = getAdjacent(n, 'e');
    const nw = getAdjacent(n, 'w');
    if (ne) neighbors.push(ne);
    if (nw) neighbors.push(nw);
  }
  if (s) {
    const se = getAdjacent(s, 'e');
    const sw = getAdjacent(s, 'w');
    if (se) neighbors.push(se);
    if (sw) neighbors.push(sw);
  }

  return neighbors;
}
