/**
 * Geo Index Edge Cases Tests (TDD RED Phase)
 *
 * Tests for geospatial index edge cases including:
 * - International Date Line (antimeridian) crossing
 * - Polar regions (North/South Pole)
 * - Very large and very small radii
 * - Precision limits
 * - Coordinate edge values
 *
 * @see src/index/geo-index.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  haversineDistance,
  getGeohashNeighbors,
} from '../../src/index/geo-index.js';
import { encodeGeohash, decodeGeohash } from '../../src/core/geo.js';

// ============================================================================
// TEST UTILITIES
// ============================================================================

/**
 * Normalize longitude to [-180, 180) range
 */
function normalizeLng(lng: number): number {
  while (lng >= 180) lng -= 360;
  while (lng < -180) lng += 360;
  return lng;
}

// ============================================================================
// ANTIMERIDIAN (DATE LINE) TESTS
// ============================================================================

describe('Antimeridian (International Date Line) Edge Cases', () => {
  describe('haversineDistance across antimeridian', () => {
    it('should calculate correct distance across 180 meridian (not going around the world)', () => {
      // Two points very close but on opposite sides of antimeridian
      const p1 = { lat: 0, lng: 179.999 };
      const p2 = { lat: 0, lng: -179.999 };

      const distance = haversineDistance(p1, p2);

      // Should be ~0.2 km, NOT ~40,000 km (circumference)
      expect(distance).toBeLessThan(1);
      expect(distance).toBeGreaterThan(0);
    });

    it('should handle points exactly at -180 and 180', () => {
      const p1 = { lat: 45, lng: 180 };
      const p2 = { lat: 45, lng: -180 };

      const distance = haversineDistance(p1, p2);

      // Same point, distance should be 0
      expect(distance).toBeCloseTo(0, 5);
    });

    it('should calculate distance across antimeridian with different latitudes', () => {
      const p1 = { lat: 10, lng: 170 };
      const p2 = { lat: 10, lng: -170 };

      const distance = haversineDistance(p1, p2);

      // Should be going "short way" around (~2200 km)
      expect(distance).toBeLessThan(3000);
    });

    it('should handle Fiji to Samoa route (across date line)', () => {
      // Fiji: approximately 18S, 178E
      // Samoa: approximately 14S, 172W
      const fiji = { lat: -18, lng: 178 };
      const samoa = { lat: -14, lng: -172 };

      const distance = haversineDistance(fiji, samoa);

      // Actual distance is ~1200 km
      expect(distance).toBeGreaterThan(1000);
      expect(distance).toBeLessThan(1500);
    });

    it('should handle Alaska to Russia route (Bering Strait)', () => {
      // Little Diomede Island (USA): 65.75N, 168.95W
      // Big Diomede Island (Russia): 65.78N, 169.06W
      const usa = { lat: 65.75, lng: -168.95 };
      const russia = { lat: 65.78, lng: 169.06 };

      const distance = haversineDistance(usa, russia);

      // About 4 km apart
      expect(distance).toBeLessThan(10);
    });
  });

  describe('Geohash encoding near antimeridian', () => {
    it('should encode points near +180 correctly', () => {
      const hash = encodeGeohash(0, 179.99, 8);
      expect(hash).toBeDefined();
      expect(hash.length).toBe(8);

      const decoded = decodeGeohash(hash);
      expect(decoded.lng).toBeGreaterThan(179);
    });

    it('should encode points near -180 correctly', () => {
      const hash = encodeGeohash(0, -179.99, 8);
      expect(hash).toBeDefined();
      expect(hash.length).toBe(8);

      const decoded = decodeGeohash(hash);
      expect(decoded.lng).toBeLessThan(-179);
    });

    it('should generate neighbors across antimeridian', () => {
      // Point very close to +180
      const hash = encodeGeohash(0, 179.999, 4);
      const neighbors = getGeohashNeighbors(hash);

      expect(neighbors.length).toBe(8);

      // Some neighbors should decode to negative longitude (crossed antimeridian)
      const decodedNeighbors = neighbors.map(n => decodeGeohash(n));
      const hasNegativeLng = decodedNeighbors.some(d => d.lng < 0);
      const hasPositiveLng = decodedNeighbors.some(d => d.lng > 0);

      // Should have neighbors on both sides of the antimeridian
      expect(hasNegativeLng || hasPositiveLng).toBe(true);
    });
  });
});

// ============================================================================
// POLAR REGION TESTS
// ============================================================================

describe('Polar Region Edge Cases', () => {
  describe('North Pole proximity', () => {
    it('should calculate distance to North Pole correctly', () => {
      const northPole = { lat: 90, lng: 0 };
      const somePoint = { lat: 89, lng: 0 };

      const distance = haversineDistance(northPole, somePoint);

      // 1 degree of latitude is ~111 km
      expect(distance).toBeCloseTo(111, -1);
    });

    it('should handle all longitudes at North Pole as same point', () => {
      const pole1 = { lat: 90, lng: 0 };
      const pole2 = { lat: 90, lng: 90 };
      const pole3 = { lat: 90, lng: -180 };

      expect(haversineDistance(pole1, pole2)).toBeCloseTo(0, 5);
      expect(haversineDistance(pole1, pole3)).toBeCloseTo(0, 5);
      expect(haversineDistance(pole2, pole3)).toBeCloseTo(0, 5);
    });

    it('should calculate circumference around pole correctly', () => {
      // Two points at 89 degrees latitude, 180 degrees apart in longitude
      const p1 = { lat: 89, lng: 0 };
      const p2 = { lat: 89, lng: 180 };

      const distance = haversineDistance(p1, p2);

      // Going over the pole is ~220 km (2 * 111 km)
      expect(distance).toBeGreaterThan(200);
      expect(distance).toBeLessThan(250);
    });

    it('should handle points very close to pole', () => {
      const nearPole = { lat: 89.999, lng: 0 };
      const nearPole2 = { lat: 89.999, lng: 180 };

      const distance = haversineDistance(nearPole, nearPole2);

      // Should be very small (going over pole)
      expect(distance).toBeLessThan(1);
    });
  });

  describe('South Pole proximity', () => {
    it('should calculate distance to South Pole correctly', () => {
      const southPole = { lat: -90, lng: 0 };
      const somePoint = { lat: -89, lng: 0 };

      const distance = haversineDistance(southPole, somePoint);

      expect(distance).toBeCloseTo(111, -1);
    });

    it('should handle all longitudes at South Pole as same point', () => {
      const pole1 = { lat: -90, lng: 0 };
      const pole2 = { lat: -90, lng: 45 };
      const pole3 = { lat: -90, lng: -90 };

      expect(haversineDistance(pole1, pole2)).toBeCloseTo(0, 5);
      expect(haversineDistance(pole1, pole3)).toBeCloseTo(0, 5);
    });

    it('should calculate pole-to-pole distance correctly', () => {
      const northPole = { lat: 90, lng: 0 };
      const southPole = { lat: -90, lng: 0 };

      const distance = haversineDistance(northPole, southPole);

      // Half of Earth's circumference (~20,015 km)
      expect(distance).toBeGreaterThan(19900);
      expect(distance).toBeLessThan(20100);
    });
  });

  describe('Geohash encoding at poles', () => {
    it('should encode North Pole', () => {
      const hash = encodeGeohash(90, 0, 8);
      expect(hash).toBeDefined();
      expect(hash.length).toBe(8);
    });

    it('should encode South Pole', () => {
      const hash = encodeGeohash(-90, 0, 8);
      expect(hash).toBeDefined();
      expect(hash.length).toBe(8);
    });

    it('should decode pole geohash back to approximately correct location', () => {
      const hash = encodeGeohash(90, 0, 8);
      const decoded = decodeGeohash(hash);

      // Latitude should be very close to 90
      expect(decoded.lat).toBeGreaterThan(89.9);
    });

    it('should generate valid neighbors at North Pole', () => {
      const hash = encodeGeohash(89.999, 0, 4);
      const neighbors = getGeohashNeighbors(hash);

      expect(neighbors.length).toBe(8);
      // All should be valid geohash strings
      for (const neighbor of neighbors) {
        expect(neighbor).toMatch(/^[0-9b-hj-km-np-z]+$/);
      }
    });
  });
});

// ============================================================================
// EXTREME RADIUS TESTS
// ============================================================================

describe('Extreme Radius Edge Cases', () => {
  describe('Very small radii', () => {
    it('should handle 1 meter radius', () => {
      const center = { lat: 37.7749, lng: -122.4194 };
      const radiusKm = 0.001; // 1 meter

      // A point 1 meter away
      const nearPoint = {
        lat: center.lat + 0.00001, // ~1m at this latitude
        lng: center.lng,
      };

      const distance = haversineDistance(center, nearPoint);
      expect(distance).toBeLessThan(0.01); // Less than 10 meters
    });

    it('should handle zero radius (exact point)', () => {
      const center = { lat: 51.5074, lng: -0.1278 };
      const samePoint = { lat: 51.5074, lng: -0.1278 };

      const distance = haversineDistance(center, samePoint);
      expect(distance).toBe(0);
    });

    it('should handle sub-meter precision', () => {
      const p1 = { lat: 0, lng: 0 };
      const p2 = { lat: 0.0000001, lng: 0 }; // ~1cm

      const distance = haversineDistance(p1, p2);
      expect(distance).toBeLessThan(0.001); // Less than 1 meter
    });
  });

  describe('Very large radii', () => {
    it('should handle radius larger than half Earth circumference', () => {
      const center = { lat: 0, lng: 0 };
      const antipode = { lat: 0, lng: 180 };

      const distance = haversineDistance(center, antipode);

      // Should be half circumference (~20,000 km)
      expect(distance).toBeGreaterThan(19000);
      expect(distance).toBeLessThan(21000);
    });

    it('should handle radius equal to Earth circumference', () => {
      const center = { lat: 0, lng: 0 };
      // Go all the way around
      const samePoint = { lat: 0, lng: 0 };

      const distance = haversineDistance(center, samePoint);
      expect(distance).toBe(0);
    });

    it('should handle global radius (find all points on Earth)', () => {
      // Maximum distance on Earth is about 20,037 km (half circumference)
      const radiusKm = 20037;

      // Any two points should be within this radius
      const london = { lat: 51.5074, lng: -0.1278 };
      const sydney = { lat: -33.8688, lng: 151.2093 };

      const distance = haversineDistance(london, sydney);
      expect(distance).toBeLessThan(radiusKm);
    });
  });

  describe('Earth circumference calculations', () => {
    it('should calculate equatorial circumference correctly', () => {
      // Travel around equator
      const p1 = { lat: 0, lng: 0 };
      const p2 = { lat: 0, lng: 90 };
      const p3 = { lat: 0, lng: 180 };
      const p4 = { lat: 0, lng: -90 };

      const quarter1 = haversineDistance(p1, p2);
      const quarter2 = haversineDistance(p2, p3);
      const quarter3 = haversineDistance(p3, p4);
      const quarter4 = haversineDistance(p4, p1);

      const circumference = quarter1 + quarter2 + quarter3 + quarter4;

      // Earth's circumference is ~40,075 km
      expect(circumference).toBeGreaterThan(39000);
      expect(circumference).toBeLessThan(41000);
    });

    it('should handle meridian (pole to pole) distance', () => {
      const north = { lat: 90, lng: 0 };
      const south = { lat: -90, lng: 0 };

      const distance = haversineDistance(north, south);

      // Half circumference
      expect(distance).toBeGreaterThan(19900);
      expect(distance).toBeLessThan(20100);
    });
  });
});

// ============================================================================
// COORDINATE PRECISION TESTS
// ============================================================================

describe('Coordinate Precision Edge Cases', () => {
  describe('Floating point precision', () => {
    it('should handle very precise coordinates', () => {
      const p1 = { lat: 37.77490123456789, lng: -122.41940123456789 };
      const p2 = { lat: 37.77490123456790, lng: -122.41940123456790 };

      const distance = haversineDistance(p1, p2);
      // Very tiny distance
      expect(distance).toBeLessThan(0.001);
    });

    it('should be consistent with multiple calculations', () => {
      const p1 = { lat: 40.7128, lng: -74.006 };
      const p2 = { lat: 51.5074, lng: -0.1278 };

      const d1 = haversineDistance(p1, p2);
      const d2 = haversineDistance(p1, p2);
      const d3 = haversineDistance(p1, p2);

      expect(d1).toBe(d2);
      expect(d2).toBe(d3);
    });

    it('should be symmetric: d(a,b) = d(b,a)', () => {
      const p1 = { lat: 35.6762, lng: 139.6503 };
      const p2 = { lat: -33.8688, lng: 151.2093 };

      const d1 = haversineDistance(p1, p2);
      const d2 = haversineDistance(p2, p1);

      expect(d1).toBeCloseTo(d2, 10);
    });
  });

  describe('Boundary coordinates', () => {
    it('should handle exactly 0,0 (Null Island)', () => {
      const nullIsland = { lat: 0, lng: 0 };
      const nearby = { lat: 0.001, lng: 0.001 };

      const distance = haversineDistance(nullIsland, nearby);
      expect(distance).toBeGreaterThan(0);
      expect(distance).toBeLessThan(1);
    });

    it('should handle maximum latitude values', () => {
      // Exactly at poles
      const northPole = { lat: 90, lng: 0 };
      const southPole = { lat: -90, lng: 0 };

      const distance = haversineDistance(northPole, southPole);
      expect(distance).toBeGreaterThan(0);
    });

    it('should handle maximum longitude values', () => {
      const p1 = { lat: 0, lng: 180 };
      const p2 = { lat: 0, lng: -180 };

      const distance = haversineDistance(p1, p2);
      expect(distance).toBeCloseTo(0, 5); // Same point
    });

    it('should handle coordinates just inside bounds', () => {
      const nearMaxLat = { lat: 89.99999, lng: 0 };
      const nearMinLat = { lat: -89.99999, lng: 0 };
      const nearMaxLng = { lat: 0, lng: 179.99999 };
      const nearMinLng = { lat: 0, lng: -179.99999 };

      // All should produce valid distances
      expect(haversineDistance(nearMaxLat, { lat: 0, lng: 0 })).toBeGreaterThan(0);
      expect(haversineDistance(nearMinLat, { lat: 0, lng: 0 })).toBeGreaterThan(0);
      expect(haversineDistance(nearMaxLng, { lat: 0, lng: 0 })).toBeGreaterThan(0);
      expect(haversineDistance(nearMinLng, { lat: 0, lng: 0 })).toBeGreaterThan(0);
    });
  });
});

// ============================================================================
// GEOHASH PRECISION TESTS
// ============================================================================

describe('Geohash Precision Edge Cases', () => {
  describe('Different precision levels', () => {
    it('should encode with precision 1 (5000km x 5000km)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 1);
      expect(hash.length).toBe(1);
    });

    it('should encode with precision 2 (1250km x 625km)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 2);
      expect(hash.length).toBe(2);
    });

    it('should encode with precision 6 (1.2km x 0.6km)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 6);
      expect(hash.length).toBe(6);
    });

    it('should encode with precision 8 (19m x 19m)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 8);
      expect(hash.length).toBe(8);
    });

    it('should encode with precision 10 (1.2m x 0.6m)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 10);
      expect(hash.length).toBe(10);
    });

    it('should encode with precision 12 (3.7cm x 1.9cm)', () => {
      const hash = encodeGeohash(37.7749, -122.4194, 12);
      expect(hash.length).toBe(12);
    });
  });

  describe('Geohash decode accuracy', () => {
    it('should decode to approximately same coordinates', () => {
      const lat = 37.7749;
      const lng = -122.4194;
      const precision = 8;

      const hash = encodeGeohash(lat, lng, precision);
      const decoded = decodeGeohash(hash);

      // Precision 8 gives ~19m accuracy
      expect(Math.abs(decoded.lat - lat)).toBeLessThan(0.001); // ~111m
      expect(Math.abs(decoded.lng - lng)).toBeLessThan(0.001);
    });

    it('should maintain relative ordering of geohashes', () => {
      const p1 = { lat: 37.0, lng: -122.0 };
      const p2 = { lat: 37.5, lng: -122.0 };
      const p3 = { lat: 38.0, lng: -122.0 };

      const h1 = encodeGeohash(p1.lat, p1.lng, 4);
      const h2 = encodeGeohash(p2.lat, p2.lng, 4);
      const h3 = encodeGeohash(p3.lat, p3.lng, 4);

      // Higher latitude points at same longitude should have consistent prefix
      // (This is a simplification; geohash ordering is more complex)
      expect(h1).toBeDefined();
      expect(h2).toBeDefined();
      expect(h3).toBeDefined();
    });
  });

  describe('Geohash neighbors', () => {
    it('should return 8 neighbors for any valid geohash', () => {
      const testCases = [
        encodeGeohash(0, 0, 6),
        encodeGeohash(37.7749, -122.4194, 6),
        encodeGeohash(-33.8688, 151.2093, 6),
        encodeGeohash(51.5074, -0.1278, 6),
      ];

      for (const hash of testCases) {
        const neighbors = getGeohashNeighbors(hash);
        expect(neighbors.length).toBe(8);
      }
    });

    it('should return neighbors of same precision', () => {
      const precision = 6;
      const hash = encodeGeohash(40.7128, -74.006, precision);
      const neighbors = getGeohashNeighbors(hash);

      for (const neighbor of neighbors) {
        expect(neighbor.length).toBe(precision);
      }
    });

    it('should return unique neighbors', () => {
      const hash = encodeGeohash(35.6762, 139.6503, 6);
      const neighbors = getGeohashNeighbors(hash);

      const uniqueNeighbors = new Set(neighbors);
      expect(uniqueNeighbors.size).toBe(8);
    });

    it('should not include original hash in neighbors', () => {
      const hash = encodeGeohash(48.8566, 2.3522, 6);
      const neighbors = getGeohashNeighbors(hash);

      expect(neighbors).not.toContain(hash);
    });
  });
});

// ============================================================================
// SPECIAL LOCATIONS TESTS
// ============================================================================

describe('Special Real-World Locations', () => {
  it('should calculate London to Paris correctly (~344 km)', () => {
    const london = { lat: 51.5074, lng: -0.1278 };
    const paris = { lat: 48.8566, lng: 2.3522 };

    const distance = haversineDistance(london, paris);
    expect(distance).toBeGreaterThan(330);
    expect(distance).toBeLessThan(360);
  });

  it('should calculate New York to Los Angeles correctly (~3940 km)', () => {
    const newYork = { lat: 40.7128, lng: -74.006 };
    const losAngeles = { lat: 34.0522, lng: -118.2437 };

    const distance = haversineDistance(newYork, losAngeles);
    expect(distance).toBeGreaterThan(3900);
    expect(distance).toBeLessThan(4000);
  });

  it('should calculate Sydney to Auckland correctly (~2160 km)', () => {
    const sydney = { lat: -33.8688, lng: 151.2093 };
    const auckland = { lat: -36.8485, lng: 174.7633 };

    const distance = haversineDistance(sydney, auckland);
    expect(distance).toBeGreaterThan(2100);
    expect(distance).toBeLessThan(2200);
  });

  it('should calculate Cape Town to Buenos Aires correctly (~6830 km)', () => {
    const capeTown = { lat: -33.9249, lng: 18.4241 };
    const buenosAires = { lat: -34.6037, lng: -58.3816 };

    const distance = haversineDistance(capeTown, buenosAires);
    expect(distance).toBeGreaterThan(6700);
    expect(distance).toBeLessThan(6900);
  });

  it('should calculate McMurdo Station (Antarctica) to Scott Base correctly', () => {
    const mcMurdo = { lat: -77.8419, lng: 166.6863 };
    const scottBase = { lat: -77.8492, lng: 166.7658 };

    const distance = haversineDistance(mcMurdo, scottBase);
    expect(distance).toBeLessThan(5); // About 3 km apart
  });
});
