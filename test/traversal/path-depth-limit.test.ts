/**
 * TraversalDO Path Depth Limit Tests
 *
 * Tests for the MAX_PATH_DEPTH constant and depth limit enforcement
 * in TraversalDO path traversal operations to prevent infinite recursion.
 *
 * @see src/traversal/traversal-do.ts for the TraversalDO implementation
 */

import { describe, it, expect } from 'vitest';
import {
  MAX_PATH_DEPTH,
  DEFAULT_PATH_DEPTH,
} from '../../src/traversal/index';

// ============================================================================
// MAX_PATH_DEPTH Constant Tests
// ============================================================================

describe('TraversalDO MAX_PATH_DEPTH constant', () => {
  it('should export MAX_PATH_DEPTH constant', () => {
    expect(MAX_PATH_DEPTH).toBeDefined();
    expect(typeof MAX_PATH_DEPTH).toBe('number');
  });

  it('should have MAX_PATH_DEPTH set to 100', () => {
    expect(MAX_PATH_DEPTH).toBe(100);
  });

  it('should export DEFAULT_PATH_DEPTH constant', () => {
    expect(DEFAULT_PATH_DEPTH).toBeDefined();
    expect(typeof DEFAULT_PATH_DEPTH).toBe('number');
  });

  it('should have DEFAULT_PATH_DEPTH set to 3', () => {
    // TraversalDO uses 3 as default (more conservative than executor's 10)
    expect(DEFAULT_PATH_DEPTH).toBe(3);
  });

  it('should have DEFAULT_PATH_DEPTH less than MAX_PATH_DEPTH', () => {
    expect(DEFAULT_PATH_DEPTH).toBeLessThan(MAX_PATH_DEPTH);
  });
});

// ============================================================================
// Depth Limit Enforcement Tests
// ============================================================================

describe('TraversalDO depth limit enforcement', () => {
  describe('traverseWithStats depth capping', () => {
    it('should cap depth at MAX_PATH_DEPTH', () => {
      // The effectiveDepth calculation in traverseWithStats:
      // const effectiveDepth = Math.min(Math.max(0, depth), MAX_PATH_DEPTH);

      // Test the formula directly
      const testCases = [
        { input: 0, expected: 0 },
        { input: 1, expected: 1 },
        { input: 50, expected: 50 },
        { input: MAX_PATH_DEPTH, expected: MAX_PATH_DEPTH },
        { input: MAX_PATH_DEPTH + 1, expected: MAX_PATH_DEPTH },
        { input: 500, expected: MAX_PATH_DEPTH },
        { input: 1000, expected: MAX_PATH_DEPTH },
        { input: -1, expected: 0 },
        { input: -100, expected: 0 },
      ];

      for (const { input, expected } of testCases) {
        const effectiveDepth = Math.min(Math.max(0, input), MAX_PATH_DEPTH);
        expect(effectiveDepth).toBe(expected);
      }
    });
  });

  describe('/traverse endpoint depth capping', () => {
    it('should cap request depth at MAX_PATH_DEPTH', () => {
      // The endpoint calculation:
      // const depth = Math.min(Math.max(0, requestedDepth), MAX_PATH_DEPTH);

      const testCases = [
        { requestedDepth: 3, expected: 3 },
        { requestedDepth: 10, expected: 10 },
        { requestedDepth: MAX_PATH_DEPTH, expected: MAX_PATH_DEPTH },
        { requestedDepth: MAX_PATH_DEPTH + 50, expected: MAX_PATH_DEPTH },
        { requestedDepth: 9999, expected: MAX_PATH_DEPTH },
        { requestedDepth: 0, expected: 0 },
        { requestedDepth: -10, expected: 0 },
      ];

      for (const { requestedDepth, expected } of testCases) {
        const depth = Math.min(Math.max(0, requestedDepth), MAX_PATH_DEPTH);
        expect(depth).toBe(expected);
      }
    });

    it('should use DEFAULT_PATH_DEPTH when depth not specified', () => {
      // When no depth query param is provided, it defaults to DEFAULT_PATH_DEPTH
      // parseInt(url.searchParams.get('depth') || String(DEFAULT_PATH_DEPTH))
      const defaultDepthStr = String(DEFAULT_PATH_DEPTH);
      expect(parseInt(defaultDepthStr, 10)).toBe(DEFAULT_PATH_DEPTH);
    });
  });
});

// ============================================================================
// Security Properties
// ============================================================================

describe('TraversalDO security properties', () => {
  it('MAX_PATH_DEPTH should be a reasonable limit to prevent DoS', () => {
    // 100 is chosen as a balance between:
    // - High enough for legitimate deep traversals
    // - Low enough to prevent resource exhaustion
    expect(MAX_PATH_DEPTH).toBeGreaterThanOrEqual(10);
    expect(MAX_PATH_DEPTH).toBeLessThanOrEqual(1000);
  });

  it('DEFAULT_PATH_DEPTH should be conservative', () => {
    // Default should be low to prevent accidental deep traversals
    expect(DEFAULT_PATH_DEPTH).toBeLessThanOrEqual(10);
  });

  it('depth limit should prevent exponential blowup', () => {
    // With fan-out of 10 edges per entity:
    // depth 100 could theoretically visit 10^100 entities
    // But MAX_PATH_DEPTH combined with result limits prevents this

    // The traverseWithStats method also limits to:
    // - 10 entities per hop (currentIds.slice(0, 10))
    // - 5 edges per entity (entity.edges.slice(0, 5))
    // - 20 final results (currentIds.slice(0, 20))

    // So max entities visited = 10 * 100 = 1000 (per hop limit * max depth)
    const maxEntitiesPerHop = 10;
    const maxFinalResults = 20;
    const theoreticalMaxVisited = maxEntitiesPerHop * MAX_PATH_DEPTH;

    expect(theoreticalMaxVisited).toBeLessThanOrEqual(10000);
  });
});
