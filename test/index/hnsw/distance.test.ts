/**
 * HNSW Distance Functions Tests
 *
 * TDD tests for pure TypeScript distance metrics used for
 * vector similarity search in HNSW indexes.
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  cosineDistance,
  euclideanDistance,
  innerProduct,
} from '../../../src/index/hnsw/distance.js';

describe('HNSW Distance Functions', () => {
  // ============================================================================
  // COSINE DISTANCE TESTS
  // ============================================================================

  describe('cosineDistance', () => {
    describe('basic functionality', () => {
      it('should return 0 for identical vectors', () => {
        const a = [1, 2, 3, 4, 5];
        const distance = cosineDistance(a, a);
        expect(distance).toBeCloseTo(0, 10);
      });

      it('should return 0 for proportional vectors', () => {
        const a = [1, 2, 3];
        const b = [2, 4, 6]; // 2 * a
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 10);
      });

      it('should return 2 for opposite vectors', () => {
        const a = [1, 0, 0];
        const b = [-1, 0, 0];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(2, 10);
      });

      it('should return 1 for orthogonal vectors', () => {
        const a = [1, 0];
        const b = [0, 1];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(1, 10);
      });

      it('should return 1 for 3D orthogonal vectors', () => {
        const a = [1, 0, 0];
        const b = [0, 1, 0];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(1, 10);
      });
    });

    describe('edge cases', () => {
      it('should throw for mismatched dimensions', () => {
        expect(() => cosineDistance([1, 2], [1, 2, 3])).toThrow(
          'Vector dimension mismatch: 2 vs 3'
        );
      });

      it('should throw for empty vs non-empty vectors', () => {
        expect(() => cosineDistance([], [1, 2, 3])).toThrow(
          'Vector dimension mismatch: 0 vs 3'
        );
      });

      it('should handle zero vectors by returning max distance', () => {
        const zeroVec = [0, 0, 0];
        const normalVec = [1, 2, 3];
        const distance = cosineDistance(zeroVec, normalVec);
        expect(distance).toBe(2);
      });

      it('should handle both zero vectors', () => {
        const distance = cosineDistance([0, 0, 0], [0, 0, 0]);
        expect(distance).toBe(2);
      });

      it('should handle single-dimension vectors', () => {
        expect(cosineDistance([1], [1])).toBeCloseTo(0, 10);
        expect(cosineDistance([1], [-1])).toBeCloseTo(2, 10);
      });

      it('should handle very small values', () => {
        const a = [1e-10, 2e-10, 3e-10];
        const b = [2e-10, 4e-10, 6e-10];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 5);
      });

      it('should handle very large values', () => {
        const a = [1e10, 2e10, 3e10];
        const b = [2e10, 4e10, 6e10];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 5);
      });

      it('should handle mixed positive and negative values', () => {
        const a = [1, -2, 3, -4];
        const b = [1, -2, 3, -4];
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 10);
      });
    });

    describe('high-dimensional vectors', () => {
      it('should work with 128-dimensional vectors', () => {
        const dim = 128;
        const a = Array(dim).fill(0).map((_, i) => Math.sin(i));
        const b = Array(dim).fill(0).map((_, i) => Math.sin(i));
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 10);
      });

      it('should work with 384-dimensional vectors (common embedding size)', () => {
        const dim = 384;
        const a = Array(dim).fill(0).map((_, i) => Math.random() - 0.5);
        const b = [...a]; // identical
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 10);
      });

      it('should handle 1536-dimensional vectors (OpenAI embedding size)', () => {
        const dim = 1536;
        const a = Array(dim).fill(0).map((_, i) => (i % 10) / 10);
        const b = Array(dim).fill(0).map((_, i) => (i % 10) / 10);
        const distance = cosineDistance(a, b);
        expect(distance).toBeCloseTo(0, 10);
      });
    });

    describe('distance range', () => {
      it('should return values in range [0, 2]', () => {
        // Test many random vector pairs
        for (let i = 0; i < 100; i++) {
          const a = [Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5];
          const b = [Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5];
          const distance = cosineDistance(a, b);
          expect(distance).toBeGreaterThanOrEqual(0);
          expect(distance).toBeLessThanOrEqual(2);
        }
      });
    });

    describe('symmetry', () => {
      it('should be symmetric: d(a,b) = d(b,a)', () => {
        const a = [1, 2, 3, 4];
        const b = [5, 6, 7, 8];
        expect(cosineDistance(a, b)).toBeCloseTo(cosineDistance(b, a), 10);
      });
    });
  });

  // ============================================================================
  // EUCLIDEAN DISTANCE TESTS
  // ============================================================================

  describe('euclideanDistance', () => {
    describe('basic functionality', () => {
      it('should return 0 for identical vectors', () => {
        const a = [1, 2, 3, 4, 5];
        const distance = euclideanDistance(a, a);
        expect(distance).toBe(0);
      });

      it('should calculate correct distance for 2D vectors', () => {
        const a = [0, 0];
        const b = [3, 4];
        const distance = euclideanDistance(a, b);
        expect(distance).toBe(5); // 3-4-5 triangle
      });

      it('should calculate correct distance for 3D vectors', () => {
        const a = [0, 0, 0];
        const b = [1, 2, 2];
        const distance = euclideanDistance(a, b);
        expect(distance).toBe(3); // sqrt(1 + 4 + 4) = 3
      });

      it('should handle negative values', () => {
        const a = [-1, -2, -3];
        const b = [1, 2, 3];
        const distance = euclideanDistance(a, b);
        // sqrt((2)^2 + (4)^2 + (6)^2) = sqrt(4 + 16 + 36) = sqrt(56)
        expect(distance).toBeCloseTo(Math.sqrt(56), 10);
      });
    });

    describe('edge cases', () => {
      it('should throw for mismatched dimensions', () => {
        expect(() => euclideanDistance([1], [1, 2])).toThrow(
          'Vector dimension mismatch: 1 vs 2'
        );
      });

      it('should handle zero vectors', () => {
        const distance = euclideanDistance([0, 0, 0], [0, 0, 0]);
        expect(distance).toBe(0);
      });

      it('should handle single-dimension vectors', () => {
        expect(euclideanDistance([0], [5])).toBe(5);
        expect(euclideanDistance([-3], [3])).toBe(6);
      });

      it('should handle very small differences', () => {
        const a = [1e-10, 1e-10];
        const b = [2e-10, 2e-10];
        const distance = euclideanDistance(a, b);
        expect(distance).toBeCloseTo(Math.sqrt(2) * 1e-10, 15);
      });

      it('should handle very large values', () => {
        const a = [1e10, 0];
        const b = [0, 1e10];
        const distance = euclideanDistance(a, b);
        expect(distance).toBeCloseTo(Math.sqrt(2) * 1e10, -5);
      });
    });

    describe('high-dimensional vectors', () => {
      it('should work with 128-dimensional vectors', () => {
        const dim = 128;
        const a = Array(dim).fill(0);
        const b = Array(dim).fill(1);
        const distance = euclideanDistance(a, b);
        expect(distance).toBeCloseTo(Math.sqrt(dim), 10);
      });

      it('should work with 384-dimensional vectors', () => {
        const dim = 384;
        const a = Array(dim).fill(0);
        const b = Array(dim).fill(0);
        b[0] = 1; // Only one dimension differs
        const distance = euclideanDistance(a, b);
        expect(distance).toBe(1);
      });
    });

    describe('distance properties', () => {
      it('should always return non-negative values', () => {
        for (let i = 0; i < 100; i++) {
          const a = [Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5];
          const b = [Math.random() - 0.5, Math.random() - 0.5, Math.random() - 0.5];
          const distance = euclideanDistance(a, b);
          expect(distance).toBeGreaterThanOrEqual(0);
        }
      });

      it('should be symmetric: d(a,b) = d(b,a)', () => {
        const a = [1, 2, 3];
        const b = [4, 5, 6];
        expect(euclideanDistance(a, b)).toBe(euclideanDistance(b, a));
      });

      it('should satisfy triangle inequality: d(a,c) <= d(a,b) + d(b,c)', () => {
        const a = [0, 0];
        const b = [3, 0];
        const c = [3, 4];

        const dAB = euclideanDistance(a, b);
        const dBC = euclideanDistance(b, c);
        const dAC = euclideanDistance(a, c);

        expect(dAC).toBeLessThanOrEqual(dAB + dBC + 1e-10); // small epsilon for floating point
      });
    });
  });

  // ============================================================================
  // INNER PRODUCT DISTANCE TESTS
  // ============================================================================

  describe('innerProduct', () => {
    describe('basic functionality', () => {
      it('should return 0 for unit vectors pointing same direction', () => {
        const a = [1, 0, 0];
        // For unit vectors, inner product = 1, so distance = 1 - 1 = 0
        const distance = innerProduct(a, a);
        expect(distance).toBeCloseTo(0, 10);
      });

      it('should calculate correct distance', () => {
        const a = [1, 2, 3];
        const b = [1, 0, 0];
        // dot product = 1*1 + 2*0 + 3*0 = 1
        // distance = 1 - 1 = 0
        const distance = innerProduct(a, b);
        expect(distance).toBe(0);
      });

      it('should return 1 for orthogonal unit vectors', () => {
        const a = [1, 0];
        const b = [0, 1];
        // dot product = 0, distance = 1 - 0 = 1
        const distance = innerProduct(a, b);
        expect(distance).toBe(1);
      });

      it('should return 2 for opposite unit vectors', () => {
        const a = [1, 0, 0];
        const b = [-1, 0, 0];
        // dot product = -1, distance = 1 - (-1) = 2
        const distance = innerProduct(a, b);
        expect(distance).toBe(2);
      });
    });

    describe('edge cases', () => {
      it('should throw for mismatched dimensions', () => {
        expect(() => innerProduct([1, 2], [1, 2, 3])).toThrow(
          'Vector dimension mismatch: 2 vs 3'
        );
      });

      it('should handle zero vectors', () => {
        const distance = innerProduct([0, 0, 0], [1, 2, 3]);
        // dot product = 0, distance = 1 - 0 = 1
        expect(distance).toBe(1);
      });

      it('should handle both zero vectors', () => {
        const distance = innerProduct([0, 0, 0], [0, 0, 0]);
        expect(distance).toBe(1);
      });

      it('should handle single-dimension vectors', () => {
        expect(innerProduct([1], [1])).toBe(0);
        expect(innerProduct([2], [3])).toBe(1 - 6); // = -5
      });
    });

    describe('magnitude sensitivity', () => {
      it('should be sensitive to magnitude (unlike cosine)', () => {
        const a = [1, 0];
        const b1 = [1, 0];
        const b2 = [2, 0]; // same direction, double magnitude

        const dist1 = innerProduct(a, b1);
        const dist2 = innerProduct(a, b2);

        // Inner product is magnitude-sensitive
        expect(dist1).not.toBe(dist2);
      });

      it('should give lower distance for larger aligned vectors', () => {
        const a = [1, 1];
        const small = [1, 1];
        const large = [10, 10];

        const distSmall = innerProduct(a, small);
        const distLarge = innerProduct(a, large);

        // Larger inner product = lower distance
        expect(distLarge).toBeLessThan(distSmall);
      });
    });

    describe('symmetry', () => {
      it('should be symmetric: d(a,b) = d(b,a)', () => {
        const a = [1, 2, 3];
        const b = [4, 5, 6];
        expect(innerProduct(a, b)).toBe(innerProduct(b, a));
      });
    });

    describe('high-dimensional vectors', () => {
      it('should work with high-dimensional vectors', () => {
        const dim = 384;
        const a = Array(dim).fill(1 / Math.sqrt(dim)); // unit vector
        const b = Array(dim).fill(1 / Math.sqrt(dim)); // unit vector

        const distance = innerProduct(a, b);
        // dot product of two identical unit vectors = 1
        // distance = 1 - 1 = 0
        expect(distance).toBeCloseTo(0, 5);
      });
    });
  });

  // ============================================================================
  // COMPARISON TESTS
  // ============================================================================

  describe('Distance function comparison', () => {
    it('cosine and inner product should be equivalent for normalized vectors', () => {
      // Normalize a vector
      const v = [3, 4];
      const norm = Math.sqrt(v[0] * v[0] + v[1] * v[1]);
      const a = [v[0] / norm, v[1] / norm];

      const w = [5, 12];
      const normW = Math.sqrt(w[0] * w[0] + w[1] * w[1]);
      const b = [w[0] / normW, w[1] / normW];

      const cosineDist = cosineDistance(a, b);
      const innerDist = innerProduct(a, b);

      // For normalized vectors, they should be the same
      expect(cosineDist).toBeCloseTo(innerDist, 10);
    });

    it('all functions should agree on identical vectors having zero/minimal distance', () => {
      const v = [1, 2, 3, 4, 5];

      expect(cosineDistance(v, v)).toBeCloseTo(0, 10);
      expect(euclideanDistance(v, v)).toBe(0);
      expect(innerProduct(v, v)).toBeLessThan(0); // dot product > 1, so distance < 0
    });

    it('all functions should handle high-dimensional embeddings consistently', () => {
      const dim = 768;
      const a = Array(dim).fill(0).map((_, i) => Math.sin(i * 0.1));
      const b = Array(dim).fill(0).map((_, i) => Math.sin(i * 0.1 + 0.01)); // slightly shifted

      // All should indicate similarity (not identical, but close)
      const cosineDist = cosineDistance(a, b);
      const euclidDist = euclideanDistance(a, b);

      expect(cosineDist).toBeGreaterThan(0);
      expect(cosineDist).toBeLessThan(0.1); // should be similar
      expect(euclidDist).toBeGreaterThan(0);
    });
  });
});
