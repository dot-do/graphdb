/**
 * Combined/Hybrid Search Tests (TDD RED Phase)
 *
 * Tests for hybrid search combining multiple index types:
 * - FTS + Vector search (semantic + keyword)
 * - FTS + Geo search (text + location)
 * - Vector + Geo search (semantic + location)
 * - All three combined
 *
 * @see src/index/combined-index.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  combineScores,
  normalizeScore,
  hybridSearch,
  reciprocalRankFusion,
  type HybridSearchOptions,
  type SearchResult,
  type IndexSearcher,
} from '../../src/index/hybrid-search.js';

// ============================================================================
// TEST FIXTURES
// ============================================================================

/**
 * Create mock FTS search results
 */
function createMockFTSResults(): SearchResult[] {
  return [
    { entityId: 'doc-1', score: 2.5, source: 'fts' },
    { entityId: 'doc-2', score: 2.0, source: 'fts' },
    { entityId: 'doc-3', score: 1.5, source: 'fts' },
    { entityId: 'doc-4', score: 1.0, source: 'fts' },
    { entityId: 'doc-5', score: 0.5, source: 'fts' },
  ];
}

/**
 * Create mock vector search results
 */
function createMockVectorResults(): SearchResult[] {
  return [
    { entityId: 'doc-1', score: 0.95, source: 'vector' },
    { entityId: 'doc-6', score: 0.90, source: 'vector' },
    { entityId: 'doc-3', score: 0.85, source: 'vector' },
    { entityId: 'doc-7', score: 0.80, source: 'vector' },
    { entityId: 'doc-2', score: 0.75, source: 'vector' },
  ];
}

/**
 * Create mock geo search results
 */
function createMockGeoResults(): SearchResult[] {
  return [
    { entityId: 'doc-1', score: 0.99, source: 'geo' },  // 0.1 km away
    { entityId: 'doc-8', score: 0.98, source: 'geo' },  // 0.2 km away
    { entityId: 'doc-3', score: 0.95, source: 'geo' },  // 0.5 km away
    { entityId: 'doc-9', score: 0.90, source: 'geo' },  // 1 km away
    { entityId: 'doc-2', score: 0.85, source: 'geo' },  // 1.5 km away
  ];
}

/**
 * Create a mock index searcher
 */
function createMockSearcher(results: SearchResult[]): IndexSearcher {
  return {
    search: async () => results,
    name: 'mock',
  };
}

// ============================================================================
// SCORE NORMALIZATION TESTS
// ============================================================================

describe('Score Normalization', () => {
  describe('normalizeScore', () => {
    it('should normalize FTS BM25 scores to [0, 1]', () => {
      const scores = [2.5, 2.0, 1.5, 1.0, 0.5];
      const normalized = scores.map(s => normalizeScore(s, 'bm25'));

      for (const n of normalized) {
        expect(n).toBeGreaterThanOrEqual(0);
        expect(n).toBeLessThanOrEqual(1);
      }

      // Higher BM25 should be higher normalized
      expect(normalized[0]).toBeGreaterThan(normalized[1]!);
    });

    it('should normalize vector cosine similarity scores (already [0, 1])', () => {
      const scores = [0.95, 0.85, 0.75, 0.65, 0.55];
      const normalized = scores.map(s => normalizeScore(s, 'cosine'));

      // Should remain in [0, 1]
      for (const n of normalized) {
        expect(n).toBeGreaterThanOrEqual(0);
        expect(n).toBeLessThanOrEqual(1);
      }
    });

    it('should normalize geo distance scores (invert distance)', () => {
      // Lower distance = higher score
      const distancesKm = [0.1, 0.5, 1.0, 2.0, 5.0];
      const normalized = distancesKm.map(d => normalizeScore(d, 'geo_distance'));

      // Closer should have higher score
      expect(normalized[0]).toBeGreaterThan(normalized[1]!);
      expect(normalized[1]).toBeGreaterThan(normalized[2]!);
    });

    it('should handle zero scores', () => {
      expect(normalizeScore(0, 'bm25')).toBe(0);
      expect(normalizeScore(0, 'cosine')).toBe(0);
      expect(normalizeScore(0, 'geo_distance')).toBe(1); // 0 distance = max score
    });

    it('should handle negative scores gracefully', () => {
      // Some scoring functions can return negative (rare)
      const normalized = normalizeScore(-0.5, 'bm25');
      expect(normalized).toBeGreaterThanOrEqual(0);
    });
  });
});

// ============================================================================
// SCORE COMBINATION TESTS
// ============================================================================

describe('Score Combination', () => {
  describe('combineScores', () => {
    it('should combine scores with equal weights', () => {
      const scores = { fts: 0.8, vector: 0.6, geo: 0.9 };
      const weights = { fts: 1, vector: 1, geo: 1 };

      const combined = combineScores(scores, weights);

      // Average of 0.8, 0.6, 0.9 = 0.767
      expect(combined).toBeCloseTo(0.767, 2);
    });

    it('should combine scores with custom weights', () => {
      const scores = { fts: 0.5, vector: 1.0 };
      const weights = { fts: 0.3, vector: 0.7 };

      const combined = combineScores(scores, weights);

      // 0.5 * 0.3 + 1.0 * 0.7 = 0.15 + 0.7 = 0.85
      expect(combined).toBeCloseTo(0.85, 5);
    });

    it('should handle missing scores', () => {
      const scores = { fts: 0.8 };
      const weights = { fts: 0.5, vector: 0.5 };

      const combined = combineScores(scores, weights);

      // Only FTS contributes, adjusted for missing vector
      expect(combined).toBeGreaterThanOrEqual(0);
      expect(combined).toBeLessThanOrEqual(1);
    });

    it('should handle all zeros', () => {
      const scores = { fts: 0, vector: 0 };
      const weights = { fts: 0.5, vector: 0.5 };

      const combined = combineScores(scores, weights);
      expect(combined).toBe(0);
    });

    it('should handle single source', () => {
      const scores = { fts: 0.75 };
      const weights = { fts: 1 };

      const combined = combineScores(scores, weights);
      expect(combined).toBe(0.75);
    });
  });
});

// ============================================================================
// RECIPROCAL RANK FUSION TESTS
// ============================================================================

describe('Reciprocal Rank Fusion', () => {
  it('should combine rankings from multiple result lists', () => {
    const ftsResults = createMockFTSResults();
    const vectorResults = createMockVectorResults();

    const combined = reciprocalRankFusion([ftsResults, vectorResults], 60);

    expect(combined.length).toBeGreaterThan(0);
    // doc-1 appears in both lists at rank 1, should be at top
    expect(combined[0]!.entityId).toBe('doc-1');
  });

  it('should boost entities appearing in multiple result lists', () => {
    const list1: SearchResult[] = [
      { entityId: 'a', score: 1, source: 'fts' },
      { entityId: 'b', score: 0.9, source: 'fts' },
      { entityId: 'c', score: 0.8, source: 'fts' },
    ];
    const list2: SearchResult[] = [
      { entityId: 'a', score: 1, source: 'vector' },
      { entityId: 'd', score: 0.9, source: 'vector' },
      { entityId: 'c', score: 0.8, source: 'vector' },
    ];

    const combined = reciprocalRankFusion([list1, list2], 60);

    // 'a' appears at rank 1 in both lists
    // 'c' appears at rank 3 in both lists
    // Both should score higher than 'b' or 'd' which only appear once
    const scores = new Map(combined.map(r => [r.entityId, r.score]));

    expect(scores.get('a')).toBeGreaterThan(scores.get('b')!);
    expect(scores.get('a')).toBeGreaterThan(scores.get('d')!);
    expect(scores.get('c')).toBeGreaterThan(scores.get('b')!);
    expect(scores.get('c')).toBeGreaterThan(scores.get('d')!);
  });

  it('should return results sorted by RRF score', () => {
    const combined = reciprocalRankFusion([
      createMockFTSResults(),
      createMockVectorResults(),
    ], 60);

    for (let i = 1; i < combined.length; i++) {
      expect(combined[i]!.score).toBeLessThanOrEqual(combined[i - 1]!.score);
    }
  });

  it('should handle empty result lists', () => {
    const combined = reciprocalRankFusion([[], []], 60);
    expect(combined).toEqual([]);
  });

  it('should handle single result list', () => {
    const results = createMockFTSResults();
    const combined = reciprocalRankFusion([results], 60);

    expect(combined.length).toBe(results.length);
    // Order should be preserved
    expect(combined[0]!.entityId).toBe(results[0]!.entityId);
  });

  it('should respect k parameter', () => {
    const list1: SearchResult[] = Array.from({ length: 100 }, (_, i) => ({
      entityId: `doc-${i}`,
      score: 1 - i * 0.01,
      source: 'fts',
    }));

    // Higher k = more emphasis on rank differences
    const rrfLowK = reciprocalRankFusion([list1], 1);
    const rrfHighK = reciprocalRankFusion([list1], 100);

    // Scores should differ based on k
    expect(rrfLowK[0]!.score).not.toBe(rrfHighK[0]!.score);
  });
});

// ============================================================================
// HYBRID SEARCH TESTS
// ============================================================================

describe('Hybrid Search', () => {
  describe('FTS + Vector (Semantic + Keyword)', () => {
    it('should combine FTS and vector search results', async () => {
      const ftsSearcher = createMockSearcher(createMockFTSResults());
      const vectorSearcher = createMockSearcher(createMockVectorResults());

      const options: HybridSearchOptions = {
        searchers: [
          { searcher: ftsSearcher, weight: 0.5 },
          { searcher: vectorSearcher, weight: 0.5 },
        ],
        limit: 10,
      };

      const results = await hybridSearch(options);

      expect(results.length).toBeLessThanOrEqual(10);
      // doc-1 appears in both, should rank high
      expect(results.slice(0, 3).some(r => r.entityId === 'doc-1')).toBe(true);
    });

    it('should respect weight preferences', async () => {
      const ftsSearcher = createMockSearcher([
        { entityId: 'fts-only', score: 2.5, source: 'fts' },
      ]);
      const vectorSearcher = createMockSearcher([
        { entityId: 'vec-only', score: 0.95, source: 'vector' },
      ]);

      // Heavily weight FTS
      const ftsWeightedResults = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.9 },
          { searcher: vectorSearcher, weight: 0.1 },
        ],
        limit: 10,
      });

      // Heavily weight vector
      const vecWeightedResults = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.1 },
          { searcher: vectorSearcher, weight: 0.9 },
        ],
        limit: 10,
      });

      // FTS-weighted should favor fts-only
      const ftsWeightedTop = ftsWeightedResults[0]!.entityId;
      const vecWeightedTop = vecWeightedResults[0]!.entityId;

      expect(ftsWeightedTop).toBe('fts-only');
      expect(vecWeightedTop).toBe('vec-only');
    });

    it('should handle one searcher returning empty results', async () => {
      const ftsSearcher = createMockSearcher(createMockFTSResults());
      const emptySearcher = createMockSearcher([]);

      const results = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.5 },
          { searcher: emptySearcher, weight: 0.5 },
        ],
        limit: 10,
      });

      // Should still return FTS results
      expect(results.length).toBeGreaterThan(0);
    });
  });

  describe('FTS + Geo (Text + Location)', () => {
    it('should combine FTS and geo search results', async () => {
      const ftsSearcher = createMockSearcher(createMockFTSResults());
      const geoSearcher = createMockSearcher(createMockGeoResults());

      const results = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.6 },
          { searcher: geoSearcher, weight: 0.4 },
        ],
        limit: 10,
      });

      expect(results.length).toBeGreaterThan(0);
      // doc-1 appears in both with high scores
      expect(results.slice(0, 3).some(r => r.entityId === 'doc-1')).toBe(true);
    });

    it('should find locally relevant results', async () => {
      // FTS returns doc-1, doc-2, doc-3
      // Geo returns doc-8, doc-9, doc-1 (doc-1 is nearby)
      const ftsSearcher = createMockSearcher([
        { entityId: 'doc-1', score: 2.0, source: 'fts' },
        { entityId: 'doc-2', score: 1.8, source: 'fts' },
        { entityId: 'doc-3', score: 1.5, source: 'fts' },
      ]);
      const geoSearcher = createMockSearcher([
        { entityId: 'doc-8', score: 0.99, source: 'geo' },
        { entityId: 'doc-1', score: 0.95, source: 'geo' },
        { entityId: 'doc-9', score: 0.90, source: 'geo' },
      ]);

      const results = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.5 },
          { searcher: geoSearcher, weight: 0.5 },
        ],
        limit: 5,
      });

      // doc-1 should rank high (matches text AND is nearby)
      const doc1Rank = results.findIndex(r => r.entityId === 'doc-1');
      expect(doc1Rank).toBeLessThan(3);
    });
  });

  describe('Vector + Geo (Semantic + Location)', () => {
    it('should combine vector and geo search results', async () => {
      const vectorSearcher = createMockSearcher(createMockVectorResults());
      const geoSearcher = createMockSearcher(createMockGeoResults());

      const results = await hybridSearch({
        searchers: [
          { searcher: vectorSearcher, weight: 0.7 },
          { searcher: geoSearcher, weight: 0.3 },
        ],
        limit: 10,
      });

      expect(results.length).toBeGreaterThan(0);
    });

    it('should find semantically similar nearby results', async () => {
      const vectorSearcher = createMockSearcher([
        { entityId: 'doc-1', score: 0.95, source: 'vector' },
        { entityId: 'doc-10', score: 0.90, source: 'vector' },
      ]);
      const geoSearcher = createMockSearcher([
        { entityId: 'doc-10', score: 0.99, source: 'geo' },
        { entityId: 'doc-1', score: 0.80, source: 'geo' },
      ]);

      const results = await hybridSearch({
        searchers: [
          { searcher: vectorSearcher, weight: 0.5 },
          { searcher: geoSearcher, weight: 0.5 },
        ],
        limit: 5,
      });

      // Both doc-1 and doc-10 appear in both lists
      expect(results.length).toBe(2);
    });
  });

  describe('FTS + Vector + Geo (All Three)', () => {
    it('should combine all three search types', async () => {
      const ftsSearcher = createMockSearcher(createMockFTSResults());
      const vectorSearcher = createMockSearcher(createMockVectorResults());
      const geoSearcher = createMockSearcher(createMockGeoResults());

      const results = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.4 },
          { searcher: vectorSearcher, weight: 0.4 },
          { searcher: geoSearcher, weight: 0.2 },
        ],
        limit: 10,
      });

      expect(results.length).toBeGreaterThan(0);
      // doc-1 and doc-3 appear in all three lists
      const entityIds = results.map(r => r.entityId);
      expect(entityIds).toContain('doc-1');
    });

    it('should rank entities appearing in all three highest', async () => {
      // doc-1 appears in all three
      // doc-3 appears in all three
      // others appear in fewer lists
      const ftsSearcher = createMockSearcher([
        { entityId: 'doc-1', score: 2.0, source: 'fts' },
        { entityId: 'doc-3', score: 1.5, source: 'fts' },
        { entityId: 'doc-fts-only', score: 3.0, source: 'fts' }, // High FTS score but only in one
      ]);
      const vectorSearcher = createMockSearcher([
        { entityId: 'doc-1', score: 0.95, source: 'vector' },
        { entityId: 'doc-3', score: 0.85, source: 'vector' },
        { entityId: 'doc-vec-only', score: 0.99, source: 'vector' },
      ]);
      const geoSearcher = createMockSearcher([
        { entityId: 'doc-1', score: 0.99, source: 'geo' },
        { entityId: 'doc-3', score: 0.95, source: 'geo' },
        { entityId: 'doc-geo-only', score: 0.99, source: 'geo' },
      ]);

      const results = await hybridSearch({
        searchers: [
          { searcher: ftsSearcher, weight: 0.33 },
          { searcher: vectorSearcher, weight: 0.33 },
          { searcher: geoSearcher, weight: 0.34 },
        ],
        limit: 10,
        fusionMethod: 'rrf',
      });

      // doc-1 should be at the top (appears in all 3 with high ranks)
      expect(results[0]!.entityId).toBe('doc-1');
      // doc-3 should also be near the top
      const doc3Rank = results.findIndex(r => r.entityId === 'doc-3');
      expect(doc3Rank).toBeLessThan(3);
    });
  });

  describe('Edge Cases', () => {
    it('should handle all searchers returning empty', async () => {
      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher([]), weight: 0.5 },
          { searcher: createMockSearcher([]), weight: 0.5 },
        ],
        limit: 10,
      });

      expect(results).toEqual([]);
    });

    it('should handle limit of 1', async () => {
      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher(createMockFTSResults()), weight: 0.5 },
          { searcher: createMockSearcher(createMockVectorResults()), weight: 0.5 },
        ],
        limit: 1,
      });

      expect(results.length).toBe(1);
    });

    it('should handle very small weights', async () => {
      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher(createMockFTSResults()), weight: 0.001 },
          { searcher: createMockSearcher(createMockVectorResults()), weight: 0.999 },
        ],
        limit: 10,
      });

      expect(results.length).toBeGreaterThan(0);
    });

    it('should handle duplicate entity IDs gracefully', async () => {
      const duplicateResults: SearchResult[] = [
        { entityId: 'doc-1', score: 0.9, source: 'fts' },
        { entityId: 'doc-1', score: 0.8, source: 'fts' }, // Duplicate
      ];

      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher(duplicateResults), weight: 1 },
        ],
        limit: 10,
      });

      // Should deduplicate
      const doc1Count = results.filter(r => r.entityId === 'doc-1').length;
      expect(doc1Count).toBe(1);
    });
  });

  describe('Fusion Methods', () => {
    it('should support weighted average fusion', async () => {
      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher(createMockFTSResults()), weight: 0.5 },
          { searcher: createMockSearcher(createMockVectorResults()), weight: 0.5 },
        ],
        limit: 10,
        fusionMethod: 'weighted_average',
      });

      expect(results.length).toBeGreaterThan(0);
    });

    it('should support RRF fusion', async () => {
      const results = await hybridSearch({
        searchers: [
          { searcher: createMockSearcher(createMockFTSResults()), weight: 0.5 },
          { searcher: createMockSearcher(createMockVectorResults()), weight: 0.5 },
        ],
        limit: 10,
        fusionMethod: 'rrf',
      });

      expect(results.length).toBeGreaterThan(0);
    });

    it('should produce different results with different fusion methods', async () => {
      const searchers = [
        { searcher: createMockSearcher(createMockFTSResults()), weight: 0.5 },
        { searcher: createMockSearcher(createMockVectorResults()), weight: 0.5 },
      ];

      const weightedResults = await hybridSearch({
        searchers,
        limit: 10,
        fusionMethod: 'weighted_average',
      });

      const rrfResults = await hybridSearch({
        searchers,
        limit: 10,
        fusionMethod: 'rrf',
      });

      // Scores should be different (even if order might be similar)
      expect(weightedResults[0]!.score).not.toBe(rrfResults[0]!.score);
    });
  });
});
