/**
 * GraphDB Query Plan Cache Tests
 *
 * Tests for caching query plans to avoid repeated planning overhead.
 * Same queries should reuse cached plans; schema changes should invalidate cache.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { createPlanCache, type PlanCache } from '../../src/query/plan-cache';
import { planQuery, type QueryPlan } from '../../src/query/planner';
import { parse } from '../../src/query/parser';

// ============================================================================
// Plan Cache Tests
// ============================================================================

describe('PlanCache', () => {
  let cache: PlanCache;

  beforeEach(() => {
    cache = createPlanCache(100);
  });

  describe('Should cache query plans', () => {
    it('should store a query plan in the cache', () => {
      const query = 'user:123.friends';
      const ast = parse(query);
      const plan = planQuery(ast);

      cache.set(query, plan);

      const cached = cache.get(query);
      expect(cached).toBeDefined();
      expect(cached).toEqual(plan);
    });

    it('should return undefined for non-cached queries', () => {
      const result = cache.get('nonexistent:query');
      expect(result).toBeUndefined();
    });
  });

  describe('Should return cached plan for same query', () => {
    it('should return the same plan object for identical queries', () => {
      const query = 'user:123.friends.posts';
      const ast = parse(query);
      const plan = planQuery(ast);

      cache.set(query, plan);

      // Multiple gets should return the same cached plan
      const cached1 = cache.get(query);
      const cached2 = cache.get(query);

      expect(cached1).toBe(cached2);
      expect(cached1).toEqual(plan);
    });

    it('should cache different queries separately', () => {
      const query1 = 'user:123.friends';
      const query2 = 'user:456.posts';

      const plan1 = planQuery(parse(query1));
      const plan2 = planQuery(parse(query2));

      cache.set(query1, plan1);
      cache.set(query2, plan2);

      expect(cache.get(query1)).toEqual(plan1);
      expect(cache.get(query2)).toEqual(plan2);
      expect(cache.get(query1)).not.toEqual(cache.get(query2));
    });

    it('should handle queries with filters', () => {
      const query = 'user:123.friends[?age > 30]';
      const plan = planQuery(parse(query));

      cache.set(query, plan);

      expect(cache.get(query)).toEqual(plan);
    });

    it('should handle queries with expansions', () => {
      const query = 'user:123 { name, email }';
      const plan = planQuery(parse(query));

      cache.set(query, plan);

      expect(cache.get(query)).toEqual(plan);
    });
  });

  describe('Should invalidate cache after schema change', () => {
    it('should clear all cached plans on invalidate()', () => {
      const query1 = 'user:123.friends';
      const query2 = 'post:456.comments';

      cache.set(query1, planQuery(parse(query1)));
      cache.set(query2, planQuery(parse(query2)));

      // Verify they are cached
      expect(cache.get(query1)).toBeDefined();
      expect(cache.get(query2)).toBeDefined();

      // Invalidate all
      cache.invalidate();

      // Should be cleared
      expect(cache.get(query1)).toBeUndefined();
      expect(cache.get(query2)).toBeUndefined();
    });

    it('should allow new entries after invalidation', () => {
      const query = 'user:123.friends';
      const plan = planQuery(parse(query));

      cache.set(query, plan);
      cache.invalidate();

      // Should be able to cache again
      cache.set(query, plan);
      expect(cache.get(query)).toEqual(plan);
    });
  });

  describe('Should limit cache size (LRU eviction)', () => {
    it('should evict least recently used entries when cache is full', () => {
      // Create a small cache for testing
      const smallCache = createPlanCache(3);

      const queries = [
        'user:1.friends',
        'user:2.friends',
        'user:3.friends',
        'user:4.friends', // This should evict user:1.friends
      ];

      // Add first 3 queries
      for (let i = 0; i < 3; i++) {
        smallCache.set(queries[i], planQuery(parse(queries[i])));
      }

      // All 3 should be cached
      expect(smallCache.get(queries[0])).toBeDefined();
      expect(smallCache.get(queries[1])).toBeDefined();
      expect(smallCache.get(queries[2])).toBeDefined();

      // Add 4th query - should evict least recently used (query[0])
      smallCache.set(queries[3], planQuery(parse(queries[3])));

      // First query should be evicted
      expect(smallCache.get(queries[0])).toBeUndefined();
      // Others should remain
      expect(smallCache.get(queries[1])).toBeDefined();
      expect(smallCache.get(queries[2])).toBeDefined();
      expect(smallCache.get(queries[3])).toBeDefined();
    });

    it('should update LRU order on get()', () => {
      const smallCache = createPlanCache(3);

      const queries = [
        'user:1.friends',
        'user:2.friends',
        'user:3.friends',
      ];

      // Add all 3
      for (const q of queries) {
        smallCache.set(q, planQuery(parse(q)));
      }

      // Access query[0] to make it recently used
      smallCache.get(queries[0]);

      // Add new query - should evict query[1] (now least recently used)
      const newQuery = 'user:4.friends';
      smallCache.set(newQuery, planQuery(parse(newQuery)));

      // query[0] should still be cached (was accessed)
      expect(smallCache.get(queries[0])).toBeDefined();
      // query[1] should be evicted (least recently used)
      expect(smallCache.get(queries[1])).toBeUndefined();
      // query[2] should remain
      expect(smallCache.get(queries[2])).toBeDefined();
      // new query should be cached
      expect(smallCache.get(newQuery)).toBeDefined();
    });

    it('should handle maxSize of 1', () => {
      const tinyCache = createPlanCache(1);

      const query1 = 'user:1.friends';
      const query2 = 'user:2.friends';

      tinyCache.set(query1, planQuery(parse(query1)));
      expect(tinyCache.get(query1)).toBeDefined();

      tinyCache.set(query2, planQuery(parse(query2)));
      expect(tinyCache.get(query1)).toBeUndefined();
      expect(tinyCache.get(query2)).toBeDefined();
    });

    it('should not evict when updating existing entry', () => {
      const smallCache = createPlanCache(2);

      const query1 = 'user:1.friends';
      const query2 = 'user:2.friends';

      smallCache.set(query1, planQuery(parse(query1)));
      smallCache.set(query2, planQuery(parse(query2)));

      // Update query1 with new plan (should not trigger eviction)
      const newPlan = planQuery(parse(query1));
      smallCache.set(query1, newPlan);

      // Both should still be cached
      expect(smallCache.get(query1)).toBeDefined();
      expect(smallCache.get(query2)).toBeDefined();
    });
  });

  describe('Edge cases', () => {
    it('should handle empty query strings', () => {
      // Note: empty strings may not be valid queries, but cache should handle them gracefully
      // This tests the cache layer, not query validity
      const plan = planQuery(parse('user:123'));
      cache.set('', plan);
      expect(cache.get('')).toEqual(plan);
    });

    it('should handle very long query strings', () => {
      // Long query with many hops
      const query = 'user:123' + '.friends'.repeat(50);
      // This may not be a valid query, but we test cache behavior
      try {
        const ast = parse(query);
        const plan = planQuery(ast);
        cache.set(query, plan);
        expect(cache.get(query)).toEqual(plan);
      } catch {
        // If parsing fails, that's ok - we're testing cache, not parser
        // Just test that cache handles arbitrary strings
        const dummyPlan = planQuery(parse('user:123'));
        cache.set(query, dummyPlan);
        expect(cache.get(query)).toEqual(dummyPlan);
      }
    });

    it('should return cache size', () => {
      expect(cache.size()).toBe(0);

      cache.set('user:1.friends', planQuery(parse('user:1.friends')));
      expect(cache.size()).toBe(1);

      cache.set('user:2.friends', planQuery(parse('user:2.friends')));
      expect(cache.size()).toBe(2);

      cache.invalidate();
      expect(cache.size()).toBe(0);
    });
  });
});

// ============================================================================
// Integration with Planner Tests
// ============================================================================

describe('PlanCache Integration', () => {
  it('should cache plans produced by planQuery', () => {
    const cache = createPlanCache(100);
    const query = 'user:123.friends.posts[?likes > 10]';

    const ast = parse(query);
    const plan = planQuery(ast);

    cache.set(query, plan);

    const cached = cache.get(query);
    expect(cached).toBeDefined();
    expect(cached?.steps).toEqual(plan.steps);
    expect(cached?.shards).toEqual(plan.shards);
    expect(cached?.estimatedCost).toBe(plan.estimatedCost);
  });

  it('should preserve plan structure through cache', () => {
    const cache = createPlanCache(100);
    const query = 'user:123.friends';

    const plan = planQuery(parse(query));
    cache.set(query, plan);

    const cached = cache.get(query)!;

    // Verify plan structure is preserved
    expect(cached.steps.length).toBe(plan.steps.length);
    expect(cached.steps[0].type).toBe('lookup');
    expect(cached.steps[1].type).toBe('traverse');
    expect(cached.steps[1].predicate).toBe('friends');
  });
});

// ============================================================================
// CachedPlanner Tests
// ============================================================================

import { createCachedPlanner, type CachedPlanner } from '../../src/query/planner';

describe('CachedPlanner', () => {
  let planner: CachedPlanner;

  beforeEach(() => {
    planner = createCachedPlanner({ maxSize: 100 });
  });

  describe('plan()', () => {
    it('should plan and cache queries', () => {
      const query = 'user:123.friends';

      const plan1 = planner.plan(query);
      expect(plan1).toBeDefined();
      expect(plan1.steps.length).toBeGreaterThan(0);

      // Second call should return cached plan
      const plan2 = planner.plan(query);
      expect(plan2).toBe(plan1); // Same reference (cached)
    });

    it('should return different plans for different queries', () => {
      const query1 = 'user:123.friends';
      const query2 = 'user:456.posts';

      const plan1 = planner.plan(query1);
      const plan2 = planner.plan(query2);

      expect(plan1).not.toBe(plan2);
      expect(plan1.steps[0].entityIds).toContain('user:123');
      expect(plan2.steps[0].entityIds).toContain('user:456');
    });

    it('should handle complex queries with filters', () => {
      const query = 'user:123.friends[?age > 30].posts';
      const plan = planner.plan(query);

      expect(plan.steps.some((s) => s.type === 'filter')).toBe(true);
    });
  });

  describe('planFromAst()', () => {
    it('should plan from AST and cache', () => {
      const query = 'user:123.friends';
      const ast = parse(query);

      const plan1 = planner.planFromAst(ast);
      expect(plan1).toBeDefined();

      // Same AST should return cached plan
      const plan2 = planner.planFromAst(ast);
      expect(plan2).toBe(plan1);
    });
  });

  describe('invalidateCache()', () => {
    it('should clear cached plans', () => {
      const query = 'user:123.friends';

      const plan1 = planner.plan(query);
      expect(planner.getCache().size()).toBe(1);

      planner.invalidateCache();
      expect(planner.getCache().size()).toBe(0);

      // Should replan after invalidation
      const plan2 = planner.plan(query);
      expect(plan2).not.toBe(plan1); // Different reference (replanned)
      expect(plan2).toEqual(plan1); // But same content
    });
  });

  describe('getCache()', () => {
    it('should return the underlying cache', () => {
      const cache = planner.getCache();
      expect(cache).toBeDefined();
      expect(typeof cache.get).toBe('function');
      expect(typeof cache.set).toBe('function');
      expect(typeof cache.invalidate).toBe('function');
      expect(typeof cache.size).toBe('function');
    });
  });

  describe('LRU eviction in CachedPlanner', () => {
    it('should evict old plans when cache is full', () => {
      const smallPlanner = createCachedPlanner({ maxSize: 2 });

      smallPlanner.plan('user:1.friends');
      smallPlanner.plan('user:2.friends');

      expect(smallPlanner.getCache().size()).toBe(2);

      // Add third query - should evict first
      smallPlanner.plan('user:3.friends');

      expect(smallPlanner.getCache().size()).toBe(2);
      // First query should be evicted, but we can't check directly without accessing internals
      // Instead verify the cache behavior works
      expect(smallPlanner.getCache().get('user:2.friends')).toBeDefined();
      expect(smallPlanner.getCache().get('user:3.friends')).toBeDefined();
    });
  });
});
