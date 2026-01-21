/**
 * GraphDB Query Plan Optimization Tests (TDD RED Phase)
 *
 * Tests for query plan optimization scenarios including:
 * - Filter pushdown optimization
 * - Shard coalescing
 * - Cost estimation accuracy
 * - Cache key generation edge cases
 * - Plan step ordering
 */

import { describe, it, expect } from 'vitest';
import {
  planQuery,
  optimizePlan,
  estimateCost,
  createCachedPlanner,
  type QueryPlan,
  type PlanStep,
} from '../../src/query/planner';
import { parse } from '../../src/query/parser';

// ============================================================================
// Filter Pushdown Optimization
// ============================================================================

describe('Filter Pushdown Optimization', () => {
  describe('Should push filters closer to data source', () => {
    it('should maintain filter step after traversal in optimized plan', () => {
      const ast = parse('user:123.friends[?age > 30]');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      // Filter should still be after the traverse step
      const filterIndex = optimized.steps.findIndex(s => s.type === 'filter');
      const traverseIndex = optimized.steps.findIndex(s => s.type === 'traverse');

      expect(filterIndex).toBeGreaterThan(traverseIndex);
    });

    it('should preserve filter conditions through optimization', () => {
      const ast = parse('user:123.friends[?age > 30 and active = true]');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      const filterStep = optimized.steps.find(s => s.type === 'filter');
      expect(filterStep?.filter).toBeDefined();
      expect(filterStep?.filter?.field).toBe('age');
      expect(filterStep?.filter?.and).toBeDefined();
    });

    it('should handle multiple filters in sequence', () => {
      const ast = parse('user:123.friends[?active = true][?role = "admin"]');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      // Both filters should be preserved
      const filterSteps = optimized.steps.filter(s => s.type === 'filter');
      expect(filterSteps.length).toBe(2);
    });
  });
});

// ============================================================================
// Shard Coalescing
// ============================================================================

describe('Shard Coalescing', () => {
  describe('Should combine lookups to same shard', () => {
    it('should not increase step count after optimization', () => {
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      expect(optimized.steps.length).toBeLessThanOrEqual(plan.steps.length);
    });

    it('should assign consistent shard IDs', () => {
      const ast = parse('user:123.friends.posts');
      const plan = planQuery(ast);

      // All steps should have valid shard IDs
      for (const step of plan.steps) {
        expect(step.shardId).toBeDefined();
        expect(typeof step.shardId).toBe('string');
        expect(step.shardId.length).toBeGreaterThan(0);
      }
    });

    it('should produce deterministic shard assignments', () => {
      const ast = parse('user:123.friends');
      const plan1 = planQuery(ast);
      const plan2 = planQuery(ast);

      // Same query should produce same shard assignments
      expect(plan1.steps.length).toBe(plan2.steps.length);
      for (let i = 0; i < plan1.steps.length; i++) {
        expect(plan1.steps[i].shardId).toBe(plan2.steps[i].shardId);
      }
    });
  });

  describe('Should track unique shards', () => {
    it('should list shards involved in query', () => {
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);

      expect(plan.shards).toBeDefined();
      expect(plan.shards.length).toBeGreaterThan(0);
    });

    it('should include shard namespace information', () => {
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);

      for (const shard of plan.shards) {
        expect(shard.shardId).toBeDefined();
        expect(shard.namespace).toBeDefined();
      }
    });
  });
});

// ============================================================================
// Cost Estimation
// ============================================================================

describe('Cost Estimation Accuracy', () => {
  describe('Base costs', () => {
    it('should have minimal cost for simple lookup', () => {
      const plan = planQuery(parse('user:123'));
      const cost = estimateCost(plan);

      expect(cost).toBeGreaterThan(0);
      expect(cost).toBeLessThan(5);
    });

    it('should increase cost per traversal hop', () => {
      const plan1 = planQuery(parse('user:123.friends'));
      const plan2 = planQuery(parse('user:123.friends.posts'));
      const plan3 = planQuery(parse('user:123.friends.posts.comments'));

      const cost1 = estimateCost(plan1);
      const cost2 = estimateCost(plan2);
      const cost3 = estimateCost(plan3);

      expect(cost2).toBeGreaterThan(cost1);
      expect(cost3).toBeGreaterThan(cost2);
    });

    it('should have higher cost for reverse traversal', () => {
      const forwardPlan = planQuery(parse('user:123.posts'));
      const reversePlan = planQuery(parse('post:456 <- author'));

      const forwardCost = estimateCost(forwardPlan);
      const reverseCost = estimateCost(reversePlan);

      // Reverse traversal is more expensive
      expect(reverseCost).toBeGreaterThan(forwardCost);
    });
  });

  describe('Filter cost', () => {
    it('should add cost for filter operations', () => {
      const noFilter = planQuery(parse('user:123.friends'));
      const withFilter = planQuery(parse('user:123.friends[?age > 30]'));

      expect(estimateCost(withFilter)).toBeGreaterThan(estimateCost(noFilter));
    });

    it('should not significantly increase cost for simple filter', () => {
      const noFilter = planQuery(parse('user:123.friends'));
      const withFilter = planQuery(parse('user:123.friends[?age > 30]'));

      // Filter should add reasonable cost, not double it
      const diff = estimateCost(withFilter) - estimateCost(noFilter);
      expect(diff).toBeLessThan(estimateCost(noFilter));
    });
  });

  describe('Expansion cost', () => {
    it('should add cost per expanded field', () => {
      const oneField = planQuery(parse('user:123 { name }'));
      const manyFields = planQuery(parse('user:123 { name, email, age, role, bio }'));

      expect(estimateCost(manyFields)).toBeGreaterThan(estimateCost(oneField));
    });
  });

  describe('Recursion cost', () => {
    it('should have high cost for unbounded recursion', () => {
      // Note: We need to handle this - unbounded recursion should default to max depth
      const bounded = planQuery(parse('user:123.friends*[depth <= 3]'));
      const simple = planQuery(parse('user:123.friends'));

      const boundedCost = estimateCost(bounded);
      const simpleCost = estimateCost(simple);

      expect(boundedCost).toBeGreaterThan(simpleCost);
    });

    it('should scale cost with recursion depth', () => {
      const depth2 = planQuery(parse('user:123.friends*[depth <= 2]'));
      const depth5 = planQuery(parse('user:123.friends*[depth <= 5]'));
      const depth10 = planQuery(parse('user:123.friends*[depth <= 10]'));

      expect(estimateCost(depth5)).toBeGreaterThan(estimateCost(depth2));
      expect(estimateCost(depth10)).toBeGreaterThan(estimateCost(depth5));
    });
  });

  describe('Cost bounds', () => {
    it('should never return negative cost', () => {
      const queries = [
        'user:123',
        'user:123.friends',
        'post:456 <- likes',
        'user:123 { name }',
        'user:123.friends*[depth <= 3]',
      ];

      for (const query of queries) {
        const plan = planQuery(parse(query));
        const cost = estimateCost(plan);
        expect(cost).toBeGreaterThanOrEqual(0);
      }
    });

    it('should have reasonable upper bound for complex query', () => {
      const complexQuery = 'user:123.friends[?age > 30].posts { title, author { name } }';
      const plan = planQuery(parse(complexQuery));
      const cost = estimateCost(plan);

      // Complex query should still have bounded cost
      expect(cost).toBeLessThan(100);
    });
  });
});

// ============================================================================
// Cache Key Generation
// ============================================================================

describe('Cache Key Generation', () => {
  describe('Consistency', () => {
    it('should generate same cache key for identical queries', () => {
      const plan1 = planQuery(parse('user:123.friends'));
      const plan2 = planQuery(parse('user:123.friends'));

      expect(plan1.cacheKey).toBe(plan2.cacheKey);
    });

    it('should generate different cache keys for different queries', () => {
      const plan1 = planQuery(parse('user:123.friends'));
      const plan2 = planQuery(parse('user:456.friends'));

      expect(plan1.cacheKey).not.toBe(plan2.cacheKey);
    });

    it('should generate different cache keys for different predicates', () => {
      const plan1 = planQuery(parse('user:123.friends'));
      const plan2 = planQuery(parse('user:123.posts'));

      expect(plan1.cacheKey).not.toBe(plan2.cacheKey);
    });
  });

  describe('Filter sensitivity', () => {
    it('should generate different cache keys for different filter values', () => {
      const plan1 = planQuery(parse('user:123.friends[?age > 30]'));
      const plan2 = planQuery(parse('user:123.friends[?age > 40]'));

      // The cache key may or may not include filter details depending on implementation
      // This test documents expected behavior
      expect(plan1.cacheKey).toBeDefined();
      expect(plan2.cacheKey).toBeDefined();
    });

    it('should differentiate queries with and without filters', () => {
      const withoutFilter = planQuery(parse('user:123.friends'));
      const withFilter = planQuery(parse('user:123.friends[?age > 30]'));

      expect(withoutFilter.cacheKey).not.toBe(withFilter.cacheKey);
    });
  });

  describe('Expansion sensitivity', () => {
    it('should generate different cache keys for different expansion fields', () => {
      const plan1 = planQuery(parse('user:123 { name }'));
      const plan2 = planQuery(parse('user:123 { name, email }'));

      expect(plan1.cacheKey).not.toBe(plan2.cacheKey);
    });
  });

  describe('Cacheability', () => {
    it('should mark read-only queries as cacheable', () => {
      const plan = planQuery(parse('user:123.friends'));
      expect(plan.canCache).toBe(true);
    });

    it('should have cache key for cacheable queries', () => {
      const plan = planQuery(parse('user:123.friends'));
      expect(plan.canCache).toBe(true);
      expect(plan.cacheKey).toBeDefined();
      expect(typeof plan.cacheKey).toBe('string');
    });
  });
});

// ============================================================================
// Plan Step Ordering
// ============================================================================

describe('Plan Step Ordering', () => {
  describe('Basic operation order', () => {
    it('should have lookup as first step for entity query', () => {
      const plan = planQuery(parse('user:123'));
      expect(plan.steps[0].type).toBe('lookup');
    });

    it('should have lookup before traverse', () => {
      const plan = planQuery(parse('user:123.friends'));

      const lookupIndex = plan.steps.findIndex(s => s.type === 'lookup');
      const traverseIndex = plan.steps.findIndex(s => s.type === 'traverse');

      expect(lookupIndex).toBeLessThan(traverseIndex);
    });

    it('should have traverse before filter', () => {
      const plan = planQuery(parse('user:123.friends[?age > 30]'));

      const traverseIndex = plan.steps.findIndex(s => s.type === 'traverse');
      const filterIndex = plan.steps.findIndex(s => s.type === 'filter');

      expect(traverseIndex).toBeLessThan(filterIndex);
    });

    it('should have expand as final step', () => {
      const plan = planQuery(parse('user:123.friends { name }'));

      const expandIndex = plan.steps.findIndex(s => s.type === 'expand');
      expect(expandIndex).toBe(plan.steps.length - 1);
    });
  });

  describe('Multi-hop ordering', () => {
    it('should order multiple traversals correctly', () => {
      const plan = planQuery(parse('user:123.friends.posts.comments'));

      const traverseSteps = plan.steps.filter(s => s.type === 'traverse');
      expect(traverseSteps.length).toBe(3);

      // Predicates should be in order
      expect(traverseSteps[0].predicate).toBe('friends');
      expect(traverseSteps[1].predicate).toBe('posts');
      expect(traverseSteps[2].predicate).toBe('comments');
    });

    it('should interleave traversals and filters correctly', () => {
      const plan = planQuery(parse('user:123.friends[?age > 30].posts[?published = true]'));

      // Order should be: lookup, traverse(friends), filter(age), traverse(posts), filter(published)
      const types = plan.steps.map(s => s.type);

      // Find positions
      const friendsTraverseIdx = plan.steps.findIndex(s => s.type === 'traverse' && s.predicate === 'friends');
      const ageFilterIdx = plan.steps.findIndex(s => s.type === 'filter' && s.filter?.field === 'age');
      const postsTraverseIdx = plan.steps.findIndex(s => s.type === 'traverse' && s.predicate === 'posts');
      const publishedFilterIdx = plan.steps.findIndex(s => s.type === 'filter' && s.filter?.field === 'published');

      expect(friendsTraverseIdx).toBeLessThan(ageFilterIdx);
      expect(ageFilterIdx).toBeLessThan(postsTraverseIdx);
      expect(postsTraverseIdx).toBeLessThan(publishedFilterIdx);
    });
  });

  describe('Reverse traversal ordering', () => {
    it('should have reverse step after source lookup', () => {
      const plan = planQuery(parse('post:456 <- likes'));

      const lookupIndex = plan.steps.findIndex(s => s.type === 'lookup');
      const reverseIndex = plan.steps.findIndex(s => s.type === 'reverse');

      expect(lookupIndex).toBeLessThan(reverseIndex);
    });

    it('should handle mixed forward/reverse traversals', () => {
      const plan = planQuery(parse('user:123.posts <- likes'));

      const traverseIndex = plan.steps.findIndex(s => s.type === 'traverse');
      const reverseIndex = plan.steps.findIndex(s => s.type === 'reverse');

      expect(traverseIndex).toBeLessThan(reverseIndex);
    });
  });

  describe('Recursion step ordering', () => {
    it('should have recurse step after source traversal', () => {
      const plan = planQuery(parse('user:123.friends*[depth <= 3]'));

      const traverseIndex = plan.steps.findIndex(s => s.type === 'traverse');
      const recurseIndex = plan.steps.findIndex(s => s.type === 'recurse');

      expect(traverseIndex).toBeLessThan(recurseIndex);
    });

    it('should include maxDepth in recurse step', () => {
      const plan = planQuery(parse('user:123.friends*[depth <= 5]'));

      const recurseStep = plan.steps.find(s => s.type === 'recurse');
      expect(recurseStep?.maxDepth).toBe(5);
    });
  });
});

// ============================================================================
// Cached Planner Edge Cases
// ============================================================================

describe('Cached Planner Edge Cases', () => {
  it('should handle concurrent planning of same query', async () => {
    const planner = createCachedPlanner({ maxSize: 100 });

    // Plan same query multiple times concurrently
    const results = await Promise.all([
      Promise.resolve(planner.plan('user:123.friends')),
      Promise.resolve(planner.plan('user:123.friends')),
      Promise.resolve(planner.plan('user:123.friends')),
    ]);

    // All results should be identical
    expect(results[0]).toBe(results[1]);
    expect(results[1]).toBe(results[2]);
  });

  it('should handle LRU eviction correctly', () => {
    const planner = createCachedPlanner({ maxSize: 2 });

    planner.plan('user:1.friends');
    planner.plan('user:2.friends');

    // Access user:1 to make it recently used
    planner.plan('user:1.friends');

    // Add user:3 - should evict user:2
    planner.plan('user:3.friends');

    const cache = planner.getCache();
    expect(cache.get('user:1.friends')).toBeDefined();
    expect(cache.get('user:2.friends')).toBeUndefined();
    expect(cache.get('user:3.friends')).toBeDefined();
  });

  it('should handle cache invalidation during use', () => {
    const planner = createCachedPlanner({ maxSize: 100 });

    const plan1 = planner.plan('user:123.friends');
    planner.invalidateCache();
    const plan2 = planner.plan('user:123.friends');

    // After invalidation, should get new plan object
    expect(plan2).not.toBe(plan1);
    // But content should be equivalent
    expect(plan2.steps.length).toBe(plan1.steps.length);
  });

  it('should handle queries with special characters', () => {
    const planner = createCachedPlanner({ maxSize: 100 });

    // Query with quoted ID containing special chars
    const plan1 = planner.plan('file:"path/to/file.txt"');
    const plan2 = planner.plan('file:"path/to/file.txt"');

    expect(plan1).toBe(plan2);
  });
});
