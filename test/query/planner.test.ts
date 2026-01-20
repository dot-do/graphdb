/**
 * GraphDB Query Planner Tests (E7.1: RED)
 *
 * Tests for the query planner that converts parsed AST into execution plans.
 * Following TDD approach: write failing tests first, then implement to GREEN.
 */

import { describe, it, expect } from 'vitest';
import {
  parse,
  stringify,
  type QueryNode,
  type EntityLookup,
  type PropertyAccess,
  type ReverseTraversal,
  type Filter,
  type Expansion,
  type Recursion,
} from '../../src/query/parser';
import {
  planQuery,
  optimizePlan,
  estimateCost,
  type QueryPlan,
  type PlanStep,
  type FilterExpr,
} from '../../src/query/planner';

// ============================================================================
// Parser Tests (adapted from spike)
// ============================================================================

describe('Query Parser', () => {
  describe('Entity Lookup', () => {
    it('should parse simple entity lookup: user:123', () => {
      const result = parse('user:123');
      expect(result).toEqual({
        type: 'entity',
        namespace: 'user',
        id: '123',
      });
    });

    it('should parse entity with string ID', () => {
      const result = parse('user:"abc-def-123"');
      expect(result).toEqual({
        type: 'entity',
        namespace: 'user',
        id: 'abc-def-123',
      });
    });
  });

  describe('Property Access (Forward Traversal)', () => {
    it('should parse single hop: user:123.friends', () => {
      const result = parse('user:123.friends');
      expect(result).toEqual({
        type: 'property',
        name: 'friends',
        source: {
          type: 'entity',
          namespace: 'user',
          id: '123',
        },
      });
    });

    it('should parse multi-hop: user:123.friends.posts', () => {
      const result = parse('user:123.friends.posts');
      expect(result.type).toBe('property');
      expect((result as PropertyAccess).name).toBe('posts');
      expect((result as PropertyAccess).source.type).toBe('property');
    });
  });

  describe('Reverse Traversal', () => {
    it('should parse reverse traversal: post:456 <- likes', () => {
      const result = parse('post:456 <- likes');
      expect(result).toEqual({
        type: 'reverse',
        predicate: 'likes',
        source: {
          type: 'entity',
          namespace: 'post',
          id: '456',
        },
      });
    });
  });

  describe('Filters', () => {
    it('should parse simple filter: user:123.friends[?age > 30]', () => {
      const result = parse('user:123.friends[?age > 30]');
      expect(result.type).toBe('filter');
      const filter = result as Filter;
      expect(filter.condition).toEqual({
        type: 'comparison',
        field: 'age',
        operator: '>',
        value: 30,
      });
    });
  });

  describe('JSON Expansion', () => {
    it('should parse simple expansion: user:123 { name, email }', () => {
      const result = parse('user:123 { name, email }');
      expect(result.type).toBe('expand');
      const expand = result as Expansion;
      expect(expand.fields).toEqual([{ name: 'name' }, { name: 'email' }]);
    });
  });

  describe('Recursion', () => {
    it('should parse bounded recursion: user:123.friends*[depth <= 3]', () => {
      const result = parse('user:123.friends*[depth <= 3]');
      expect(result.type).toBe('recurse');
      const recurse = result as Recursion;
      expect(recurse.maxDepth).toBe(3);
    });
  });

  describe('Stringify (Round-Trip)', () => {
    const testCases = [
      'user:123',
      'user:123.friends',
      'post:456 <- likes',
    ];

    for (const query of testCases) {
      it(`should round-trip: ${query}`, () => {
        const ast = parse(query);
        const result = stringify(ast);
        const reparsed = parse(result);
        expect(reparsed).toEqual(ast);
      });
    }
  });
});

// ============================================================================
// Query Planner Tests
// ============================================================================

describe('Query Planner', () => {
  describe('planQuery', () => {
    it('should create lookup step for entity:id', () => {
      const ast = parse('user:123');
      const plan = planQuery(ast);

      expect(plan.steps.length).toBeGreaterThan(0);
      expect(plan.steps[0].type).toBe('lookup');
      expect(plan.steps[0].entityIds).toContain('user:123');
    });

    it('should create traverse step for .predicate', () => {
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);

      // Should have lookup + traverse
      expect(plan.steps.length).toBe(2);
      expect(plan.steps[0].type).toBe('lookup');
      expect(plan.steps[1].type).toBe('traverse');
      expect(plan.steps[1].predicate).toBe('friends');
    });

    it('should create reverse step for <- predicate', () => {
      const ast = parse('post:456 <- likes');
      const plan = planQuery(ast);

      expect(plan.steps.some((s) => s.type === 'reverse')).toBe(true);
      const reverseStep = plan.steps.find((s) => s.type === 'reverse');
      expect(reverseStep?.predicate).toBe('likes');
    });

    it('should handle multi-hop queries', () => {
      const ast = parse('user:123.friends.posts.comments');
      const plan = planQuery(ast);

      // Should have: lookup + traverse + traverse + traverse
      expect(plan.steps.length).toBe(4);
      expect(plan.steps[0].type).toBe('lookup');
      expect(plan.steps[1].type).toBe('traverse');
      expect(plan.steps[2].type).toBe('traverse');
      expect(plan.steps[3].type).toBe('traverse');

      // Verify predicates
      expect(plan.steps[1].predicate).toBe('friends');
      expect(plan.steps[2].predicate).toBe('posts');
      expect(plan.steps[3].predicate).toBe('comments');
    });

    it('should handle filters [?field > value]', () => {
      const ast = parse('user:123.friends[?age > 30]');
      const plan = planQuery(ast);

      expect(plan.steps.some((s) => s.type === 'filter')).toBe(true);
      const filterStep = plan.steps.find((s) => s.type === 'filter');
      expect(filterStep?.filter).toBeDefined();
      expect(filterStep?.filter?.field).toBe('age');
      expect(filterStep?.filter?.op).toBe('>');
      expect(filterStep?.filter?.value).toBe(30);
    });

    it('should handle expansion { field1, field2 }', () => {
      const ast = parse('user:123 { name, email }');
      const plan = planQuery(ast);

      expect(plan.steps.some((s) => s.type === 'expand')).toBe(true);
      const expandStep = plan.steps.find((s) => s.type === 'expand');
      expect(expandStep?.fields).toContain('name');
      expect(expandStep?.fields).toContain('email');
    });

    it('should handle recursion *[depth <= N]', () => {
      const ast = parse('user:123.friends*[depth <= 3]');
      const plan = planQuery(ast);

      expect(plan.steps.some((s) => s.type === 'recurse')).toBe(true);
      const recurseStep = plan.steps.find((s) => s.type === 'recurse');
      expect(recurseStep?.maxDepth).toBe(3);
    });

    it('should include shard information in plan', () => {
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);

      expect(plan.shards.length).toBeGreaterThan(0);
      expect(plan.shards[0].shardId).toBeDefined();
    });
  });

  describe('optimizePlan', () => {
    it('should combine adjacent lookups to same shard', () => {
      // Create a plan with multiple lookups that could be batched
      const ast = parse('user:123.friends');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      // Optimized plan should not have more steps than original
      expect(optimized.steps.length).toBeLessThanOrEqual(plan.steps.length);
    });

    it('should preserve query semantics after optimization', () => {
      const ast = parse('user:123.friends.posts');
      const plan = planQuery(ast);
      const optimized = optimizePlan(plan);

      // Should still produce correct traversal order
      const traverseSteps = optimized.steps.filter(
        (s) => s.type === 'traverse'
      );
      expect(traverseSteps.length).toBeGreaterThan(0);
    });
  });

  describe('estimateCost', () => {
    it('should reflect query complexity', () => {
      const simplePlan = planQuery(parse('user:123'));
      const complexPlan = planQuery(parse('user:123.friends.posts.comments'));

      const simpleCost = estimateCost(simplePlan);
      const complexCost = estimateCost(complexPlan);

      expect(complexCost).toBeGreaterThan(simpleCost);
    });

    it('should add cost for filters', () => {
      const withoutFilter = planQuery(parse('user:123.friends'));
      const withFilter = planQuery(parse('user:123.friends[?age > 30]'));

      const costWithout = estimateCost(withoutFilter);
      const costWith = estimateCost(withFilter);

      expect(costWith).toBeGreaterThan(costWithout);
    });

    it('should add cost for recursion', () => {
      const withoutRecursion = planQuery(parse('user:123.friends'));
      const withRecursion = planQuery(parse('user:123.friends*[depth <= 3]'));

      const costWithout = estimateCost(withoutRecursion);
      const costWith = estimateCost(withRecursion);

      expect(costWith).toBeGreaterThan(costWithout);
    });

    it('should return reasonable cost for simple queries', () => {
      const plan = planQuery(parse('user:123'));
      const cost = estimateCost(plan);

      expect(cost).toBeGreaterThan(0);
      expect(cost).toBeLessThan(100); // Should not be excessive
    });
  });

  describe('canCache', () => {
    it('should be true for read-only queries', () => {
      const readPlan = planQuery(parse('user:123.friends'));
      expect(readPlan.canCache).toBe(true);
    });

    it('should generate cacheKey for cacheable queries', () => {
      const plan = planQuery(parse('user:123.friends'));
      expect(plan.cacheKey).toBeDefined();
      expect(typeof plan.cacheKey).toBe('string');
    });

    it('should generate consistent cacheKey for same query', () => {
      const plan1 = planQuery(parse('user:123.friends'));
      const plan2 = planQuery(parse('user:123.friends'));

      expect(plan1.cacheKey).toBe(plan2.cacheKey);
    });
  });
});

// ============================================================================
// FilterExpr Tests
// ============================================================================

describe('FilterExpr', () => {
  it('should support all comparison operators', () => {
    const operators = ['=', '!=', '>', '<', '>=', '<='] as const;

    for (const op of operators) {
      const query = `user:123.friends[?age ${op} 30]`;
      const ast = parse(query);
      const plan = planQuery(ast);
      const filterStep = plan.steps.find((s) => s.type === 'filter');

      expect(filterStep?.filter?.op).toBe(op);
    }
  });

  it('should support AND conditions', () => {
    const ast = parse('user:123.friends[?age > 30 and status = "active"]');
    const plan = planQuery(ast);
    const filterStep = plan.steps.find((s) => s.type === 'filter');

    expect(filterStep?.filter?.and).toBeDefined();
  });

  it('should support OR conditions', () => {
    const ast = parse('user:123.friends[?role = "admin" or role = "mod"]');
    const plan = planQuery(ast);
    const filterStep = plan.steps.find((s) => s.type === 'filter');

    expect(filterStep?.filter?.or).toBeDefined();
  });
});

// ============================================================================
// Complex Query Planning Tests
// ============================================================================

describe('Complex Query Planning', () => {
  it('should plan query with filter after traversal', () => {
    const ast = parse('user:123.friends[?age > 30].posts');
    const plan = planQuery(ast);

    // Order should be: lookup -> traverse -> filter -> traverse
    const types = plan.steps.map((s) => s.type);
    expect(types).toContain('lookup');
    expect(types).toContain('traverse');
    expect(types).toContain('filter');
  });

  it('should plan query with expansion', () => {
    const ast = parse('user:123.friends { name, email }');
    const plan = planQuery(ast);

    const types = plan.steps.map((s) => s.type);
    expect(types).toContain('expand');
  });

  it('should handle deeply nested queries', () => {
    const ast = parse(
      'user:123.friends[?age > 30].posts { title, author { name } }'
    );
    const plan = planQuery(ast);

    expect(plan.steps.length).toBeGreaterThan(2);
    expect(plan.estimatedCost).toBeGreaterThan(0);
  });
});
