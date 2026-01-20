/**
 * Shard Router Tests
 *
 * TDD RED phase - these tests define the expected behavior
 * for the shard routing logic.
 */

import { describe, it, expect } from 'vitest';
import {
  routeEntity,
  routeQuery,
  getShardId,
  canServeFromCache,
  generateCacheKey,
  estimateQueryCost,
  type ShardInfo,
  type RouteResult,
} from '../../src/snippet/router';
import { createEntityId, createNamespace } from '../../src/core/types';

describe('routeEntity', () => {
  it('should extract correct namespace and shard from simple URL', () => {
    const entityId = createEntityId('https://example.com/users/123');
    const result = routeEntity(entityId);

    // Namespace includes first path segment when there are multiple segments
    expect(result.namespace).toBe('https://example.com/users/');
    expect(result.shardId).toBeDefined();
    expect(typeof result.shardId).toBe('string');
  });

  it('should extract namespace from deep path URL', () => {
    const entityId = createEntityId('https://example.com/crm/acme/customer/123');
    const result = routeEntity(entityId);

    // Namespace should be derived from host + first path segment
    expect(result.namespace).toBe('https://example.com/crm/');
    expect(result.shardId).toBeDefined();
  });

  it('should handle URL with subdomain', () => {
    const entityId = createEntityId('https://api.example.com/v1/users/456');
    const result = routeEntity(entityId);

    expect(result.namespace).toBe('https://api.example.com/v1/');
    expect(result.shardId).toBeDefined();
  });

  it('should handle bare domain URL', () => {
    const entityId = createEntityId('https://example.com');
    const result = routeEntity(entityId);

    expect(result.namespace).toBe('https://example.com/');
    expect(result.shardId).toBeDefined();
  });

  it('should produce consistent shard for same namespace', () => {
    const entity1 = createEntityId('https://example.com/users/123');
    const entity2 = createEntityId('https://example.com/users/456');

    const result1 = routeEntity(entity1);
    const result2 = routeEntity(entity2);

    // Same namespace should map to same shard
    expect(result1.shardId).toBe(result2.shardId);
  });

  it('should produce different shards for different namespaces', () => {
    const entity1 = createEntityId('https://example.com/users/123');
    const entity2 = createEntityId('https://other.com/users/123');

    const result1 = routeEntity(entity1);
    const result2 = routeEntity(entity2);

    // Different namespaces should likely map to different shards
    expect(result1.shardId).not.toBe(result2.shardId);
  });
});

describe('routeQuery', () => {
  it('should identify single namespace in simple query', () => {
    const query = 'https://example.com/users/123.friends';
    const result = routeQuery(query);

    expect(result.shards.length).toBe(1);
    // Query path /users/123 has 2 segments, so namespace includes first segment
    expect(result.shards[0].namespace).toBe('https://example.com/users/');
  });

  it('should identify multiple namespaces in cross-namespace query', () => {
    const query = 'https://example.com/users/123.friends[?(@.$type == "https://other.com/Person")]';
    const result = routeQuery(query);

    expect(result.shards.length).toBe(2);
    const namespaces = result.shards.map((s) => s.namespace);
    expect(namespaces).toContain('https://example.com/users/');
    expect(namespaces).toContain('https://other.com/');
  });

  it('should deduplicate shards when same namespace appears multiple times', () => {
    const query = 'https://example.com/users/123.friends.friends';
    const result = routeQuery(query);

    // Even though we traverse multiple times, same namespace = same shard
    expect(result.shards.length).toBe(1);
  });

  it('should return cache key for cacheable queries', () => {
    const query = 'https://example.com/users/123';
    const result = routeQuery(query);

    expect(result.cacheKey).toBeDefined();
    expect(typeof result.cacheKey).toBe('string');
  });

  it('should include TTL for read queries', () => {
    const query = 'https://example.com/users/123';
    const result = routeQuery(query);

    expect(result.ttl).toBeDefined();
    expect(result.ttl).toBeGreaterThan(0);
  });
});

describe('getShardId', () => {
  it('should be deterministic - same input produces same output', () => {
    const namespace = createNamespace('https://example.com/');

    const shard1 = getShardId(namespace);
    const shard2 = getShardId(namespace);

    expect(shard1).toBe(shard2);
  });

  it('should produce valid DO ID format', () => {
    const namespace = createNamespace('https://example.com/');
    const shardId = getShardId(namespace);

    // DO ID should be a non-empty string, typically hex or base36
    expect(shardId).toMatch(/^[a-z0-9-]+$/);
  });

  it('should produce different IDs for different namespaces', () => {
    const ns1 = createNamespace('https://example.com/');
    const ns2 = createNamespace('https://other.com/');

    const shard1 = getShardId(ns1);
    const shard2 = getShardId(ns2);

    expect(shard1).not.toBe(shard2);
  });

  it('should handle namespaces with paths', () => {
    const ns1 = createNamespace('https://example.com/api/');
    const ns2 = createNamespace('https://example.com/graph/');

    const shard1 = getShardId(ns1);
    const shard2 = getShardId(ns2);

    expect(shard1).toBeDefined();
    expect(shard2).toBeDefined();
    expect(shard1).not.toBe(shard2);
  });
});

describe('canServeFromCache', () => {
  it('should return true for read-only entity lookup', () => {
    const query = 'https://example.com/users/123';
    expect(canServeFromCache(query)).toBe(true);
  });

  it('should return true for property traversal', () => {
    const query = 'https://example.com/users/123.name';
    expect(canServeFromCache(query)).toBe(true);
  });

  it('should return true for multi-hop traversal', () => {
    const query = 'https://example.com/users/123.friends.name';
    expect(canServeFromCache(query)).toBe(true);
  });

  it('should return false for mutation operations', () => {
    // Assuming mutation queries have specific markers
    const query = 'MUTATE https://example.com/users/123';
    expect(canServeFromCache(query)).toBe(false);
  });

  it('should return false for queries with timestamp filters', () => {
    // Time-sensitive queries should not be cached
    const query = 'https://example.com/users/123[?(@.createdAt > NOW())]';
    expect(canServeFromCache(query)).toBe(false);
  });
});

describe('generateCacheKey', () => {
  it('should be deterministic - same query produces same key', () => {
    const query = 'https://example.com/users/123.friends';

    const key1 = generateCacheKey(query);
    const key2 = generateCacheKey(query);

    expect(key1).toBe(key2);
  });

  it('should produce different keys for different queries', () => {
    const query1 = 'https://example.com/users/123';
    const query2 = 'https://example.com/users/456';

    const key1 = generateCacheKey(query1);
    const key2 = generateCacheKey(query2);

    expect(key1).not.toBe(key2);
  });

  it('should produce reasonably short keys', () => {
    const longQuery =
      'https://example.com/very/long/path/users/123.friends.friends.friends.name';
    const key = generateCacheKey(longQuery);

    // Cache keys should be reasonably short for efficiency
    expect(key.length).toBeLessThanOrEqual(64);
  });

  it('should normalize equivalent queries to same key', () => {
    // Queries with different whitespace should produce same key
    const query1 = 'https://example.com/users/123';
    const query2 = '  https://example.com/users/123  ';

    const key1 = generateCacheKey(query1);
    const key2 = generateCacheKey(query2);

    expect(key1).toBe(key2);
  });
});

describe('estimateQueryCost', () => {
  it('should return 1 for simple entity lookup', () => {
    const query = 'https://example.com/users/123';
    const cost = estimateQueryCost(query);

    expect(cost).toBe(1);
  });

  it('should return 2 for single-hop traversal', () => {
    const query = 'https://example.com/users/123.friends';
    const cost = estimateQueryCost(query);

    expect(cost).toBe(2);
  });

  it('should increase cost for multiple hops', () => {
    const query1 = 'https://example.com/users/123.friends';
    const query2 = 'https://example.com/users/123.friends.friends';
    const query3 = 'https://example.com/users/123.friends.friends.name';

    const cost1 = estimateQueryCost(query1);
    const cost2 = estimateQueryCost(query2);
    const cost3 = estimateQueryCost(query3);

    expect(cost2).toBeGreaterThan(cost1);
    expect(cost3).toBeGreaterThan(cost2);
  });

  it('should increase cost for cross-namespace queries', () => {
    const sameNs = 'https://example.com/users/123.friends';
    const crossNs =
      'https://example.com/users/123.friends[?(@.$type == "https://other.com/Person")]';

    const cost1 = estimateQueryCost(sameNs);
    const cost2 = estimateQueryCost(crossNs);

    expect(cost2).toBeGreaterThan(cost1);
  });

  it('should cap cost at reasonable maximum', () => {
    // Even very complex queries should have bounded cost
    const complexQuery =
      'https://example.com/a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z';
    const cost = estimateQueryCost(complexQuery);

    // Max cost should be capped (e.g., 100 for rate limiting)
    expect(cost).toBeLessThanOrEqual(100);
  });

  it('should return positive integer', () => {
    const queries = [
      'https://example.com/users/123',
      'https://example.com/users/123.name',
      'https://example.com/users/123.friends.name',
    ];

    for (const query of queries) {
      const cost = estimateQueryCost(query);
      expect(Number.isInteger(cost)).toBe(true);
      expect(cost).toBeGreaterThan(0);
    }
  });
});

describe('namespace extraction from URLs', () => {
  it('should extract namespace from CRM-style URL', () => {
    // "https://example.com/crm/acme/customer/123" -> namespace "https://example.com/crm/"
    const entityId = createEntityId('https://example.com/crm/acme/customer/123');
    const result = routeEntity(entityId);

    expect(result.namespace).toBe('https://example.com/crm/');
  });

  it('should extract namespace from API-style URL', () => {
    const entityId = createEntityId('https://api.example.com/v2/resources/abc');
    const result = routeEntity(entityId);

    expect(result.namespace).toBe('https://api.example.com/v2/');
  });

  it('should handle root-level resources', () => {
    const entityId = createEntityId('https://example.com/123');
    const result = routeEntity(entityId);

    expect(result.namespace).toBe('https://example.com/');
  });
});

describe('edge cases', () => {
  it('should handle query with filters', () => {
    const query = 'https://example.com/users[?(@.age > 21)]';
    const result = routeQuery(query);

    expect(result.shards.length).toBeGreaterThanOrEqual(1);
  });

  it('should handle query with array access', () => {
    const query = 'https://example.com/users/123.friends[0]';
    const result = routeQuery(query);

    expect(result.shards.length).toBe(1);
  });

  it('should handle deeply nested paths', () => {
    const entityId = createEntityId(
      'https://example.com/org/dept/team/project/resource/123'
    );
    const result = routeEntity(entityId);

    // Should still extract a valid namespace
    expect(result.namespace).toBeDefined();
    expect(result.shardId).toBeDefined();
  });
});
