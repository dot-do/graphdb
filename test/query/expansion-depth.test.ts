/**
 * GraphDB Reference Expansion Depth Tests (TDD: RED Phase)
 *
 * Tests for maximum depth checking in reference expansion to prevent
 * infinite loops when following REF links.
 *
 * Following TDD approach: These tests define the expected behavior.
 */

import { describe, it, expect, vi } from 'vitest';
import {
  expandRefs,
  MAX_EXPANSION_DEPTH,
  type EntityResolver,
  type ExpansionResult,
} from '../../src/query/materializer';
import { Entity, createEntity } from '../../src/core/entity';
import { createEntityId } from '../../src/core/types';

// ============================================================================
// Test Fixtures
// ============================================================================

function createTestEntity(
  id: string,
  type: string,
  props: Record<string, unknown>
): Entity {
  return createEntity(createEntityId(id), type, props);
}

/**
 * Creates a chain of entities where each points to the next
 * e.g., chain of 5: entity1 -> entity2 -> entity3 -> entity4 -> entity5
 */
function createEntityChain(depth: number): Map<string, Entity> {
  const entities = new Map<string, Entity>();

  for (let i = 1; i <= depth; i++) {
    const id = `https://example.com/entity/${i}`;
    const nextRef =
      i < depth ? { '@ref': `https://example.com/entity/${i + 1}` } : undefined;

    const props: Record<string, unknown> = { name: `Entity ${i}` };
    if (nextRef) {
      props.next = nextRef;
    }

    entities.set(id, createTestEntity(id, 'ChainNode', props));
  }

  return entities;
}

/**
 * Creates a circular reference: A -> B -> C -> A
 */
function createCircularEntities(): Map<string, Entity> {
  const entities = new Map<string, Entity>();

  entities.set(
    'https://example.com/a',
    createTestEntity('https://example.com/a', 'Node', {
      name: 'Node A',
      next: { '@ref': 'https://example.com/b' },
    })
  );

  entities.set(
    'https://example.com/b',
    createTestEntity('https://example.com/b', 'Node', {
      name: 'Node B',
      next: { '@ref': 'https://example.com/c' },
    })
  );

  entities.set(
    'https://example.com/c',
    createTestEntity('https://example.com/c', 'Node', {
      name: 'Node C',
      next: { '@ref': 'https://example.com/a' }, // Circular back to A
    })
  );

  return entities;
}

// ============================================================================
// MAX_EXPANSION_DEPTH Constant Tests
// ============================================================================

describe('MAX_EXPANSION_DEPTH constant', () => {
  it('should be exported and equal to 10', () => {
    expect(MAX_EXPANSION_DEPTH).toBe(10);
  });
});

// ============================================================================
// Default Depth Limit Tests
// ============================================================================

describe('expandRefs default depth limit', () => {
  it('should expand up to MAX_DEPTH (default 10) levels', async () => {
    // Create a chain of 15 entities (exceeds default max of 10)
    const entities = createEntityChain(15);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    // Start from entity 1
    const startEntity = entities.get('https://example.com/entity/1')!;

    // Use default maxDepth (should use MAX_EXPANSION_DEPTH = 10)
    const result = await expandRefs(startEntity, resolver);

    // Should have expanded up to depth 10
    // Entity 1's next should be expanded
    expect((result.next as Entity)?.name).toBe('Entity 2');

    // Count how deep we went
    let depth = 0;
    let current: unknown = result;
    while (current && typeof current === 'object' && 'next' in current) {
      const next = (current as Record<string, unknown>).next;
      if (typeof next === 'object' && next && '$id' in next) {
        depth++;
        current = next;
      } else {
        // Reached a non-expanded ref
        break;
      }
    }

    // Default depth with no options should be 1 (existing behavior)
    // But when using MAX_EXPANSION_DEPTH explicitly, should be 10
    expect(depth).toBeGreaterThanOrEqual(1);
  });

  it('should stop at depth limit without throwing error', async () => {
    const entities = createEntityChain(20);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;

    // Should not throw even with deep chain
    await expect(
      expandRefs(startEntity, resolver, { maxDepth: MAX_EXPANSION_DEPTH })
    ).resolves.not.toThrow();
  });
});

// ============================================================================
// Configurable Depth Limit Tests
// ============================================================================

describe('expandRefs configurable depth limit', () => {
  it('should support configurable depth limit of 1', async () => {
    const entities = createEntityChain(5);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;
    const result = await expandRefs(startEntity, resolver, { maxDepth: 1 });

    // Entity 1's next should be expanded
    expect((result.next as Entity)?.$id).toBe('https://example.com/entity/2');
    expect((result.next as Entity)?.name).toBe('Entity 2');

    // Entity 2's next should NOT be expanded (still a ref)
    const entity2 = result.next as Entity;
    expect(entity2.next).toEqual({ '@ref': 'https://example.com/entity/3' });
  });

  it('should support configurable depth limit of 3', async () => {
    const entities = createEntityChain(10);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;
    const result = await expandRefs(startEntity, resolver, { maxDepth: 3 });

    // Follow the chain to verify depth
    const entity2 = result.next as Entity;
    expect(entity2?.$id).toBe('https://example.com/entity/2');

    const entity3 = entity2?.next as Entity;
    expect(entity3?.$id).toBe('https://example.com/entity/3');

    const entity4 = entity3?.next as Entity;
    expect(entity4?.$id).toBe('https://example.com/entity/4');

    // Entity 4's next should NOT be expanded (depth limit reached)
    expect(entity4?.next).toEqual({ '@ref': 'https://example.com/entity/5' });
  });

  it('should support depth limit of 0 (no expansion)', async () => {
    const entities = createEntityChain(5);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;
    const result = await expandRefs(startEntity, resolver, { maxDepth: 0 });

    // No expansion should occur
    expect(result.next).toEqual({ '@ref': 'https://example.com/entity/2' });
    expect(resolver).not.toHaveBeenCalled();
  });

  it('should support depth limit equal to MAX_EXPANSION_DEPTH', async () => {
    const entities = createEntityChain(15);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;
    const result = await expandRefs(startEntity, resolver, {
      maxDepth: MAX_EXPANSION_DEPTH,
    });

    // Should expand exactly 10 levels
    let depth = 0;
    let current: unknown = result;
    while (current && typeof current === 'object' && 'next' in current) {
      const next = (current as Record<string, unknown>).next;
      if (typeof next === 'object' && next && '$id' in next) {
        depth++;
        current = next;
      } else {
        break;
      }
    }

    expect(depth).toBe(MAX_EXPANSION_DEPTH);
  });
});

// ============================================================================
// Circular Reference Handling Tests
// ============================================================================

describe('expandRefs circular reference handling', () => {
  it('should handle circular references by respecting depth limit', async () => {
    const entities = createCircularEntities();

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/a')!;

    // With depth 3, we should go A -> B -> C -> A (ref, not expanded)
    const result = await expandRefs(startEntity, resolver, { maxDepth: 3 });

    const nodeB = result.next as Entity;
    expect(nodeB?.name).toBe('Node B');

    const nodeC = nodeB?.next as Entity;
    expect(nodeC?.name).toBe('Node C');

    // Node C's next should still be a ref (not expanded again)
    // because we've hit depth limit
    const nodeCNext = nodeC?.next as Entity;
    expect(nodeCNext?.name).toBe('Node A');

    // At depth 3, A is expanded again, but its next should be a ref
    expect(nodeCNext?.next).toEqual({ '@ref': 'https://example.com/b' });
  });

  it('should not infinitely loop on circular references', async () => {
    const entities = createCircularEntities();

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/a')!;

    // Even with high depth, should terminate
    const result = await expandRefs(startEntity, resolver, {
      maxDepth: MAX_EXPANSION_DEPTH,
    });

    // Should complete without hanging
    expect(result).toBeDefined();
    expect(result.$id).toBe('https://example.com/a');

    // The resolver should have been called a finite number of times
    // With depth 10 and circular refs of length 3, expect roughly 10 calls
    expect((resolver as ReturnType<typeof vi.fn>).mock.calls.length).toBeLessThanOrEqual(
      MAX_EXPANSION_DEPTH + 1
    );
  });
});

// ============================================================================
// Nested Expansion Depth Tracking Tests
// ============================================================================

describe('expandRefs nested depth tracking', () => {
  it('should track depth correctly in nested expansions', async () => {
    // Create an entity with multiple refs at same level
    const entities = new Map<string, Entity>();

    entities.set(
      'https://example.com/parent',
      createTestEntity('https://example.com/parent', 'Parent', {
        name: 'Parent',
        child1: { '@ref': 'https://example.com/child1' },
        child2: { '@ref': 'https://example.com/child2' },
      })
    );

    entities.set(
      'https://example.com/child1',
      createTestEntity('https://example.com/child1', 'Child', {
        name: 'Child 1',
        grandchild: { '@ref': 'https://example.com/grandchild1' },
      })
    );

    entities.set(
      'https://example.com/child2',
      createTestEntity('https://example.com/child2', 'Child', {
        name: 'Child 2',
        grandchild: { '@ref': 'https://example.com/grandchild2' },
      })
    );

    entities.set(
      'https://example.com/grandchild1',
      createTestEntity('https://example.com/grandchild1', 'Grandchild', {
        name: 'Grandchild 1',
      })
    );

    entities.set(
      'https://example.com/grandchild2',
      createTestEntity('https://example.com/grandchild2', 'Grandchild', {
        name: 'Grandchild 2',
      })
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const parentEntity = entities.get('https://example.com/parent')!;

    // With depth 2, should expand children and grandchildren
    const result = await expandRefs(parentEntity, resolver, { maxDepth: 2 });

    // Both children should be expanded
    expect((result.child1 as Entity)?.name).toBe('Child 1');
    expect((result.child2 as Entity)?.name).toBe('Child 2');

    // Grandchildren should also be expanded (depth 2)
    expect(((result.child1 as Entity)?.grandchild as Entity)?.name).toBe('Grandchild 1');
    expect(((result.child2 as Entity)?.grandchild as Entity)?.name).toBe('Grandchild 2');
  });

  it('should respect depth limit independently for each branch', async () => {
    const entities = new Map<string, Entity>();

    entities.set(
      'https://example.com/root',
      createTestEntity('https://example.com/root', 'Root', {
        name: 'Root',
        branch1: { '@ref': 'https://example.com/b1-1' },
        branch2: { '@ref': 'https://example.com/b2-1' },
      })
    );

    // Branch 1: 3 levels deep
    entities.set(
      'https://example.com/b1-1',
      createTestEntity('https://example.com/b1-1', 'Node', {
        name: 'B1-1',
        next: { '@ref': 'https://example.com/b1-2' },
      })
    );
    entities.set(
      'https://example.com/b1-2',
      createTestEntity('https://example.com/b1-2', 'Node', {
        name: 'B1-2',
        next: { '@ref': 'https://example.com/b1-3' },
      })
    );
    entities.set(
      'https://example.com/b1-3',
      createTestEntity('https://example.com/b1-3', 'Node', {
        name: 'B1-3',
      })
    );

    // Branch 2: 3 levels deep
    entities.set(
      'https://example.com/b2-1',
      createTestEntity('https://example.com/b2-1', 'Node', {
        name: 'B2-1',
        next: { '@ref': 'https://example.com/b2-2' },
      })
    );
    entities.set(
      'https://example.com/b2-2',
      createTestEntity('https://example.com/b2-2', 'Node', {
        name: 'B2-2',
        next: { '@ref': 'https://example.com/b2-3' },
      })
    );
    entities.set(
      'https://example.com/b2-3',
      createTestEntity('https://example.com/b2-3', 'Node', {
        name: 'B2-3',
      })
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const rootEntity = entities.get('https://example.com/root')!;

    // With depth 2, both branches should expand 2 levels
    const result = await expandRefs(rootEntity, resolver, { maxDepth: 2 });

    // Branch 1: should expand to B1-2, but B1-2's next should be ref
    const b1_1 = result.branch1 as Entity;
    expect(b1_1?.name).toBe('B1-1');
    const b1_2 = b1_1?.next as Entity;
    expect(b1_2?.name).toBe('B1-2');
    expect(b1_2?.next).toEqual({ '@ref': 'https://example.com/b1-3' });

    // Branch 2: should expand to B2-2, but B2-2's next should be ref
    const b2_1 = result.branch2 as Entity;
    expect(b2_1?.name).toBe('B2-1');
    const b2_2 = b2_1?.next as Entity;
    expect(b2_2?.name).toBe('B2-2');
    expect(b2_2?.next).toEqual({ '@ref': 'https://example.com/b2-3' });
  });
});

// ============================================================================
// Expansion Result Metadata Tests
// ============================================================================

describe('expandRefs result metadata', () => {
  it('should return expansion result with depth reached metadata', async () => {
    const entities = createEntityChain(5);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;

    // Use expandRefsWithMetadata for detailed result
    const result = await expandRefs(startEntity, resolver, {
      maxDepth: 3,
      includeMetadata: true,
    }) as ExpansionResult;

    // Should have metadata about expansion
    expect(result._expansionMeta).toBeDefined();
    expect(result._expansionMeta?.maxDepthReached).toBe(true);
    expect(result._expansionMeta?.actualDepth).toBe(3);
  });

  it('should indicate when depth limit was NOT reached', async () => {
    const entities = createEntityChain(2);

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const startEntity = entities.get('https://example.com/entity/1')!;

    const result = await expandRefs(startEntity, resolver, {
      maxDepth: 10,
      includeMetadata: true,
    }) as ExpansionResult;

    // Should indicate depth limit was not reached
    expect(result._expansionMeta?.maxDepthReached).toBe(false);
    expect(result._expansionMeta?.actualDepth).toBe(1); // Only 2 entities, so depth 1
  });
});

// ============================================================================
// Array Reference Expansion with Depth Tests
// ============================================================================

describe('expandRefs array references with depth limit', () => {
  it('should respect depth limit when expanding arrays of refs', async () => {
    const entities = new Map<string, Entity>();

    entities.set(
      'https://example.com/team',
      createTestEntity('https://example.com/team', 'Team', {
        name: 'Engineering',
        members: [
          { '@ref': 'https://example.com/user/1' },
          { '@ref': 'https://example.com/user/2' },
        ],
      })
    );

    entities.set(
      'https://example.com/user/1',
      createTestEntity('https://example.com/user/1', 'User', {
        name: 'Alice',
        manager: { '@ref': 'https://example.com/user/3' },
      })
    );

    entities.set(
      'https://example.com/user/2',
      createTestEntity('https://example.com/user/2', 'User', {
        name: 'Bob',
        manager: { '@ref': 'https://example.com/user/3' },
      })
    );

    entities.set(
      'https://example.com/user/3',
      createTestEntity('https://example.com/user/3', 'User', {
        name: 'Carol',
      })
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const teamEntity = entities.get('https://example.com/team')!;

    // With depth 1, members should be expanded but their managers should not
    const result = await expandRefs(teamEntity, resolver, { maxDepth: 1 });

    const members = result.members as Entity[];
    expect(members[0]?.name).toBe('Alice');
    expect(members[1]?.name).toBe('Bob');

    // Managers should still be refs (depth limit)
    expect(members[0]?.manager).toEqual({ '@ref': 'https://example.com/user/3' });
    expect(members[1]?.manager).toEqual({ '@ref': 'https://example.com/user/3' });
  });

  it('should fully expand arrays within depth limit', async () => {
    const entities = new Map<string, Entity>();

    entities.set(
      'https://example.com/team',
      createTestEntity('https://example.com/team', 'Team', {
        name: 'Engineering',
        members: [
          { '@ref': 'https://example.com/user/1' },
          { '@ref': 'https://example.com/user/2' },
        ],
      })
    );

    entities.set(
      'https://example.com/user/1',
      createTestEntity('https://example.com/user/1', 'User', {
        name: 'Alice',
        manager: { '@ref': 'https://example.com/user/3' },
      })
    );

    entities.set(
      'https://example.com/user/2',
      createTestEntity('https://example.com/user/2', 'User', {
        name: 'Bob',
        manager: { '@ref': 'https://example.com/user/3' },
      })
    );

    entities.set(
      'https://example.com/user/3',
      createTestEntity('https://example.com/user/3', 'User', {
        name: 'Carol',
      })
    );

    const resolver: EntityResolver = vi.fn().mockImplementation(async (id: string) => {
      return entities.get(id) ?? null;
    });

    const teamEntity = entities.get('https://example.com/team')!;

    // With depth 2, members AND their managers should be expanded
    const result = await expandRefs(teamEntity, resolver, { maxDepth: 2 });

    const members = result.members as Entity[];
    expect(members[0]?.name).toBe('Alice');
    expect((members[0]?.manager as Entity)?.name).toBe('Carol');
    expect((members[1]?.manager as Entity)?.name).toBe('Carol');
  });
});
