/**
 * GraphDB Query Plan Cache
 *
 * LRU cache for query plans to avoid repeated planning overhead.
 * Same queries reuse cached plans; schema changes invalidate the cache.
 */

import type { QueryPlan } from './planner';

// ============================================================================
// Types
// ============================================================================

/**
 * Interface for the plan cache
 */
export interface PlanCache {
  /**
   * Get a cached query plan
   * @param query - The query string
   * @returns The cached plan or undefined if not found
   */
  get(query: string): QueryPlan | undefined;

  /**
   * Store a query plan in the cache
   * @param query - The query string
   * @param plan - The query plan to cache
   */
  set(query: string, plan: QueryPlan): void;

  /**
   * Invalidate all cached plans (e.g., after schema change)
   */
  invalidate(): void;

  /**
   * Get the current number of cached plans
   */
  size(): number;
}

// ============================================================================
// LRU Node for doubly-linked list
// ============================================================================

interface LRUNode {
  key: string;
  value: QueryPlan;
  prev: LRUNode | null;
  next: LRUNode | null;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create a new plan cache with LRU eviction
 * @param maxSize - Maximum number of plans to cache
 * @returns A PlanCache instance
 */
export function createPlanCache(maxSize: number): PlanCache {
  // Map for O(1) lookup
  const cache = new Map<string, LRUNode>();

  // Doubly-linked list for LRU tracking
  // head.next is most recently used, tail.prev is least recently used
  const head: LRUNode = {
    key: '',
    value: null as unknown as QueryPlan,
    prev: null,
    next: null,
  };
  const tail: LRUNode = {
    key: '',
    value: null as unknown as QueryPlan,
    prev: head,
    next: null,
  };
  head.next = tail;

  /**
   * Remove a node from the linked list
   */
  function removeNode(node: LRUNode): void {
    const prev = node.prev!;
    const next = node.next!;
    prev.next = next;
    next.prev = prev;
  }

  /**
   * Add a node right after head (most recently used position)
   */
  function addToFront(node: LRUNode): void {
    node.prev = head;
    node.next = head.next;
    head.next!.prev = node;
    head.next = node;
  }

  /**
   * Move existing node to front (most recently used)
   */
  function moveToFront(node: LRUNode): void {
    removeNode(node);
    addToFront(node);
  }

  /**
   * Remove the least recently used node (right before tail)
   */
  function removeLRU(): void {
    const lru = tail.prev!;
    if (lru !== head) {
      removeNode(lru);
      cache.delete(lru.key);
    }
  }

  return {
    get(query: string): QueryPlan | undefined {
      const node = cache.get(query);
      if (node === undefined) {
        return undefined;
      }

      // Move to front (mark as recently used)
      moveToFront(node);

      return node.value;
    },

    set(query: string, plan: QueryPlan): void {
      const existingNode = cache.get(query);

      if (existingNode !== undefined) {
        // Update existing entry
        existingNode.value = plan;
        moveToFront(existingNode);
        return;
      }

      // Check if we need to evict
      if (cache.size >= maxSize) {
        removeLRU();
      }

      // Create new node
      const newNode: LRUNode = {
        key: query,
        value: plan,
        prev: null,
        next: null,
      };

      cache.set(query, newNode);
      addToFront(newNode);
    },

    invalidate(): void {
      cache.clear();
      // Reset linked list
      head.next = tail;
      tail.prev = head;
    },

    size(): number {
      return cache.size;
    },
  };
}
