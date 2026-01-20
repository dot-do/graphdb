/**
 * R2 Vector Store - R2-backed HNSW Vector Storage
 *
 * Stores vector embeddings in Cloudflare R2:
 * - Vectors stored as Float32Array binary data
 * - Key format: vectors/{predicate}/{nodeId}
 * - Batch fetching for efficient search operations
 *
 * Why R2 for vectors:
 * - Vectors are large (384-1536 floats = 1.5-6KB each)
 * - Vectors are read-heavy during search (batch loading)
 * - R2 provides efficient bulk storage with Edge caching
 * - Keeps DO SQLite small for fast graph traversal
 *
 * @packageDocumentation
 */

import type { VectorStore } from './store.js';

// ============================================================================
// R2 VECTOR STORE IMPLEMENTATION
// ============================================================================

/**
 * R2-backed vector storage
 *
 * Implements VectorStore interface for R2 persistence.
 * Vectors are stored as Float32Array binary data with predicate namespacing.
 */
export class R2VectorStore implements VectorStore {
  private r2: R2Bucket;
  private predicate: string;
  private keyPrefix: string;

  /**
   * Create a new R2 vector store
   *
   * @param r2 - R2Bucket instance
   * @param predicate - Predicate name for namespacing (e.g., "embedding")
   */
  constructor(r2: R2Bucket, predicate: string) {
    this.r2 = r2;
    this.predicate = predicate;
    this.keyPrefix = `vectors/${predicate}/`;
  }

  /**
   * Get the R2 key for a node's vector
   */
  private getKey(nodeId: string): string {
    return `${this.keyPrefix}${nodeId}`;
  }

  /**
   * Save a vector for a node
   *
   * Stores the vector as Float32Array binary data.
   */
  async saveVector(nodeId: string, vector: number[]): Promise<void> {
    const key = this.getKey(nodeId);
    const float32 = new Float32Array(vector);
    await this.r2.put(key, float32.buffer);
  }

  /**
   * Load a single vector by node ID
   *
   * @returns The vector as number[] or null if not found
   */
  async loadVector(nodeId: string): Promise<number[] | null> {
    const key = this.getKey(nodeId);
    const object = await this.r2.get(key);

    if (!object) {
      return null;
    }

    const buffer = await object.arrayBuffer();
    const float32 = new Float32Array(buffer);
    return Array.from(float32);
  }

  /**
   * Batch load vectors for multiple nodes
   *
   * More efficient than multiple loadVector calls for search operations.
   * Loads vectors in parallel for better performance.
   *
   * @param nodeIds - Array of node identifiers
   * @returns Map of nodeId -> vector for found nodes
   */
  async loadVectors(nodeIds: string[]): Promise<Map<string, number[]>> {
    const result = new Map<string, number[]>();

    if (nodeIds.length === 0) {
      return result;
    }

    // Load all vectors in parallel
    const promises = nodeIds.map(async (nodeId) => {
      const vector = await this.loadVector(nodeId);
      return { nodeId, vector };
    });

    const results = await Promise.all(promises);

    for (const { nodeId, vector } of results) {
      if (vector !== null) {
        result.set(nodeId, vector);
      }
    }

    return result;
  }

  /**
   * Delete a vector by node ID
   */
  async deleteVector(nodeId: string): Promise<void> {
    const key = this.getKey(nodeId);
    await this.r2.delete(key);
  }

  /**
   * Get the number of stored vectors
   *
   * Uses R2 list operation with prefix matching.
   * Note: For large datasets, this may be slow - consider caching.
   */
  async count(): Promise<number> {
    let count = 0;
    let cursor: string | undefined;

    do {
      const listOptions: R2ListOptions = {
        prefix: this.keyPrefix,
        limit: 1000,
      };
      if (cursor !== undefined) {
        listOptions.cursor = cursor;
      }
      const listed = await this.r2.list(listOptions);

      count += listed.objects.length;
      cursor = listed.truncated ? (listed as { cursor?: string }).cursor : undefined;
    } while (cursor);

    return count;
  }

  /**
   * Get the predicate this store is namespaced to
   */
  getPredicate(): string {
    return this.predicate;
  }

  /**
   * Get the key prefix used for storage
   */
  getKeyPrefix(): string {
    return this.keyPrefix;
  }
}

// ============================================================================
// MEMORY VECTOR STORE (for testing)
// ============================================================================

/**
 * In-memory vector store for testing
 *
 * Implements VectorStore interface with simple Map storage.
 * Useful for unit tests that don't need R2.
 */
export class MemoryVectorStore implements VectorStore {
  private vectors: Map<string, number[]> = new Map();

  async saveVector(nodeId: string, vector: number[]): Promise<void> {
    // Store a copy to prevent mutation
    this.vectors.set(nodeId, [...vector]);
  }

  async loadVector(nodeId: string): Promise<number[] | null> {
    const vector = this.vectors.get(nodeId);
    return vector ? [...vector] : null;
  }

  async loadVectors(nodeIds: string[]): Promise<Map<string, number[]>> {
    const result = new Map<string, number[]>();
    for (const nodeId of nodeIds) {
      const vector = this.vectors.get(nodeId);
      if (vector) {
        result.set(nodeId, [...vector]);
      }
    }
    return result;
  }

  async deleteVector(nodeId: string): Promise<void> {
    this.vectors.delete(nodeId);
  }

  async count(): Promise<number> {
    return this.vectors.size;
  }

  /**
   * Clear all vectors (testing helper)
   */
  clear(): void {
    this.vectors.clear();
  }
}
