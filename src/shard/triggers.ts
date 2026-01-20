/**
 * Index Maintenance Triggers for GraphDB Shard DO
 *
 * Provides hooks for index maintenance when CRUD operations occur:
 * - onInsert: Called after a triple is inserted
 * - onUpdate: Called after a triple is updated (with old and new values)
 * - onDelete: Called after a triple is deleted
 * - onBatchInsert: Called after multiple triples are inserted
 * - onBatchDelete: Called after multiple entities are deleted
 *
 * Also provides CDC (Change Data Capture) buffering for streaming
 * changes to the coordinator for replication and consistency.
 *
 * @see CLAUDE.md for architecture details
 */

import type { Triple } from '../core/triple.js';
import type { EntityId } from '../core/types.js';
import { ObjectType } from '../core/types.js';
import type { BloomFilter } from '../snippet/bloom.js';
import { addToFilter } from '../snippet/bloom.js';

// ============================================================================
// CDC Event Types
// ============================================================================

/**
 * CDC event for streaming to coordinator
 *
 * Represents a change that occurred in the shard:
 * - insert: A new triple was added
 * - update: An existing triple was modified (previousValue contains old data)
 * - delete: A triple was removed (soft delete via tombstone)
 */
export interface CDCEvent {
  /** Type of change */
  type: 'insert' | 'update' | 'delete';
  /** The triple that was affected (new value for updates) */
  triple: Triple;
  /** Previous value for updates */
  previousValue?: Triple;
  /** Timestamp of the change event */
  timestamp: bigint;
}

// ============================================================================
// Index Maintainer Interface
// ============================================================================

/**
 * Index maintenance hooks for CRUD operations
 *
 * These hooks are called after the primary CRUD operation completes.
 * They ensure indexes remain consistent with the triple data.
 */
export interface IndexMaintainer {
  /**
   * Called after a triple is inserted
   * Updates SPO, POS, and OSP (for REF types) indexes
   */
  onInsert(triple: Triple): Promise<void>;

  /**
   * Called after a triple is updated
   * Updates indexes to reflect the new value, handles type changes
   */
  onUpdate(oldTriple: Triple, newTriple: Triple): Promise<void>;

  /**
   * Called after a triple is deleted
   * Cleans up index entries for the deleted triple
   */
  onDelete(triple: Triple): Promise<void>;

  /**
   * Batch insert optimization
   * More efficient than calling onInsert for each triple
   */
  onBatchInsert(triples: Triple[]): Promise<void>;

  /**
   * Batch delete by subject
   * Cleans up all index entries for the given subjects
   */
  onBatchDelete(subjects: EntityId[]): Promise<void>;
}

// ============================================================================
// CDC Buffer Interface
// ============================================================================

/**
 * CDC buffer for streaming changes to coordinator
 *
 * Buffers change events for batch transmission to reduce
 * network overhead and improve throughput.
 */
export interface CDCBuffer {
  /** Append a new event to the buffer */
  append(event: CDCEvent): void;

  /** Flush all events and clear the buffer */
  flush(): CDCEvent[];

  /** Get current buffer size */
  size(): number;
}

// ============================================================================
// Index Maintainer Implementation
// ============================================================================

/**
 * Create an IndexMaintainer for the given SqlStorage
 *
 * The maintainer tracks index operations but the actual index
 * updates happen through SQLite's built-in indexes. This layer
 * provides hooks for:
 * - CDC event generation
 * - Bloom filter updates
 * - Custom index maintenance (FTS, geo, etc.)
 *
 * @param sql SqlStorage instance from DurableObjectState
 * @returns IndexMaintainer implementation
 */
export function createIndexMaintainer(_sql: SqlStorage): IndexMaintainer {
  return {
    async onInsert(_triple: Triple): Promise<void> {
      // SQLite's built-in indexes (idx_spo, idx_pos, idx_osp) are
      // automatically maintained by INSERT statements.
      //
      // This hook is for:
      // 1. Custom index maintenance (FTS, geo indexing)
      // 2. Tracking for CDC events
      // 3. Future: external index services

      // If this is a REF type, the OSP index is automatically updated
      // by the partial index: idx_osp WHERE obj_type = 10

      // Log index operation (future: metrics)
      // console.debug(`Index: INSERT ${triple.subject} ${triple.predicate}`);
    },

    async onUpdate(oldTriple: Triple, newTriple: Triple): Promise<void> {
      // For MVCC, updates insert new rows rather than modifying existing ones.
      // SQLite indexes are automatically maintained.
      //
      // This hook handles special cases:
      // 1. Type changes (e.g., STRING -> INT64) may affect POS queries
      // 2. REF changes need OSP index consideration
      // 3. Custom index updates (FTS re-indexing, geo updates)

      // Check for type changes that might affect query patterns
      if (oldTriple.object.type !== newTriple.object.type) {
        // Type changed - may need to update type-specific indexes
        // console.debug(`Index: TYPE_CHANGE ${oldTriple.object.type} -> ${newTriple.object.type}`);
      }

      // Check for REF changes that affect OSP index
      if (oldTriple.object.type === ObjectType.REF && newTriple.object.type === ObjectType.REF) {
        if (oldTriple.object.value !== newTriple.object.value) {
          // REF target changed - reverse lookup index affected
          // console.debug(`Index: REF_CHANGE ${oldTriple.object.value} -> ${newTriple.object.value}`);
        }
      }
    },

    async onDelete(triple: Triple): Promise<void> {
      // For soft deletes, a tombstone (NULL type) is inserted.
      // The original data remains for MVCC history.
      //
      // This hook handles:
      // 1. Custom index cleanup (FTS, geo)
      // 2. OSP index considerations for REF types
      // 3. Future: garbage collection triggers

      if (triple.object.type === ObjectType.REF) {
        // REF deletion affects reverse lookups
        // console.debug(`Index: DELETE_REF ${triple.object.value}`);
      }
    },

    async onBatchInsert(triples: Triple[]): Promise<void> {
      // Batch insert optimization
      // SQLite transactions handle the actual efficiency
      //
      // This hook provides opportunity for:
      // 1. Bulk FTS indexing
      // 2. Batch CDC event generation
      // 3. Bloom filter batch updates

      if (triples.length === 0) {
        return;
      }

      // Process each triple through onInsert
      // In future, this could use more efficient batch operations
      for (const triple of triples) {
        await this.onInsert(triple);
      }
    },

    async onBatchDelete(subjects: EntityId[]): Promise<void> {
      // Batch delete by subject
      // Useful for entity deletion which removes all predicates

      if (subjects.length === 0) {
        return;
      }

      // Future: Could query for all triples of these subjects
      // and call onDelete for each, or use bulk index cleanup
      // console.debug(`Index: BATCH_DELETE ${subjects.length} subjects`);
    },
  };
}

// ============================================================================
// CDC Buffer Implementation
// ============================================================================

/**
 * Default maximum buffer size (1000 events)
 */
const DEFAULT_MAX_BUFFER_SIZE = 1000;

/**
 * Create a CDC buffer for streaming changes to coordinator
 *
 * The buffer accumulates change events and flushes them in batches
 * for efficient transmission to the coordinator.
 *
 * @param maxSize Maximum number of events to buffer (default: 1000)
 * @returns CDCBuffer implementation
 */
export function createCDCBuffer(maxSize: number = DEFAULT_MAX_BUFFER_SIZE): CDCBuffer {
  let events: CDCEvent[] = [];

  return {
    append(event: CDCEvent): void {
      events.push(event);

      // If buffer exceeds max size, auto-flush oldest events
      // This prevents unbounded memory growth
      if (events.length > maxSize) {
        // Keep most recent events, discard oldest
        events = events.slice(-maxSize);
      }
    },

    flush(): CDCEvent[] {
      const flushed = events;
      events = [];
      return flushed;
    },

    size(): number {
      return events.length;
    },
  };
}

// ============================================================================
// Bloom Filter Update
// ============================================================================

/**
 * Update a bloom filter when an entity changes
 *
 * For 'add' actions, the entity ID is added to the filter.
 * For 'remove' actions, the count is decremented (bloom filters
 * cannot truly remove items, but we track the count for stats).
 *
 * @param filter The bloom filter to update
 * @param entityId The entity ID that changed
 * @param action 'add' for inserts, 'remove' for deletes
 */
export function updateBloomFilter(
  filter: BloomFilter,
  entityId: EntityId,
  action: 'add' | 'remove'
): void {
  if (action === 'add') {
    // Add entity to bloom filter
    addToFilter(filter, entityId);
  } else if (action === 'remove') {
    // Bloom filters cannot remove items
    // But we decrement the count for tracking purposes
    // This is useful for knowing when to rebuild the filter
    if (filter.count > 0) {
      filter.count--;
    }
  }
}

// ============================================================================
// Integration Helper
// ============================================================================

/**
 * Options for creating an integrated index maintenance system
 */
export interface IndexMaintenanceOptions {
  /** SqlStorage instance */
  sql: SqlStorage;
  /** Optional CDC buffer (created if not provided) */
  cdcBuffer?: CDCBuffer;
  /** Optional bloom filter for entity tracking */
  bloomFilter?: BloomFilter;
  /** Maximum CDC buffer size */
  maxBufferSize?: number;
}

/**
 * Integrated index maintenance system
 *
 * Combines IndexMaintainer, CDCBuffer, and BloomFilter updates
 * into a single coordinated system.
 */
export interface IntegratedIndexMaintenance {
  /** The index maintainer */
  maintainer: IndexMaintainer;
  /** The CDC buffer */
  cdcBuffer: CDCBuffer;
  /** Handle triple insert with full index maintenance */
  handleInsert(triple: Triple): Promise<void>;
  /** Handle triple update with full index maintenance */
  handleUpdate(oldTriple: Triple, newTriple: Triple): Promise<void>;
  /** Handle triple delete with full index maintenance */
  handleDelete(triple: Triple): Promise<void>;
}

/**
 * Create an integrated index maintenance system
 *
 * This helper combines all index maintenance components for
 * easy integration with the TripleStore.
 *
 * @param options Configuration options
 * @returns Integrated index maintenance system
 */
export function createIntegratedIndexMaintenance(
  options: IndexMaintenanceOptions
): IntegratedIndexMaintenance {
  const { sql, maxBufferSize } = options;

  const maintainer = createIndexMaintainer(sql);
  const cdcBuffer = options.cdcBuffer ?? createCDCBuffer(maxBufferSize);
  const bloomFilter = options.bloomFilter;

  return {
    maintainer,
    cdcBuffer,

    async handleInsert(triple: Triple): Promise<void> {
      // Update indexes
      await maintainer.onInsert(triple);

      // Generate CDC event
      cdcBuffer.append({
        type: 'insert',
        triple,
        timestamp: BigInt(Date.now()),
      });

      // Update bloom filter if provided
      if (bloomFilter) {
        updateBloomFilter(bloomFilter, triple.subject, 'add');
      }
    },

    async handleUpdate(oldTriple: Triple, newTriple: Triple): Promise<void> {
      // Update indexes
      await maintainer.onUpdate(oldTriple, newTriple);

      // Generate CDC event
      cdcBuffer.append({
        type: 'update',
        triple: newTriple,
        previousValue: oldTriple,
        timestamp: BigInt(Date.now()),
      });

      // Bloom filter doesn't need update for updates
      // (entity already exists)
    },

    async handleDelete(triple: Triple): Promise<void> {
      // Update indexes
      await maintainer.onDelete(triple);

      // Generate CDC event
      cdcBuffer.append({
        type: 'delete',
        triple,
        timestamp: BigInt(Date.now()),
      });

      // Update bloom filter if provided
      // Note: This decrements count but can't truly remove from filter
      if (bloomFilter) {
        updateBloomFilter(bloomFilter, triple.subject, 'remove');
      }
    },
  };
}
