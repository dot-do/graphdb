/**
 * CDC (Change Data Capture) Types
 *
 * Core types for CDC event streaming and buffering.
 * These types are used by the R2 writer and coordinator for
 * streaming changes to storage.
 *
 * @packageDocumentation
 */

import type { Triple } from '../core/triple';

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
