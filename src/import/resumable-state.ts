/**
 * ResumableImportState - Checkpoint management for long-running imports
 *
 * Key features:
 * - Persists checkpoints to Durable Object storage for durability
 * - Enables resume after timeout/restart
 * - Minimal overhead (~1KB per checkpoint)
 *
 * @packageDocumentation
 */

import type { LineReaderState } from './streaming-reader';
import type { BatchWriterState } from './batched-writer';

// ============================================================================
// Types
// ============================================================================

/**
 * Import checkpoint for resumability
 */
export interface ImportCheckpoint {
  /** Unique job identifier */
  jobId: string;
  /** Source URL being imported */
  sourceUrl: string;
  /** Current byte offset in source */
  byteOffset: number;
  /** Total bytes in source (if known) */
  totalBytes?: number;
  /** Lines processed so far */
  linesProcessed: number;
  /** Triples written so far */
  triplesWritten: number;
  /** Line reader state */
  lineReaderState: LineReaderState;
  /** Batch writer state */
  batchWriterState: BatchWriterState;
  /** Timestamp of last checkpoint */
  checkpointedAt: string;
  /** Custom metadata */
  metadata?: Record<string, unknown>;
}

/**
 * ResumableImportState interface for checkpoint management
 */
export interface ResumableImportState {
  /** Load a checkpoint for a job */
  loadCheckpoint(jobId: string): Promise<ImportCheckpoint | null>;
  /** Save a checkpoint */
  saveCheckpoint(checkpoint: ImportCheckpoint): Promise<void>;
  /** Update an existing checkpoint with partial data */
  updateCheckpoint(jobId: string, updates: Partial<ImportCheckpoint>): Promise<void>;
  /** Delete a checkpoint (after successful completion) */
  deleteCheckpoint(jobId: string): Promise<void>;
  /** List all active checkpoints */
  listCheckpoints(): Promise<string[]>;
}

// ============================================================================
// Validation
// ============================================================================

/**
 * Validates that a checkpoint has all required fields and correct structure.
 *
 * @param data - The data to validate
 * @returns True if the data is a valid checkpoint, false otherwise
 */
function isValidCheckpoint(data: unknown): data is ImportCheckpoint {
  if (data === null || typeof data !== 'object') {
    return false;
  }

  const checkpoint = data as Record<string, unknown>;

  // Check required fields exist and have correct types
  if (typeof checkpoint.jobId !== 'string' || checkpoint.jobId.length === 0) {
    return false;
  }

  if (typeof checkpoint.sourceUrl !== 'string') {
    return false;
  }

  if (typeof checkpoint.byteOffset !== 'number') {
    return false;
  }

  if (typeof checkpoint.linesProcessed !== 'number') {
    return false;
  }

  if (typeof checkpoint.triplesWritten !== 'number') {
    return false;
  }

  if (typeof checkpoint.checkpointedAt !== 'string') {
    return false;
  }

  // Validate lineReaderState structure
  if (!checkpoint.lineReaderState || typeof checkpoint.lineReaderState !== 'object') {
    return false;
  }

  const lineReaderState = checkpoint.lineReaderState as Record<string, unknown>;
  if (
    typeof lineReaderState.bytesProcessed !== 'number' ||
    typeof lineReaderState.linesEmitted !== 'number' ||
    typeof lineReaderState.partialLine !== 'string'
  ) {
    return false;
  }

  // Validate batchWriterState structure
  if (!checkpoint.batchWriterState || typeof checkpoint.batchWriterState !== 'object') {
    return false;
  }

  const batchWriterState = checkpoint.batchWriterState as Record<string, unknown>;
  if (
    typeof batchWriterState.triplesWritten !== 'number' ||
    typeof batchWriterState.chunksUploaded !== 'number' ||
    typeof batchWriterState.bytesUploaded !== 'number' ||
    !Array.isArray(batchWriterState.chunkInfos) ||
    !batchWriterState.bloomState ||
    typeof batchWriterState.bloomState !== 'object'
  ) {
    return false;
  }

  return true;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create a resumable import state manager using Durable Object storage
 *
 * Key features:
 * - Persists checkpoints to DO storage for durability
 * - Enables resume after timeout/restart
 * - Minimal overhead (~1KB per checkpoint)
 * - Validates checkpoint structure on load
 *
 * @param storage Durable Object storage
 * @returns ResumableImportState instance
 *
 * @example
 * ```typescript
 * const importState = createResumableImportState(this.state.storage);
 *
 * // Check for existing checkpoint
 * const checkpoint = await importState.loadCheckpoint('wiktionary-load');
 * if (checkpoint) {
 *   // Resume from checkpoint
 *   lineReader.restoreState(checkpoint.lineReaderState);
 *   writer.restoreState(checkpoint.batchWriterState);
 *   startOffset = checkpoint.byteOffset;
 * }
 *
 * // Save checkpoint periodically
 * await importState.saveCheckpoint({
 *   jobId: 'wiktionary-load',
 *   byteOffset: currentOffset,
 *   // ... other state
 * });
 * ```
 */
export function createResumableImportState(
  storage: DurableObjectStorage
): ResumableImportState {
  const CHECKPOINT_PREFIX = 'checkpoint:';

  return {
    async loadCheckpoint(jobId: string): Promise<ImportCheckpoint | null> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      const data = await storage.get<unknown>(key);

      if (data === undefined) {
        return null;
      }

      // Validate checkpoint structure
      if (!isValidCheckpoint(data)) {
        console.warn(
          `[ResumableImportState] Invalid checkpoint structure for job ${jobId}, returning null`
        );
        return null;
      }

      return data;
    },

    async saveCheckpoint(checkpoint: ImportCheckpoint): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${checkpoint.jobId}`;
      checkpoint.checkpointedAt = new Date().toISOString();
      await storage.put(key, checkpoint);
    },

    async updateCheckpoint(jobId: string, updates: Partial<ImportCheckpoint>): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      const existing = await storage.get<ImportCheckpoint>(key);
      if (existing) {
        const updated = { ...existing, ...updates, checkpointedAt: new Date().toISOString() };
        await storage.put(key, updated);
      }
    },

    async deleteCheckpoint(jobId: string): Promise<void> {
      const key = `${CHECKPOINT_PREFIX}${jobId}`;
      await storage.delete(key);
    },

    async listCheckpoints(): Promise<string[]> {
      const entries = await storage.list({ prefix: CHECKPOINT_PREFIX });
      return Array.from(entries.keys()).map((key) => key.replace(CHECKPOINT_PREFIX, ''));
    },
  };
}
