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
// Implementation
// ============================================================================

/**
 * Create a resumable import state manager using Durable Object storage
 *
 * Key features:
 * - Persists checkpoints to DO storage for durability
 * - Enables resume after timeout/restart
 * - Minimal overhead (~1KB per checkpoint)
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
      const checkpoint = await storage.get<ImportCheckpoint>(key);
      return checkpoint ?? null;
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
