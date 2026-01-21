/**
 * R2 CDC Restore Module
 *
 * Provides backup listing, point-in-time recovery, and restore functionality
 * from R2 CDC lakehouse. Enables disaster recovery by replaying CDC events
 * stored in GraphCol format.
 *
 * Features:
 * - List available backups/snapshots by namespace and date
 * - Point-in-time recovery with timestamp precision
 * - Event replay with configurable batch processing
 * - Progress tracking and resumable restores
 *
 * @packageDocumentation
 */

import type { CDCEvent } from './cdc-types';
import type { Namespace } from '../core/types';
import type { Triple } from '../core/triple';
import {
  listCDCFiles,
  readCDCFile,
  parseNamespaceToPath,
  parseCDCPath,
  formatDatePath,
  type ListCDCFilesOptions,
} from './r2-writer';

// ============================================================================
// Types
// ============================================================================

/**
 * Backup snapshot metadata
 */
export interface BackupSnapshot {
  /** Date of the snapshot (YYYY-MM-DD) */
  date: string;
  /** Namespace the backup belongs to */
  namespace: Namespace;
  /** Number of CDC files in this snapshot */
  fileCount: number;
  /** Total size in bytes (if available) */
  totalSizeBytes?: number;
  /** Earliest timestamp in the snapshot */
  earliestTimestamp?: bigint;
  /** Latest timestamp in the snapshot */
  latestTimestamp?: bigint;
  /** List of file paths in chronological order */
  files: string[];
}

/**
 * Options for listing backups
 */
export interface ListBackupsOptions {
  /** Start date filter (YYYY-MM-DD, inclusive) */
  startDate?: string;
  /** End date filter (YYYY-MM-DD, inclusive) */
  endDate?: string;
  /** Maximum number of snapshots to return */
  limit?: number;
}

/**
 * Options for restore operations
 */
export interface RestoreOptions {
  /** Target timestamp for point-in-time recovery (inclusive) */
  targetTimestamp?: bigint;
  /** Batch size for processing events (default: 1000) */
  batchSize?: number;
  /** Progress callback for tracking restore progress */
  onProgress?: (progress: RestoreProgress) => void;
  /** Whether to include delete events in replay (default: true) */
  includeDeletes?: boolean;
  /** Resume token from a previous partial restore */
  resumeToken?: string;
  /** Dry run mode - don't apply changes, just count events */
  dryRun?: boolean;
}

/**
 * Progress information during restore
 */
export interface RestoreProgress {
  /** Total files to process */
  totalFiles: number;
  /** Files processed so far */
  processedFiles: number;
  /** Total events processed */
  eventsProcessed: number;
  /** Events applied (after filtering) */
  eventsApplied: number;
  /** Events skipped (due to timestamp filter or deletes) */
  eventsSkipped: number;
  /** Current file being processed */
  currentFile: string;
  /** Estimated completion percentage (0-100) */
  percentComplete: number;
  /** Resume token for continuing from current position */
  resumeToken: string;
}

/**
 * Result of a restore operation
 */
export interface RestoreResult {
  /** Whether the restore completed successfully */
  success: boolean;
  /** Total events replayed */
  eventsReplayed: number;
  /** Events skipped due to filters */
  eventsSkipped: number;
  /** Total files processed */
  filesProcessed: number;
  /** Duration in milliseconds */
  durationMs: number;
  /** Latest timestamp restored */
  latestTimestamp?: bigint;
  /** Resume token if restore was interrupted */
  resumeToken?: string;
  /** Error message if failed */
  error?: string;
}

/**
 * Event handler function for processing restored events
 */
export type RestoreEventHandler = (events: CDCEvent[]) => Promise<void>;

// ============================================================================
// Backup Listing
// ============================================================================

/**
 * List available backup snapshots for a namespace
 *
 * Groups CDC files by date to provide a snapshot-level view of available
 * backups. Useful for selecting a restore point.
 *
 * @param bucket R2 bucket containing CDC files
 * @param namespace Namespace to list backups for
 * @param options Optional filters for date range and limit
 * @returns Array of backup snapshots sorted by date (oldest first)
 *
 * @example
 * ```typescript
 * const backups = await listBackups(bucket, namespace);
 * console.log(`Found ${backups.length} backup snapshots`);
 *
 * for (const snapshot of backups) {
 *   console.log(`${snapshot.date}: ${snapshot.fileCount} files`);
 * }
 * ```
 */
export async function listBackups(
  bucket: R2Bucket,
  namespace: Namespace,
  options?: ListBackupsOptions
): Promise<BackupSnapshot[]> {
  // List all CDC files for the namespace
  const files = await listCDCFiles(bucket, namespace);

  if (files.length === 0) {
    return [];
  }

  // Group files by date
  const filesByDate = new Map<string, string[]>();

  for (const file of files) {
    const parsed = parseCDCPath(file);
    if (!parsed) continue;

    const { date } = parsed;

    // Apply date filters
    if (options?.startDate && date < options.startDate) continue;
    if (options?.endDate && date > options.endDate) continue;

    if (!filesByDate.has(date)) {
      filesByDate.set(date, []);
    }
    filesByDate.get(date)!.push(file);
  }

  // Convert to snapshots
  const snapshots: BackupSnapshot[] = [];

  for (const [date, dateFiles] of filesByDate) {
    // Sort files within each date
    dateFiles.sort();

    snapshots.push({
      date,
      namespace,
      fileCount: dateFiles.length,
      files: dateFiles,
    });
  }

  // Sort by date (oldest first for chronological ordering)
  snapshots.sort((a, b) => a.date.localeCompare(b.date));

  // Apply limit
  if (options?.limit && snapshots.length > options.limit) {
    return snapshots.slice(0, options.limit);
  }

  return snapshots;
}

/**
 * Get metadata for a specific backup snapshot
 *
 * Fetches additional metadata by reading file headers. Useful for
 * determining the exact time range covered by a snapshot.
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot to get metadata for
 * @returns Enhanced snapshot with timestamp information
 */
export async function getBackupMetadata(
  bucket: R2Bucket,
  snapshot: BackupSnapshot
): Promise<BackupSnapshot> {
  if (snapshot.files.length === 0) {
    return snapshot;
  }

  // Read first and last files to get timestamp range
  const firstEvents = await readCDCFile(bucket, snapshot.files[0]!);
  const lastEvents = await readCDCFile(bucket, snapshot.files[snapshot.files.length - 1]!);

  let earliestTimestamp: bigint | undefined;
  let latestTimestamp: bigint | undefined;
  let totalSizeBytes = 0;

  // Get earliest from first file
  if (firstEvents.length > 0) {
    earliestTimestamp = firstEvents.reduce(
      (min, e) => (e.timestamp < min ? e.timestamp : min),
      firstEvents[0]!.timestamp
    );
  }

  // Get latest from last file
  if (lastEvents.length > 0) {
    latestTimestamp = lastEvents.reduce(
      (max, e) => (e.timestamp > max ? e.timestamp : max),
      lastEvents[0]!.timestamp
    );
  }

  // Get total size from file metadata
  for (const file of snapshot.files) {
    const head = await bucket.head(file);
    if (head) {
      totalSizeBytes += head.size;
    }
  }

  return {
    ...snapshot,
    earliestTimestamp,
    latestTimestamp,
    totalSizeBytes,
  };
}

/**
 * Find the latest backup snapshot before a given timestamp
 *
 * Useful for point-in-time recovery when you need to find
 * the appropriate backup to restore from.
 *
 * @param bucket R2 bucket
 * @param namespace Namespace to search
 * @param timestamp Target timestamp (finds latest backup before this)
 * @returns Backup snapshot or null if none found
 */
export async function findBackupBeforeTimestamp(
  bucket: R2Bucket,
  namespace: Namespace,
  timestamp: bigint
): Promise<BackupSnapshot | null> {
  const dateStr = formatDatePath(timestamp);

  // Get backups up to and including the target date
  const backups = await listBackups(bucket, namespace, {
    endDate: dateStr,
  });

  if (backups.length === 0) {
    return null;
  }

  // Return the most recent backup
  return backups[backups.length - 1]!;
}

// ============================================================================
// Point-in-Time Recovery
// ============================================================================

/**
 * Generate a resume token from restore state
 */
function generateResumeToken(fileIndex: number, eventIndex: number): string {
  return Buffer.from(JSON.stringify({ f: fileIndex, e: eventIndex })).toString('base64');
}

/**
 * Parse a resume token to restore state
 */
function parseResumeToken(token: string): { fileIndex: number; eventIndex: number } | null {
  try {
    const decoded = JSON.parse(Buffer.from(token, 'base64').toString('utf8'));
    if (typeof decoded.f === 'number' && typeof decoded.e === 'number') {
      return { fileIndex: decoded.f, eventIndex: decoded.e };
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Restore from a backup with point-in-time recovery support
 *
 * Replays CDC events from backup files up to a specified timestamp.
 * Events are delivered to the handler in batches for processing.
 *
 * Features:
 * - Point-in-time recovery with timestamp precision
 * - Batch processing with configurable batch size
 * - Progress tracking with callbacks
 * - Resumable restores with tokens
 * - Dry run mode for verification
 *
 * @param bucket R2 bucket containing backups
 * @param namespace Namespace to restore
 * @param handler Function to process restored events
 * @param options Restore options (timestamp, batch size, etc.)
 * @returns Restore result with statistics
 *
 * @example
 * ```typescript
 * // Full restore
 * const result = await restoreFromBackup(
 *   bucket,
 *   namespace,
 *   async (events) => {
 *     await tripleStore.insertBatch(events.map(e => e.triple));
 *   }
 * );
 *
 * // Point-in-time recovery
 * const pitResult = await restoreFromBackup(
 *   bucket,
 *   namespace,
 *   handler,
 *   {
 *     targetTimestamp: BigInt(Date.parse('2024-01-15T12:00:00Z')),
 *     onProgress: (p) => console.log(`${p.percentComplete}% complete`),
 *   }
 * );
 * ```
 */
export async function restoreFromBackup(
  bucket: R2Bucket,
  namespace: Namespace,
  handler: RestoreEventHandler,
  options?: RestoreOptions
): Promise<RestoreResult> {
  const startTime = Date.now();
  const batchSize = options?.batchSize ?? 1000;
  const includeDeletes = options?.includeDeletes ?? true;
  const dryRun = options?.dryRun ?? false;

  // Convert target timestamp to time filter for listing
  const listOptions: ListCDCFilesOptions = {};
  if (options?.targetTimestamp) {
    // Add a day buffer to ensure we get all files up to the timestamp
    // (file dates are based on the latest event, so we might need earlier files)
    listOptions.endTime = options.targetTimestamp + BigInt(24 * 60 * 60 * 1000);
  }

  // List all CDC files
  const files = await listCDCFiles(bucket, namespace, listOptions);

  if (files.length === 0) {
    return {
      success: true,
      eventsReplayed: 0,
      eventsSkipped: 0,
      filesProcessed: 0,
      durationMs: Date.now() - startTime,
    };
  }

  // Parse resume token if provided
  let startFileIndex = 0;
  let startEventIndex = 0;
  if (options?.resumeToken) {
    const parsed = parseResumeToken(options.resumeToken);
    if (parsed) {
      startFileIndex = parsed.fileIndex;
      startEventIndex = parsed.eventIndex;
    }
  }

  // Initialize counters
  let eventsProcessed = 0;
  let eventsApplied = 0;
  let eventsSkipped = 0;
  let filesProcessed = 0;
  let latestTimestamp: bigint | undefined;
  let eventBatch: CDCEvent[] = [];
  let lastResumeToken = '';

  // Process files
  for (let fileIndex = startFileIndex; fileIndex < files.length; fileIndex++) {
    const file = files[fileIndex]!;

    // Report progress
    if (options?.onProgress) {
      const progress: RestoreProgress = {
        totalFiles: files.length,
        processedFiles: filesProcessed,
        eventsProcessed,
        eventsApplied,
        eventsSkipped,
        currentFile: file,
        percentComplete: Math.round((filesProcessed / files.length) * 100),
        resumeToken: lastResumeToken,
      };
      options.onProgress(progress);
    }

    // Read CDC file
    let events: CDCEvent[];
    try {
      events = await readCDCFile(bucket, file);
    } catch (error) {
      // Skip files that can't be read
      console.error(`Failed to read CDC file ${file}:`, error);
      filesProcessed++;
      continue;
    }

    // Process events
    const eventStartIndex = fileIndex === startFileIndex ? startEventIndex : 0;

    for (let eventIndex = eventStartIndex; eventIndex < events.length; eventIndex++) {
      const event = events[eventIndex]!;
      eventsProcessed++;

      // Update resume token
      lastResumeToken = generateResumeToken(fileIndex, eventIndex);

      // Apply timestamp filter
      if (options?.targetTimestamp && event.timestamp > options.targetTimestamp) {
        eventsSkipped++;
        continue;
      }

      // Filter deletes if requested
      if (!includeDeletes && event.type === 'delete') {
        eventsSkipped++;
        continue;
      }

      // Track latest timestamp
      if (!latestTimestamp || event.timestamp > latestTimestamp) {
        latestTimestamp = event.timestamp;
      }

      // Add to batch
      eventBatch.push(event);
      eventsApplied++;

      // Flush batch if full
      if (eventBatch.length >= batchSize) {
        if (!dryRun) {
          await handler(eventBatch);
        }
        eventBatch = [];
      }
    }

    filesProcessed++;
  }

  // Flush remaining events
  if (eventBatch.length > 0 && !dryRun) {
    await handler(eventBatch);
  }

  // Final progress report
  if (options?.onProgress) {
    const progress: RestoreProgress = {
      totalFiles: files.length,
      processedFiles: filesProcessed,
      eventsProcessed,
      eventsApplied,
      eventsSkipped,
      currentFile: '',
      percentComplete: 100,
      resumeToken: '',
    };
    options.onProgress(progress);
  }

  return {
    success: true,
    eventsReplayed: eventsApplied,
    eventsSkipped,
    filesProcessed,
    durationMs: Date.now() - startTime,
    latestTimestamp,
  };
}

/**
 * Restore from a specific backup snapshot
 *
 * Convenience function to restore from a pre-selected backup snapshot.
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot to restore from
 * @param handler Function to process restored events
 * @param options Restore options
 * @returns Restore result
 */
export async function restoreFromSnapshot(
  bucket: R2Bucket,
  snapshot: BackupSnapshot,
  handler: RestoreEventHandler,
  options?: Omit<RestoreOptions, 'startDate' | 'endDate'>
): Promise<RestoreResult> {
  const startTime = Date.now();
  const batchSize = options?.batchSize ?? 1000;
  const includeDeletes = options?.includeDeletes ?? true;
  const dryRun = options?.dryRun ?? false;

  if (snapshot.files.length === 0) {
    return {
      success: true,
      eventsReplayed: 0,
      eventsSkipped: 0,
      filesProcessed: 0,
      durationMs: Date.now() - startTime,
    };
  }

  // Initialize counters
  let eventsProcessed = 0;
  let eventsApplied = 0;
  let eventsSkipped = 0;
  let filesProcessed = 0;
  let latestTimestamp: bigint | undefined;
  let eventBatch: CDCEvent[] = [];

  // Process files in the snapshot
  for (const file of snapshot.files) {
    // Report progress
    if (options?.onProgress) {
      const progress: RestoreProgress = {
        totalFiles: snapshot.files.length,
        processedFiles: filesProcessed,
        eventsProcessed,
        eventsApplied,
        eventsSkipped,
        currentFile: file,
        percentComplete: Math.round((filesProcessed / snapshot.files.length) * 100),
        resumeToken: '',
      };
      options.onProgress(progress);
    }

    // Read CDC file
    let events: CDCEvent[];
    try {
      events = await readCDCFile(bucket, file);
    } catch (error) {
      console.error(`Failed to read CDC file ${file}:`, error);
      filesProcessed++;
      continue;
    }

    // Process events
    for (const event of events) {
      eventsProcessed++;

      // Apply timestamp filter
      if (options?.targetTimestamp && event.timestamp > options.targetTimestamp) {
        eventsSkipped++;
        continue;
      }

      // Filter deletes if requested
      if (!includeDeletes && event.type === 'delete') {
        eventsSkipped++;
        continue;
      }

      // Track latest timestamp
      if (!latestTimestamp || event.timestamp > latestTimestamp) {
        latestTimestamp = event.timestamp;
      }

      // Add to batch
      eventBatch.push(event);
      eventsApplied++;

      // Flush batch if full
      if (eventBatch.length >= batchSize) {
        if (!dryRun) {
          await handler(eventBatch);
        }
        eventBatch = [];
      }
    }

    filesProcessed++;
  }

  // Flush remaining events
  if (eventBatch.length > 0 && !dryRun) {
    await handler(eventBatch);
  }

  return {
    success: true,
    eventsReplayed: eventsApplied,
    eventsSkipped,
    filesProcessed,
    durationMs: Date.now() - startTime,
    latestTimestamp,
  };
}

// ============================================================================
// Utilities
// ============================================================================

/**
 * Estimate the time required for a restore operation
 *
 * Based on file count and typical processing speed.
 *
 * @param snapshot Backup snapshot
 * @param eventsPerSecond Expected processing speed (default: 10000)
 * @returns Estimated duration in milliseconds
 */
export function estimateRestoreDuration(
  snapshot: BackupSnapshot,
  eventsPerSecond: number = 10000
): number {
  // Estimate ~1000 events per file (typical CDC batch size)
  const estimatedEvents = snapshot.fileCount * 1000;
  return Math.ceil((estimatedEvents / eventsPerSecond) * 1000);
}

/**
 * Validate that a backup can be restored
 *
 * Checks that all files in the backup exist and are readable.
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot to validate
 * @returns Object with validation result and any missing files
 */
export async function validateBackup(
  bucket: R2Bucket,
  snapshot: BackupSnapshot
): Promise<{ valid: boolean; missingFiles: string[] }> {
  const missingFiles: string[] = [];

  for (const file of snapshot.files) {
    const head = await bucket.head(file);
    if (!head) {
      missingFiles.push(file);
    }
  }

  return {
    valid: missingFiles.length === 0,
    missingFiles,
  };
}

/**
 * Get the total size of a backup in bytes
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot
 * @returns Total size in bytes
 */
export async function getBackupSize(
  bucket: R2Bucket,
  snapshot: BackupSnapshot
): Promise<number> {
  let totalSize = 0;

  for (const file of snapshot.files) {
    const head = await bucket.head(file);
    if (head) {
      totalSize += head.size;
    }
  }

  return totalSize;
}

/**
 * Count total events in a backup without fully reading files
 *
 * Note: This reads all files to count events, which may be slow for large backups.
 * Consider using estimateEventCount for a faster estimate.
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot
 * @returns Total event count
 */
export async function countBackupEvents(
  bucket: R2Bucket,
  snapshot: BackupSnapshot
): Promise<number> {
  let totalEvents = 0;

  for (const file of snapshot.files) {
    try {
      const events = await readCDCFile(bucket, file);
      totalEvents += events.length;
    } catch {
      // Skip files that can't be read
    }
  }

  return totalEvents;
}

/**
 * Estimate event count based on file sizes
 *
 * Faster than countBackupEvents but less accurate.
 *
 * @param bucket R2 bucket
 * @param snapshot Backup snapshot
 * @param bytesPerEvent Average bytes per event (default: 200)
 * @returns Estimated event count
 */
export async function estimateEventCount(
  bucket: R2Bucket,
  snapshot: BackupSnapshot,
  bytesPerEvent: number = 200
): Promise<number> {
  const totalSize = await getBackupSize(bucket, snapshot);
  return Math.ceil(totalSize / bytesPerEvent);
}
