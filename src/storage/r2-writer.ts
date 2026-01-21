/**
 * R2 CDC Writer
 *
 * Writes CDC (Change Data Capture) events to R2 storage in GraphCol format.
 * Supports batching, interval-based flushing, and URL hierarchy path generation.
 *
 * Path format: {tld}/{domain}/{path}/_wal/{date}/{sequence}.gcol
 * Example: .com/.example/crm/acme/_wal/2024-01-16/001.gcol
 *
 * @packageDocumentation
 */

import type { CDCEvent } from './cdc-types';
import type { Namespace } from '../core/types';
import type { Triple } from '../core/triple';
import { encodeGraphCol, decodeGraphCol } from './graphcol';

// ============================================================================
// Types
// ============================================================================

/**
 * R2Writer configuration options
 */
export interface R2WriterConfig {
  /** R2 bucket to write to */
  bucket: R2Bucket;
  /** Namespace URL for the CDC events */
  namespace: Namespace;
  /** Flush interval in milliseconds (default: 100ms) */
  flushIntervalMs?: number;
  /** Maximum batch size before auto-flush (default: 1000 events) */
  maxBatchSize?: number;
  /** Maximum retry attempts for transient failures (default: 3) */
  maxRetries?: number;
  /** Base backoff delay in milliseconds for retries (default: 100ms) */
  retryBackoffMs?: number;
  /** Error callback for permanent failures */
  onError?: (event: R2WriterErrorEvent) => void;
}

/**
 * Error event emitted on permanent flush failure
 */
export interface R2WriterErrorEvent {
  /** The error that caused the failure */
  error: Error;
  /** Number of events that failed to flush */
  eventCount: number;
  /** Number of retry attempts made */
  attempts: number;
  /** Namespace of the writer */
  namespace: Namespace;
  /** Timestamp of the failure */
  timestamp: Date;
}

/**
 * R2Writer statistics
 */
export interface R2WriterStats {
  /** Total number of events written */
  eventsWritten: number;
  /** Total bytes written to R2 */
  bytesWritten: number;
  /** Number of flush operations */
  flushCount: number;
}

/**
 * R2Writer interface for streaming CDC events to R2
 */
export interface R2Writer {
  /** Write events to the buffer (may trigger auto-flush) */
  write(events: CDCEvent[]): Promise<void>;
  /** Force flush all buffered events to R2 */
  flush(): Promise<void>;
  /** Get current statistics */
  getStats(): R2WriterStats;
  /** Get count of events pending in buffer */
  getPendingEventCount(): number;
  /** Close the writer and stop interval timer */
  close(): void;
}

/**
 * Options for listing CDC files
 */
export interface ListCDCFilesOptions {
  /** Start timestamp (inclusive) */
  startTime?: bigint;
  /** End timestamp (exclusive) */
  endTime?: bigint;
}

// ============================================================================
// Path Generation
// ============================================================================

/**
 * Parse a URL namespace into path components for R2 hierarchy
 *
 * Converts: https://example.com/crm/acme
 * To: .com/.example/crm/acme
 *
 * The hierarchy is reversed for better R2 list performance:
 * - TLD first (.com)
 * - Domain reversed (.example)
 * - Path segments follow
 */
export function parseNamespaceToPath(namespace: Namespace): string {
  const url = new URL(namespace);

  // Get domain parts and reverse them
  const domainParts = url.hostname.split('.');
  const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');

  // Get path parts (remove leading slash, keep empty check)
  const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
  const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';

  return reversedDomain + pathStr;
}

/**
 * Format a timestamp as a date string for path
 *
 * @param timestamp Milliseconds since epoch (bigint)
 * @returns Date string in YYYY-MM-DD format
 */
export function formatDatePath(timestamp: bigint): string {
  const date = new Date(Number(timestamp));
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

/**
 * Generate sequence number for CDC file
 *
 * Uses timestamp to generate a sortable sequence:
 * - Higher precision than date alone
 * - Allows multiple files per day
 * - Includes millisecond component for uniqueness
 *
 * @param timestamp Milliseconds since epoch
 * @returns Sequence string with time components for uniqueness
 */
export function generateSequence(timestamp: bigint): string {
  const date = new Date(Number(timestamp));
  // Format: HHMMSS-mmm (hours, minutes, seconds, milliseconds)
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const mins = String(date.getUTCMinutes()).padStart(2, '0');
  const secs = String(date.getUTCSeconds()).padStart(2, '0');
  const ms = String(date.getUTCMilliseconds()).padStart(3, '0');
  return `${hours}${mins}${secs}-${ms}`;
}

/**
 * Generate the full CDC path for a namespace and timestamp
 *
 * Format: {namespace_path}/_wal/{date}/{sequence}.gcol
 * Example: .com/.example/crm/acme/_wal/2024-01-16/001.gcol
 *
 * @param namespace Namespace URL
 * @param timestamp Timestamp for the CDC file
 * @returns Full R2 key path
 */
export function getCDCPath(namespace: Namespace, timestamp: bigint): string {
  const namespacePath = parseNamespaceToPath(namespace);
  const datePath = formatDatePath(timestamp);
  const sequence = generateSequence(timestamp);

  return `${namespacePath}/_wal/${datePath}/${sequence}.gcol`;
}

/**
 * Parse a CDC path back to its components
 *
 * @param path R2 key path
 * @returns Parsed components or null if invalid
 */
export function parseCDCPath(path: string): { date: string; sequence: string } | null {
  // Match both old format (3-digit sequence) and new format (HHMMSS-mmm)
  const walMatch = path.match(/\/_wal\/(\d{4}-\d{2}-\d{2})\/(\d{6}-\d{3}|\d{3})\.gcol$/);
  if (!walMatch) {
    return null;
  }
  return {
    date: walMatch[1]!,
    sequence: walMatch[2]!,
  };
}

// ============================================================================
// R2Writer Implementation
// ============================================================================

/**
 * Default configuration values
 */
const DEFAULT_FLUSH_INTERVAL_MS = 100;
const DEFAULT_MAX_BATCH_SIZE = 1000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_BACKOFF_MS = 100;

/**
 * Sleep utility for exponential backoff
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create an R2Writer for streaming CDC events
 *
 * Features:
 * - Batches events for efficient R2 writes
 * - Auto-flushes on interval or batch size
 * - Encodes events in GraphCol format
 * - Tracks write statistics
 * - Retries with exponential backoff on transient failures
 * - Emits error events on permanent failures
 * - Preserves data in buffer on failure for later retry
 *
 * @param config Writer configuration
 * @returns R2Writer instance
 */
export function createR2Writer(config: R2WriterConfig): R2Writer {
  const {
    bucket,
    namespace,
    flushIntervalMs = DEFAULT_FLUSH_INTERVAL_MS,
    maxBatchSize = DEFAULT_MAX_BATCH_SIZE,
    maxRetries = DEFAULT_MAX_RETRIES,
    retryBackoffMs = DEFAULT_RETRY_BACKOFF_MS,
    onError,
  } = config;

  // Internal state
  let eventBuffer: CDCEvent[] = [];
  let stats: R2WriterStats = {
    eventsWritten: 0,
    bytesWritten: 0,
    flushCount: 0,
  };
  let flushTimer: ReturnType<typeof setInterval> | null = null;
  let isClosed = false;

  /**
   * Convert CDC events to triples for GraphCol encoding
   */
  function eventsToTriples(events: CDCEvent[]): Triple[] {
    return events.map((event) => event.triple);
  }

  /**
   * Emit an error event
   */
  function emitError(error: Error, eventCount: number, attempts: number): void {
    const event: R2WriterErrorEvent = {
      error,
      eventCount,
      attempts,
      namespace,
      timestamp: new Date(),
    };

    if (onError) {
      onError(event);
    }
  }

  /**
   * Log structured error with context
   */
  function logError(message: string, error: Error, context: Record<string, unknown>): void {
    console.error(
      `R2Writer flush error: ${message}`,
      JSON.stringify({
        namespace,
        ...context,
        error: error.message,
      })
    );
  }

  /**
   * Internal flush implementation with retry logic
   * @param throwOnError Whether to throw on error (true for explicit flush, false for interval)
   */
  async function doFlush(throwOnError: boolean = false): Promise<void> {
    if (eventBuffer.length === 0 || isClosed) {
      return;
    }

    // Get current buffer but DON'T clear it yet - preserve for retry
    const eventsToFlush = [...eventBuffer];
    const eventCount = eventsToFlush.length;

    // Find the latest timestamp for path generation
    // eventsToFlush is guaranteed to be non-empty at this point (we return early if empty)
    const maxTimestamp = eventsToFlush.reduce(
      (max, e) => (e.timestamp > max ? e.timestamp : max),
      eventsToFlush[0]!.timestamp
    );

    // Convert to triples and encode
    const triples = eventsToTriples(eventsToFlush);
    const encoded = encodeGraphCol(triples, namespace);

    // Generate path
    const path = getCDCPath(namespace, maxTimestamp);

    // Retry loop with exponential backoff
    let lastError: Error | null = null;
    const totalAttempts = maxRetries + 1; // Initial attempt + retries

    for (let attempt = 1; attempt <= totalAttempts; attempt++) {
      try {
        // Attempt to write to R2
        await bucket.put(path, encoded);

        // Success! Clear the buffer and update stats
        eventBuffer = eventBuffer.slice(eventCount);
        stats.eventsWritten += eventCount;
        stats.bytesWritten += encoded.length;
        stats.flushCount++;

        return; // Success - exit the function
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));

        // Log retry attempt
        if (attempt < totalAttempts) {
          const backoffMs = retryBackoffMs * Math.pow(2, attempt - 1);
          console.error(
            `R2Writer flush retry ${attempt}/${totalAttempts}:`,
            JSON.stringify({
              namespace,
              eventCount,
              error: lastError.message,
              nextRetryMs: backoffMs,
            })
          );

          // Wait before retrying (exponential backoff)
          await sleep(backoffMs);
        }
      }
    }

    // All retries exhausted - emit error event and log
    if (lastError) {
      emitError(lastError, eventCount, totalAttempts);
      logError('All retries exhausted', lastError, {
        eventCount,
        attempts: totalAttempts,
        path,
      });

      // Data is preserved in buffer for potential later retry

      // If this was an explicit flush call, throw the error
      if (throwOnError) {
        throw lastError;
      }
    }
  }

  /**
   * Start the flush interval timer
   */
  function startFlushTimer(): void {
    if (flushTimer === null && flushIntervalMs > 0) {
      flushTimer = setInterval(async () => {
        try {
          // Interval flush does NOT throw - errors are logged and data preserved
          await doFlush(false);
        } catch (error) {
          // This shouldn't happen since doFlush(false) doesn't throw,
          // but log just in case
          console.error('R2Writer flush error:', error);
        }
      }, flushIntervalMs);
    }
  }

  /**
   * Stop the flush interval timer
   */
  function stopFlushTimer(): void {
    if (flushTimer !== null) {
      clearInterval(flushTimer);
      flushTimer = null;
    }
  }

  // Start the timer
  startFlushTimer();

  return {
    async write(events: CDCEvent[]): Promise<void> {
      if (isClosed) {
        throw new Error('R2Writer is closed');
      }

      if (events.length === 0) {
        return;
      }

      // Add events to buffer
      eventBuffer.push(...events);

      // Check if we should auto-flush due to batch size
      if (eventBuffer.length >= maxBatchSize) {
        await doFlush(true); // Auto-flush throws errors
      }
    },

    async flush(): Promise<void> {
      if (isClosed) {
        throw new Error('R2Writer is closed');
      }
      // Explicit flush throws errors to the caller
      await doFlush(true);
    },

    getStats(): R2WriterStats {
      return { ...stats };
    },

    getPendingEventCount(): number {
      return eventBuffer.length;
    },

    close(): void {
      if (!isClosed) {
        isClosed = true;
        stopFlushTimer();
        // Don't flush on close - caller should flush explicitly if needed
      }
    },
  };
}

// ============================================================================
// CDC File Operations
// ============================================================================

/**
 * List CDC files for a namespace within a time range
 *
 * @param bucket R2 bucket
 * @param namespace Namespace to list files for
 * @param options Optional time range filter
 * @returns Array of CDC file paths
 */
export async function listCDCFiles(
  bucket: R2Bucket,
  namespace: Namespace,
  options?: ListCDCFilesOptions
): Promise<string[]> {
  const namespacePath = parseNamespaceToPath(namespace);
  const walPrefix = `${namespacePath}/_wal/`;

  // List all objects with the WAL prefix
  const listed = await bucket.list({ prefix: walPrefix });

  // Filter to .gcol files
  let paths = listed.objects
    .filter((obj) => obj.key.endsWith('.gcol'))
    .map((obj) => obj.key);

  // Apply time range filter if provided
  if (options?.startTime !== undefined || options?.endTime !== undefined) {
    const startDate = options.startTime
      ? formatDatePath(options.startTime)
      : '0000-00-00';
    const endDate = options.endTime
      ? formatDatePath(options.endTime)
      : '9999-99-99';

    paths = paths.filter((path) => {
      const parsed = parseCDCPath(path);
      if (!parsed) return false;

      // startTime is inclusive, endTime is exclusive
      return parsed.date >= startDate && parsed.date < endDate;
    });
  }

  // Sort by path (which sorts by date and sequence)
  return paths.sort();
}

/**
 * Read a CDC file and decode its events
 *
 * @param bucket R2 bucket
 * @param path Full R2 key path to the CDC file
 * @returns Array of CDC events
 */
export async function readCDCFile(
  bucket: R2Bucket,
  path: string
): Promise<CDCEvent[]> {
  const object = await bucket.get(path);
  if (!object) {
    throw new Error(`CDC file not found: ${path}`);
  }

  const data = new Uint8Array(await object.arrayBuffer());
  const triples = decodeGraphCol(data);

  // Convert triples back to CDC events
  // Note: We reconstruct as 'insert' events since we don't store event type in GraphCol
  // For full CDC fidelity, a separate metadata file or header extension would be needed
  return triples.map((triple) => ({
    type: 'insert' as const,
    triple,
    timestamp: triple.timestamp,
  }));
}

/**
 * Delete a CDC file
 *
 * @param bucket R2 bucket
 * @param path Full R2 key path to delete
 */
export async function deleteCDCFile(
  bucket: R2Bucket,
  path: string
): Promise<void> {
  await bucket.delete(path);
}

/**
 * Get metadata about a CDC file without reading its contents
 *
 * @param bucket R2 bucket
 * @param path Full R2 key path
 * @returns Object metadata or null if not found
 */
export async function getCDCFileMetadata(
  bucket: R2Bucket,
  path: string
): Promise<{ size: number; etag: string; uploaded: Date } | null> {
  const head = await bucket.head(path);
  if (!head) {
    return null;
  }

  return {
    size: head.size,
    etag: head.etag,
    uploaded: head.uploaded,
  };
}
