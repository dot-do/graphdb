/**
 * Streaming Import Module for GraphDB
 *
 * Memory-efficient data ingestion utilities for large-scale imports.
 *
 * Components:
 * - StreamingLineReader: Process chunks without loading full text
 * - BatchedTripleWriter: Batch 10K triples before flush
 * - ResumableImportState: Checkpoint to DO storage
 * - RangeFetcher: HTTP Range requests for large files
 *
 * @example
 * ```typescript
 * import {
 *   createStreamingLineReader,
 *   createBatchedTripleWriter,
 *   createResumableImportState,
 * } from '@dotdo/graphdb/import';
 *
 * // Create components for streaming import
 * const reader = createStreamingLineReader({ bufferSize: 1024 * 1024 });
 * const writer = createBatchedTripleWriter({ batchSize: 10000 });
 * const state = createResumableImportState(storage);
 *
 * // Process file in chunks
 * for await (const chunk of fetchChunks(url)) {
 *   const lines = reader.processChunk(chunk);
 *   for (const line of lines) {
 *     const triple = parseTriple(line);
 *     await writer.write(triple);
 *   }
 *   await state.checkpoint(reader.getState());
 * }
 * ```
 *
 * @packageDocumentation
 */

// StreamingLineReader
export {
  /**
   * Create a streaming line reader for memory-efficient text processing.
   *
   * Processes data chunks without loading entire files into memory.
   * Handles partial lines across chunk boundaries automatically.
   *
   * @param options - Configuration options
   * @returns StreamingLineReader instance
   *
   * @example
   * ```typescript
   * const reader = createStreamingLineReader({
   *   bufferSize: 64 * 1024, // 64KB buffer
   *   encoding: 'utf-8',
   * });
   *
   * // Process chunks from a stream
   * for await (const chunk of stream) {
   *   const lines = reader.processChunk(chunk);
   *   for (const line of lines) {
   *     console.log(line);
   *   }
   * }
   *
   * // Flush remaining data at end
   * const remaining = reader.flush();
   * ```
   */
  createStreamingLineReader,
  type StreamingLineReader,
  type StreamingLineReaderOptions,
  type LineReaderState,
} from './streaming-reader';

// BatchedTripleWriter
export {
  /**
   * Create a batched triple writer for efficient bulk writes.
   *
   * Buffers triples and flushes them in batches to reduce write overhead.
   * Typically batches 10K triples before flushing.
   *
   * @param options - Configuration options
   * @returns BatchedTripleWriter instance
   *
   * @example
   * ```typescript
   * const writer = createBatchedTripleWriter({
   *   batchSize: 10000,
   *   flushInterval: 5000, // Auto-flush every 5 seconds
   *   onFlush: async (triples) => {
   *     await shardStub.fetch('/write', {
   *       method: 'POST',
   *       body: JSON.stringify({ triples }),
   *     });
   *   },
   * });
   *
   * // Write triples (auto-batched)
   * await writer.write(triple1);
   * await writer.write(triple2);
   *
   * // Force flush at end
   * await writer.flush();
   * ```
   */
  createBatchedTripleWriter,
  type BatchedTripleWriter,
  type BatchedTripleWriterOptions,
  type BatchWriterState,
  type WriterResult,
  type ImportChunkInfo,
} from './batched-writer';

// ResumableImportState
export {
  /**
   * Create a resumable import state manager for checkpoint-based imports.
   *
   * Persists progress to Durable Object storage, enabling recovery
   * from failures without re-processing already imported data.
   *
   * @param storage - Durable Object storage instance
   * @returns ResumableImportState instance
   *
   * @example
   * ```typescript
   * const state = createResumableImportState(ctx.storage);
   *
   * // Check for existing checkpoint
   * const checkpoint = await state.load();
   * if (checkpoint) {
   *   console.log(`Resuming from offset ${checkpoint.byteOffset}`);
   * }
   *
   * // Save checkpoint during import
   * await state.checkpoint({
   *   byteOffset: currentOffset,
   *   linesProcessed: lineCount,
   *   triplesWritten: tripleCount,
   *   timestamp: Date.now(),
   * });
   *
   * // Clear on completion
   * await state.clear();
   * ```
   */
  createResumableImportState,
  type ResumableImportState,
  type ImportCheckpoint,
} from './resumable-state';

// RangeFetcher
export {
  /**
   * Create a range fetcher for HTTP Range requests on large files.
   *
   * Enables partial downloads for memory-efficient processing
   * of files too large to load entirely.
   *
   * @param options - Configuration options
   * @returns RangeFetcher instance
   *
   * @example
   * ```typescript
   * const fetcher = createRangeFetcher({
   *   chunkSize: 10 * 1024 * 1024, // 10MB chunks
   *   retries: 3,
   *   retryDelay: 1000,
   * });
   *
   * // Fetch file metadata
   * const info = await fetcher.getInfo(url);
   * console.log(`File size: ${info.contentLength} bytes`);
   *
   * // Fetch range
   * const result = await fetcher.fetchRange(url, {
   *   start: 0,
   *   end: 1024 * 1024,
   * });
   * console.log(`Fetched ${result.data.byteLength} bytes`);
   * ```
   */
  createRangeFetcher,
  type RangeFetcher,
  type RangeFetcherOptions,
  type RangeFetchResult,
} from './range-fetcher';
