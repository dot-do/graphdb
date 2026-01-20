/**
 * RangeFetcher - HTTP Range requests for memory-efficient large file downloads
 *
 * Key features:
 * - Uses HTTP Range requests for incremental downloading
 * - Configurable chunk size (default 10MB)
 * - Exponential backoff retry on failures
 * - Yields chunks as async generator
 *
 * @packageDocumentation
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Result from a range fetch operation
 */
export interface RangeFetchResult {
  /** The fetched data chunk */
  data: Uint8Array;
  /** Start byte offset */
  start: number;
  /** End byte offset (exclusive) */
  end: number;
  /** Total size of the resource (if known) */
  totalSize?: number;
  /** Whether this is the last chunk */
  isLast: boolean;
}

/**
 * RangeFetcher interface for chunked HTTP downloads
 */
export interface RangeFetcher {
  /** Fetch a specific byte range */
  fetchRange(start: number, end?: number): Promise<RangeFetchResult>;
  /** Async generator that yields chunks from a starting offset */
  chunks(startOffset?: number): AsyncGenerator<RangeFetchResult>;
  /** Get total size of the resource (may require a HEAD request) */
  getTotalSize(): Promise<number | null>;
}

/**
 * Options for creating a RangeFetcher
 */
export interface RangeFetcherOptions {
  /** Chunk size in bytes (default 10MB) */
  chunkSize?: number;
  /** Maximum retry attempts (default 3) */
  maxRetries?: number;
  /** Base delay for exponential backoff in ms (default 1000) */
  baseDelayMs?: number;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create a range fetcher for chunked HTTP downloads
 *
 * Key features:
 * - Uses HTTP Range requests for incremental downloading
 * - Configurable chunk size (default 10MB, not 50MB for safety)
 * - Exponential backoff retry on failures
 * - Yields chunks as async generator
 *
 * @param url Source URL
 * @param options Configuration options
 * @returns RangeFetcher instance
 *
 * @example
 * ```typescript
 * const fetcher = createRangeFetcher(WIKTIONARY_URL, { chunkSize: 10 * 1024 * 1024 });
 *
 * // Resume from checkpoint
 * const checkpoint = await importState.loadCheckpoint('wiktionary');
 * const startOffset = checkpoint?.byteOffset ?? 0;
 *
 * for await (const { data, start, end, isLast } of fetcher.chunks(startOffset)) {
 *   for await (const line of lineReader.processChunk(data)) {
 *     // process line...
 *   }
 *
 *   // Save checkpoint after each chunk
 *   await importState.updateCheckpoint('wiktionary', { byteOffset: end });
 * }
 * ```
 */
export function createRangeFetcher(
  url: string,
  options?: RangeFetcherOptions
): RangeFetcher {
  const chunkSize = options?.chunkSize ?? 10 * 1024 * 1024; // 10MB default
  const maxRetries = options?.maxRetries ?? 3;
  const baseDelayMs = options?.baseDelayMs ?? 1000;

  let cachedTotalSize: number | null = null;

  async function fetchWithRetry(
    start: number,
    end?: number
  ): Promise<{ response: Response; actualEnd: number; totalSize?: number }> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        const requestEnd = end ?? start + chunkSize - 1;
        const rangeHeader = `bytes=${start}-${requestEnd}`;

        const response = await fetch(url, {
          headers: { Range: rangeHeader },
        });

        if (response.status === 206) {
          // Partial content - parse Content-Range header
          const contentRange = response.headers.get('Content-Range');
          let totalSize: number | undefined;
          let actualEnd = requestEnd;

          if (contentRange) {
            // Format: bytes start-end/total or bytes start-end/*
            const match = contentRange.match(/bytes (\d+)-(\d+)\/(\d+|\*)/);
            if (match && match[2] !== undefined) {
              actualEnd = parseInt(match[2], 10);
              if (match[3] !== undefined && match[3] !== '*') {
                totalSize = parseInt(match[3], 10);
                cachedTotalSize = totalSize;
              }
            }
          }

          return totalSize !== undefined
            ? { response, actualEnd, totalSize }
            : { response, actualEnd };
        } else if (response.status === 200) {
          // Server doesn't support range requests - return full response
          const contentLength = response.headers.get('Content-Length');
          const totalSize = contentLength ? parseInt(contentLength, 10) : undefined;
          if (totalSize !== undefined) cachedTotalSize = totalSize;

          const actualEnd = totalSize !== undefined ? totalSize - 1 : start + chunkSize - 1;
          return totalSize !== undefined
            ? { response, actualEnd, totalSize }
            : { response, actualEnd };
        } else if (response.status === 416) {
          // Range not satisfiable - we're past the end
          const totalSize = cachedTotalSize ?? undefined;
          return totalSize !== undefined
            ? { response: new Response(null, { status: 200 }), actualEnd: start, totalSize }
            : { response: new Response(null, { status: 200 }), actualEnd: start };
        } else {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < maxRetries - 1) {
          const delay = baseDelayMs * Math.pow(2, attempt);
          console.warn(`[RangeFetcher] Retry ${attempt + 1}/${maxRetries} after ${delay}ms: ${lastError.message}`);
          await new Promise((resolve) => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError ?? new Error('Failed to fetch range');
  }

  return {
    async fetchRange(start: number, end?: number): Promise<RangeFetchResult> {
      const requestEnd = end ?? start + chunkSize - 1;
      const { response, actualEnd, totalSize } = await fetchWithRetry(start, requestEnd);

      const data = new Uint8Array(await response.arrayBuffer());

      const result: RangeFetchResult = {
        data,
        start,
        end: actualEnd + 1, // Make end exclusive
        isLast: totalSize !== undefined && actualEnd >= totalSize - 1,
      };
      if (totalSize !== undefined) {
        result.totalSize = totalSize;
      }
      return result;
    },

    async *chunks(startOffset: number = 0): AsyncGenerator<RangeFetchResult> {
      let currentOffset = startOffset;

      while (true) {
        const requestEnd = currentOffset + chunkSize - 1;
        const { response, actualEnd, totalSize } = await fetchWithRetry(currentOffset, requestEnd);

        const data = new Uint8Array(await response.arrayBuffer());

        if (data.length === 0) {
          // No more data
          break;
        }

        const isLast = totalSize !== undefined && actualEnd >= totalSize - 1;

        const result: RangeFetchResult = {
          data,
          start: currentOffset,
          end: actualEnd + 1,
          isLast,
        };
        if (totalSize !== undefined) {
          result.totalSize = totalSize;
        }
        yield result;

        if (isLast) {
          break;
        }

        currentOffset = actualEnd + 1;
      }
    },

    async getTotalSize(): Promise<number | null> {
      if (cachedTotalSize !== null) {
        return cachedTotalSize;
      }

      try {
        const response = await fetch(url, { method: 'HEAD' });
        const contentLength = response.headers.get('Content-Length');
        if (contentLength) {
          cachedTotalSize = parseInt(contentLength, 10);
          return cachedTotalSize;
        }
      } catch {
        // HEAD request failed, try range request
        try {
          const { totalSize } = await fetchWithRetry(0, 0);
          if (totalSize) {
            cachedTotalSize = totalSize;
            return cachedTotalSize;
          }
        } catch {
          // Ignore
        }
      }

      return null;
    },
  };
}
