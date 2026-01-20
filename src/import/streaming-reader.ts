/**
 * StreamingLineReader - Memory-efficient line processing for GraphDB imports
 *
 * Key features:
 * - Processes chunks without loading full text into memory
 * - Only keeps 1 incomplete line in buffer (< 64KB typical)
 * - Tracks byte and line counts for checkpointing
 * - Can be paused and resumed with state persistence
 *
 * @packageDocumentation
 */

// ============================================================================
// Types
// ============================================================================

/**
 * State for StreamingLineReader - can be persisted and restored
 */
export interface LineReaderState {
  /** Total bytes processed so far */
  bytesProcessed: number;
  /** Total lines emitted so far */
  linesEmitted: number;
  /** Partial line carried over from previous chunk */
  partialLine: string;
}

/**
 * StreamingLineReader interface for memory-efficient line processing
 */
export interface StreamingLineReader {
  /** Process a chunk of data and yield complete lines */
  processChunk(chunk: Uint8Array): AsyncGenerator<string>;
  /** Get current state for checkpointing */
  getState(): LineReaderState;
  /** Restore from a saved state */
  restoreState(state: LineReaderState): void;
  /** Flush any remaining partial line */
  flush(): string | null;
}

/**
 * Options for creating a StreamingLineReader
 */
export interface StreamingLineReaderOptions {
  /** Maximum buffer size for partial lines (default 64KB) */
  maxBufferSize?: number;
}

// ============================================================================
// Implementation
// ============================================================================

/**
 * Create a streaming line reader that processes chunks without loading full text
 *
 * Key features:
 * - Only keeps 1 incomplete line in memory (< 64KB typical)
 * - Tracks byte and line counts for checkpointing
 * - Can be paused and resumed with state persistence
 *
 * @param options Configuration options
 * @returns StreamingLineReader instance
 *
 * @example
 * ```typescript
 * const lineReader = createStreamingLineReader();
 *
 * for await (const chunk of fetchChunks(url)) {
 *   for await (const line of lineReader.processChunk(chunk)) {
 *     const record = JSON.parse(line);
 *     // process record...
 *   }
 * }
 *
 * // Handle any remaining partial line
 * const remaining = lineReader.flush();
 * if (remaining) {
 *   const record = JSON.parse(remaining);
 * }
 * ```
 */
export function createStreamingLineReader(
  options?: StreamingLineReaderOptions
): StreamingLineReader {
  const maxBufferSize = options?.maxBufferSize ?? 64 * 1024; // 64KB default
  const decoder = new TextDecoder('utf-8');

  let state: LineReaderState = {
    bytesProcessed: 0,
    linesEmitted: 0,
    partialLine: '',
  };

  return {
    async *processChunk(chunk: Uint8Array): AsyncGenerator<string> {
      // Decode chunk to text
      const text = decoder.decode(chunk, { stream: true });
      state.bytesProcessed += chunk.byteLength;

      // Combine with any partial line from previous chunk
      const fullText = state.partialLine + text;

      // Split into lines
      const lines = fullText.split('\n');

      // Last element is either empty (if text ended with \n) or partial
      state.partialLine = lines.pop() ?? '';

      // Check buffer size limit
      if (state.partialLine.length > maxBufferSize) {
        console.warn(
          `[StreamingLineReader] Partial line exceeds ${maxBufferSize} bytes, truncating`
        );
        state.partialLine = state.partialLine.slice(-maxBufferSize);
      }

      // Yield complete lines
      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed) {
          state.linesEmitted++;
          yield trimmed;
        }
      }
    },

    getState(): LineReaderState {
      return { ...state };
    },

    restoreState(savedState: LineReaderState): void {
      state = { ...savedState };
    },

    flush(): string | null {
      if (state.partialLine.trim()) {
        const line = state.partialLine.trim();
        state.partialLine = '';
        state.linesEmitted++;
        return line;
      }
      return null;
    },
  };
}
