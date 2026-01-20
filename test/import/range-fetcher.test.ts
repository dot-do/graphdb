/**
 * Tests for RangeFetcher - HTTP Range requests for memory-efficient large file downloads
 *
 * Tests cover:
 * - Range request handling (206 Partial Content)
 * - Async generator chunks iteration
 * - Resume from offset
 * - Total size detection (HEAD request)
 * - Retry logic with exponential backoff
 * - Default chunk size configuration
 * - Edge cases (no range support, 416 Range Not Satisfiable)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  createRangeFetcher,
  type RangeFetcher,
  type RangeFetcherOptions,
  type RangeFetchResult,
} from '../../src/import/range-fetcher';

// ============================================================================
// Test Setup
// ============================================================================

let originalFetch: typeof globalThis.fetch;

beforeEach(() => {
  originalFetch = globalThis.fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
});

// ============================================================================
// RangeFetcher Tests
// ============================================================================

describe('RangeFetcher', () => {
  describe('Range Requests', () => {
    it('should fetch a specific byte range', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const content = 'Hello, World! This is test data for range requests.';
          const slice = content.slice(start, end + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 10,
      });

      const result = await fetcher.fetchRange(0, 4);
      expect(new TextDecoder().decode(result.data)).toBe('Hello');
      expect(result.start).toBe(0);
      expect(result.isLast).toBe(false);
    });

    it('should iterate chunks with generator', async () => {
      const content = 'AAAABBBBCCCC';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
      });

      const chunks: string[] = [];
      for await (const { data } of fetcher.chunks(0)) {
        chunks.push(new TextDecoder().decode(data));
      }

      expect(chunks).toEqual(['AAAA', 'BBBB', 'CCCC']);
    });

    it('should resume from offset', async () => {
      const content = 'AAAABBBBCCCC';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${start + slice.length - 1}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
      });

      // Start from offset 4 (skip first chunk)
      const chunks: string[] = [];
      for await (const { data } of fetcher.chunks(4)) {
        chunks.push(new TextDecoder().decode(data));
      }

      expect(chunks).toEqual(['BBBB', 'CCCC']);
    });

    it('should include start and end in result', async () => {
      const content = 'ABCDEFGHIJ';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 3,
      });

      const result = await fetcher.fetchRange(0, 2);
      expect(result.start).toBe(0);
      expect(result.end).toBe(3); // end is exclusive
    });

    it('should mark last chunk correctly', async () => {
      const content = 'ABCDEF';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 3,
      });

      const results: RangeFetchResult[] = [];
      for await (const result of fetcher.chunks(0)) {
        results.push(result);
      }

      expect(results).toHaveLength(2);
      expect(results[0]!.isLast).toBe(false);
      expect(results[1]!.isLast).toBe(true);
    });

    it('should include total size when available', async () => {
      const content = 'Test content for total size check';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 10,
      });

      const result = await fetcher.fetchRange(0, 9);
      expect(result.totalSize).toBe(content.length);
    });
  });

  describe('Total Size Detection', () => {
    it('should get total size via HEAD request', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          return new Response(null, {
            status: 200,
            headers: {
              'Content-Length': '12345',
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      const size = await fetcher.getTotalSize();
      expect(size).toBe(12345);
    });

    it('should cache total size', async () => {
      let headCalls = 0;
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          headCalls++;
          return new Response(null, {
            status: 200,
            headers: {
              'Content-Length': '12345',
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      await fetcher.getTotalSize();
      await fetcher.getTotalSize();
      await fetcher.getTotalSize();

      expect(headCalls).toBe(1);
    });

    it('should return null when size cannot be determined', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          return new Response(null, {
            status: 200,
            headers: {},
          });
        }
        // Range request also doesn't include size
        return new Response('data', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-3/*',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      const size = await fetcher.getTotalSize();
      expect(size).toBeNull();
    });

    it('should fallback to range request for size detection', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        if (options?.method === 'HEAD') {
          throw new Error('HEAD not supported');
        }
        return new Response('x', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-0/9999',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      const size = await fetcher.getTotalSize();
      expect(size).toBe(9999);
    });

    it('should cache size from range request response', async () => {
      let fetchCalls = 0;
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        fetchCalls++;
        return new Response('data', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-3/1000',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');

      // First call fetches range
      await fetcher.fetchRange(0, 3);

      // getTotalSize should use cached value
      const size = await fetcher.getTotalSize();
      expect(size).toBe(1000);
      expect(fetchCalls).toBe(1); // No additional fetch for size
    });
  });

  describe('Retry Logic', () => {
    it('should retry on failure with exponential backoff', async () => {
      let attempts = 0;

      globalThis.fetch = vi.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 3) {
          throw new Error('Network error');
        }
        return new Response('Success', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-6/7',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        maxRetries: 3,
        baseDelayMs: 10, // Short delay for tests
      });

      const result = await fetcher.fetchRange(0, 6);
      expect(new TextDecoder().decode(result.data)).toBe('Success');
      expect(attempts).toBe(3);
    });

    it('should fail after max retries exceeded', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        throw new Error('Network error');
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        maxRetries: 2,
        baseDelayMs: 10,
      });

      await expect(fetcher.fetchRange(0, 100)).rejects.toThrow('Network error');
    });

    it('should retry on HTTP errors', async () => {
      let attempts = 0;

      globalThis.fetch = vi.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 2) {
          return new Response('Server Error', { status: 500, statusText: 'Internal Server Error' });
        }
        return new Response('OK', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-1/2',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        maxRetries: 3,
        baseDelayMs: 10,
      });

      const result = await fetcher.fetchRange(0, 1);
      expect(new TextDecoder().decode(result.data)).toBe('OK');
      expect(attempts).toBe(2);
    });

    it('should use default retry settings', async () => {
      let attempts = 0;

      globalThis.fetch = vi.fn().mockImplementation(async () => {
        attempts++;
        if (attempts < 3) {
          throw new Error('Temporary failure');
        }
        return new Response('OK', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-1/2',
          },
        });
      });

      // Use default settings (maxRetries: 3)
      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        baseDelayMs: 10,
      });

      const result = await fetcher.fetchRange(0, 1);
      expect(result.data.length).toBe(2);
    });
  });

  describe('Default Chunk Size', () => {
    it('should default to 10MB chunk size', async () => {
      let requestedRange: string | null = null;

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        requestedRange = (options?.headers as Record<string, string>)?.Range;
        return new Response('x'.repeat(10 * 1024 * 1024), {
          status: 206,
          headers: {
            'Content-Range': `bytes 0-${10 * 1024 * 1024 - 1}/${100 * 1024 * 1024}`,
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      await fetcher.fetchRange(0);

      // Should request 0 to 10MB-1
      expect(requestedRange).toBe(`bytes=0-${10 * 1024 * 1024 - 1}`);
    });

    it('should respect custom chunk size', async () => {
      let requestedRange: string | null = null;

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        requestedRange = (options?.headers as Record<string, string>)?.Range;
        return new Response('x'.repeat(1024), {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-1023/10000',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 1024,
      });
      await fetcher.fetchRange(0);

      expect(requestedRange).toBe('bytes=0-1023');
    });
  });

  describe('Server Without Range Support', () => {
    it('should handle server returning 200 instead of 206', async () => {
      const content = 'Full file content';

      globalThis.fetch = vi.fn().mockImplementation(async () => {
        return new Response(content, {
          status: 200,
          headers: {
            'Content-Length': String(content.length),
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 5,
      });

      const result = await fetcher.fetchRange(0, 4);
      expect(new TextDecoder().decode(result.data)).toBe(content);
    });

    it('should handle 416 Range Not Satisfiable', async () => {
      const content = 'Short';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader) {
          const [start] = rangeHeader.slice(6).split('-').map(Number);
          if (start >= content.length) {
            return new Response(null, { status: 416 });
          }
          const actualEnd = Math.min(start + 10, content.length - 1);
          return new Response(content.slice(start, actualEnd + 1), {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 10,
      });

      // Should get data up to the end
      const chunks: string[] = [];
      for await (const { data } of fetcher.chunks(0)) {
        chunks.push(new TextDecoder().decode(data));
      }

      expect(chunks).toEqual(['Short']);
    });
  });

  describe('Async Generator Behavior', () => {
    it('should stop iteration when no more data', async () => {
      const content = 'ABC';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          if (start >= content.length) {
            return new Response('', {
              status: 206,
              headers: {
                'Content-Range': `bytes ${start}-${start}/${content.length}`,
              },
            });
          }
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 2,
      });

      const results: RangeFetchResult[] = [];
      for await (const result of fetcher.chunks(0)) {
        results.push(result);
      }

      expect(results).toHaveLength(2);
      expect(new TextDecoder().decode(results[0]!.data)).toBe('AB');
      expect(new TextDecoder().decode(results[1]!.data)).toBe('C');
    });

    it('should handle empty file', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        return new Response('', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-0/0',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/empty.txt', {
        chunkSize: 10,
      });

      const chunks: Uint8Array[] = [];
      for await (const { data } of fetcher.chunks(0)) {
        chunks.push(data);
      }

      expect(chunks).toHaveLength(0);
    });

    it('should maintain correct offset progression', async () => {
      const content = '0123456789ABCDEF';

      globalThis.fetch = vi.fn().mockImplementation(async (url: string, options?: RequestInit) => {
        const rangeHeader = (options?.headers as Record<string, string>)?.Range;
        if (rangeHeader && rangeHeader.startsWith('bytes=')) {
          const [start, end] = rangeHeader.slice(6).split('-').map(Number);
          const actualEnd = Math.min(end, content.length - 1);
          const slice = content.slice(start, actualEnd + 1);

          return new Response(slice, {
            status: 206,
            headers: {
              'Content-Range': `bytes ${start}-${actualEnd}/${content.length}`,
            },
          });
        }
        return new Response('', { status: 400 });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 4,
      });

      const offsets: { start: number; end: number }[] = [];
      for await (const result of fetcher.chunks(0)) {
        offsets.push({ start: result.start, end: result.end });
      }

      expect(offsets).toEqual([
        { start: 0, end: 4 },
        { start: 4, end: 8 },
        { start: 8, end: 12 },
        { start: 12, end: 16 },
      ]);
    });
  });

  describe('URL Handling', () => {
    it('should work with different URL formats', async () => {
      const urls = [
        'https://example.com/file.txt',
        'https://example.com/path/to/file.txt',
        'https://example.com/file.txt?query=param',
        'http://localhost:8080/file.txt',
      ];

      for (const testUrl of urls) {
        let calledUrl: string | null = null;

        globalThis.fetch = vi.fn().mockImplementation(async (url: string) => {
          calledUrl = url;
          return new Response('data', {
            status: 206,
            headers: {
              'Content-Range': 'bytes 0-3/4',
            },
          });
        });

        const fetcher = createRangeFetcher(testUrl);
        await fetcher.fetchRange(0, 3);

        expect(calledUrl).toBe(testUrl);
      }
    });
  });

  describe('Content-Range Parsing', () => {
    it('should parse Content-Range with wildcard total', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        return new Response('data', {
          status: 206,
          headers: {
            'Content-Range': 'bytes 0-3/*',
          },
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt');
      const result = await fetcher.fetchRange(0, 3);

      expect(result.totalSize).toBeUndefined();
      expect(result.start).toBe(0);
    });

    it('should handle missing Content-Range header', async () => {
      globalThis.fetch = vi.fn().mockImplementation(async () => {
        return new Response('data', {
          status: 206,
          headers: {},
        });
      });

      const fetcher = createRangeFetcher('https://example.com/data.txt', {
        chunkSize: 10,
      });

      const result = await fetcher.fetchRange(0, 3);
      expect(result.data.length).toBe(4);
    });
  });
});
