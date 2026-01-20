/**
 * Tests for ResumableImportState - Checkpoint management for long-running imports
 *
 * Tests cover:
 * - Checkpoint save and load operations
 * - Checkpoint update with partial data
 * - Checkpoint deletion
 * - Listing all checkpoints
 * - Resume capability from checkpoints
 * - Edge cases (non-existent checkpoints, empty storage)
 * - Metadata handling
 */

import { describe, it, expect } from 'vitest';
import {
  createResumableImportState,
  type ResumableImportState,
  type ImportCheckpoint,
} from '../../src/import/resumable-state';
import type { LineReaderState } from '../../src/import/streaming-reader';
import type { BatchWriterState } from '../../src/import/batched-writer';

// ============================================================================
// Test Helpers
// ============================================================================

function createMockDOStorage(): DurableObjectStorage & {
  _data: Map<string, unknown>;
} {
  const data = new Map<string, unknown>();

  return {
    _data: data,
    async get<T>(key: string): Promise<T | undefined> {
      return data.get(key) as T | undefined;
    },
    async put(key: string, value: unknown): Promise<void> {
      data.set(key, value);
    },
    async delete(key: string | string[]): Promise<boolean> {
      if (Array.isArray(key)) {
        let deleted = false;
        for (const k of key) {
          if (data.delete(k)) deleted = true;
        }
        return deleted;
      }
      return data.delete(key);
    },
    async list(options?: { prefix?: string }): Promise<Map<string, unknown>> {
      const result = new Map<string, unknown>();
      for (const [key, value] of data) {
        if (!options?.prefix || key.startsWith(options.prefix)) {
          result.set(key, value);
        }
      }
      return result;
    },
  } as unknown as DurableObjectStorage & { _data: Map<string, unknown> };
}

function createTestCheckpoint(overrides?: Partial<ImportCheckpoint>): ImportCheckpoint {
  const defaultLineReaderState: LineReaderState = {
    bytesProcessed: 1000,
    linesEmitted: 50,
    partialLine: '',
  };

  const defaultBatchWriterState: BatchWriterState = {
    triplesWritten: 200,
    chunksUploaded: 2,
    bytesUploaded: 5000,
    chunkInfos: [],
    bloomState: {
      filter: '',
      k: 7,
      m: 1000,
      version: 'v1',
      meta: {
        count: 0,
        capacity: 1000,
        targetFpr: 0.01,
        expectedFpr: 0.01,
        sizeBytes: 125,
      },
    },
  };

  return {
    jobId: 'test-job',
    sourceUrl: 'https://example.com/data.json',
    byteOffset: 1000,
    linesProcessed: 50,
    triplesWritten: 200,
    lineReaderState: defaultLineReaderState,
    batchWriterState: defaultBatchWriterState,
    checkpointedAt: new Date().toISOString(),
    ...overrides,
  };
}

// ============================================================================
// ResumableImportState Tests
// ============================================================================

describe('ResumableImportState', () => {
  describe('Checkpoint Save and Load', () => {
    it('should save and load a checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded).toBeDefined();
      expect(loaded?.jobId).toBe('test-job');
      expect(loaded?.byteOffset).toBe(1000);
      expect(loaded?.linesProcessed).toBe(50);
      expect(loaded?.triplesWritten).toBe(200);
    });

    it('should return null for non-existent checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const loaded = await importState.loadCheckpoint('non-existent');
      expect(loaded).toBeNull();
    });

    it('should update checkpointedAt timestamp on save', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const oldTimestamp = '2020-01-01T00:00:00.000Z';
      const checkpoint = createTestCheckpoint({ checkpointedAt: oldTimestamp });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.checkpointedAt).not.toBe(oldTimestamp);
      expect(new Date(loaded!.checkpointedAt).getTime()).toBeGreaterThan(new Date(oldTimestamp).getTime());
    });

    it('should preserve all checkpoint fields', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        totalBytes: 100000,
        metadata: { source: 'test', version: '1.0' },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.sourceUrl).toBe('https://example.com/data.json');
      expect(loaded?.totalBytes).toBe(100000);
      expect(loaded?.lineReaderState).toEqual(checkpoint.lineReaderState);
      expect(loaded?.batchWriterState).toEqual(checkpoint.batchWriterState);
      expect(loaded?.metadata).toEqual({ source: 'test', version: '1.0' });
    });

    it('should overwrite existing checkpoint on save', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint1 = createTestCheckpoint({ byteOffset: 1000 });
      await importState.saveCheckpoint(checkpoint1);

      const checkpoint2 = createTestCheckpoint({ byteOffset: 2000 });
      await importState.saveCheckpoint(checkpoint2);

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.byteOffset).toBe(2000);
    });
  });

  describe('Checkpoint Update', () => {
    it('should update existing checkpoint with partial data', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();
      await importState.saveCheckpoint(checkpoint);

      await importState.updateCheckpoint('test-job', {
        byteOffset: 2000,
        linesProcessed: 100,
      });

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.byteOffset).toBe(2000);
      expect(loaded?.linesProcessed).toBe(100);
      // Other fields should remain unchanged
      expect(loaded?.triplesWritten).toBe(200);
      expect(loaded?.sourceUrl).toBe('https://example.com/data.json');
    });

    it('should update checkpointedAt on update', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();
      await importState.saveCheckpoint(checkpoint);

      const afterSave = await importState.loadCheckpoint('test-job');
      const savedTimestamp = afterSave?.checkpointedAt;

      // Small delay to ensure different timestamp
      await new Promise((resolve) => setTimeout(resolve, 10));

      await importState.updateCheckpoint('test-job', { byteOffset: 5000 });

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.checkpointedAt).not.toBe(savedTimestamp);
    });

    it('should not create checkpoint if it does not exist', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      await importState.updateCheckpoint('non-existent', { byteOffset: 1000 });

      const loaded = await importState.loadCheckpoint('non-existent');
      expect(loaded).toBeNull();
    });

    it('should update nested state objects', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();
      await importState.saveCheckpoint(checkpoint);

      const newLineReaderState: LineReaderState = {
        bytesProcessed: 5000,
        linesEmitted: 200,
        partialLine: '{"partial":',
      };

      await importState.updateCheckpoint('test-job', {
        lineReaderState: newLineReaderState,
      });

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.lineReaderState).toEqual(newLineReaderState);
    });
  });

  describe('Checkpoint Deletion', () => {
    it('should delete a checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();
      await importState.saveCheckpoint(checkpoint);

      await importState.deleteCheckpoint('test-job');

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded).toBeNull();
    });

    it('should not error when deleting non-existent checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Should not throw
      await expect(importState.deleteCheckpoint('non-existent')).resolves.not.toThrow();
    });

    it('should only delete the specified checkpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-1' }));
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-2' }));
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-3' }));

      await importState.deleteCheckpoint('job-2');

      expect(await importState.loadCheckpoint('job-1')).not.toBeNull();
      expect(await importState.loadCheckpoint('job-2')).toBeNull();
      expect(await importState.loadCheckpoint('job-3')).not.toBeNull();
    });
  });

  describe('Listing Checkpoints', () => {
    it('should list all checkpoints', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-1' }));
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-2' }));
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-3' }));

      const jobs = await importState.listCheckpoints();
      expect(jobs).toHaveLength(3);
      expect(jobs.sort()).toEqual(['job-1', 'job-2', 'job-3']);
    });

    it('should return empty array when no checkpoints exist', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const jobs = await importState.listCheckpoints();
      expect(jobs).toHaveLength(0);
    });

    it('should not include deleted checkpoints in list', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-1' }));
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'job-2' }));
      await importState.deleteCheckpoint('job-1');

      const jobs = await importState.listCheckpoints();
      expect(jobs).toEqual(['job-2']);
    });

    it('should not include other storage keys in list', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Add checkpoint
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'my-job' }));

      // Add other data directly to storage (simulating other DO usage)
      await storage.put('other-key', { data: 'test' });
      await storage.put('another-key', 'value');

      const jobs = await importState.listCheckpoints();
      expect(jobs).toEqual(['my-job']);
    });
  });

  describe('Resume Capability', () => {
    it('should enable resume from checkpoint on timeout', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Simulate first run with timeout
      const checkpoint: ImportCheckpoint = createTestCheckpoint({
        jobId: 'import-job',
        sourceUrl: 'https://example.com/large-file.json',
        byteOffset: 50_000_000, // 50MB processed before timeout
        totalBytes: 100_000_000, // 100MB total
        linesProcessed: 250_000,
        triplesWritten: 500_000,
        lineReaderState: {
          bytesProcessed: 50_000_000,
          linesEmitted: 250_000,
          partialLine: '{"partial":',
        },
        batchWriterState: {
          triplesWritten: 500_000,
          chunksUploaded: 50,
          bytesUploaded: 25_000_000,
          chunkInfos: [],
          bloomState: {
            filter: '',
            k: 7,
            m: 1000000,
            version: 'v1',
            meta: {
              count: 500000,
              capacity: 1000000,
              targetFpr: 0.01,
              expectedFpr: 0.01,
              sizeBytes: 125000,
            },
          },
        },
      });

      await importState.saveCheckpoint(checkpoint);

      // Simulate resume
      const loaded = await importState.loadCheckpoint('import-job');
      expect(loaded).toBeDefined();
      expect(loaded?.byteOffset).toBe(50_000_000);
      expect(loaded?.lineReaderState.partialLine).toBe('{"partial":');
    });

    it('should preserve writer state for continuation', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        batchWriterState: {
          triplesWritten: 1000,
          chunksUploaded: 10,
          bytesUploaded: 50000,
          chunkInfos: [
            {
              id: 'chunk-1',
              tripleCount: 100,
              minTime: 1000n,
              maxTime: 2000n,
              bytes: 5000,
              path: '/test/chunk-1.gcol',
            },
          ],
          bloomState: {
            filter: 'abc123',
            k: 7,
            m: 10000,
            version: 'v1',
            meta: {
              count: 1000,
              capacity: 10000,
              targetFpr: 0.01,
              expectedFpr: 0.01,
              sizeBytes: 1250,
            },
          },
        },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.batchWriterState.chunksUploaded).toBe(10);
      expect(loaded?.batchWriterState.chunkInfos).toHaveLength(1);
      expect(loaded?.batchWriterState.bloomState.filter).toBe('abc123');
    });
  });

  describe('Metadata Handling', () => {
    it('should store and retrieve custom metadata', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        metadata: {
          source: 'wikidata',
          format: 'ndjson',
          version: '20230101',
          customField: { nested: true },
        },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.metadata).toEqual({
        source: 'wikidata',
        format: 'ndjson',
        version: '20230101',
        customField: { nested: true },
      });
    });

    it('should handle checkpoint without metadata', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint();
      delete checkpoint.metadata;

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.metadata).toBeUndefined();
    });

    it('should update metadata via updateCheckpoint', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        metadata: { phase: 'download' },
      });

      await importState.saveCheckpoint(checkpoint);
      await importState.updateCheckpoint('test-job', {
        metadata: { phase: 'transform' },
      });

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded?.metadata).toEqual({ phase: 'transform' });
    });
  });

  describe('Edge Cases', () => {
    it('should handle job IDs with special characters', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const specialIds = [
        'job-with-dashes',
        'job_with_underscores',
        'job.with.dots',
        'job:with:colons',
        'job/with/slashes',
      ];

      for (const jobId of specialIds) {
        const checkpoint = createTestCheckpoint({ jobId });
        await importState.saveCheckpoint(checkpoint);

        const loaded = await importState.loadCheckpoint(jobId);
        expect(loaded?.jobId).toBe(jobId);
      }
    });

    it('should handle very large checkpoints', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Create checkpoint with many chunk infos
      const chunkInfos = Array.from({ length: 1000 }, (_, i) => ({
        id: `chunk-${i}`,
        tripleCount: 10000,
        minTime: BigInt(i * 1000),
        maxTime: BigInt((i + 1) * 1000),
        bytes: 100000,
        path: `/test/chunk-${i}.gcol`,
      }));

      const checkpoint = createTestCheckpoint({
        batchWriterState: {
          triplesWritten: 10000000,
          chunksUploaded: 1000,
          bytesUploaded: 100000000,
          chunkInfos,
          bloomState: {
            filter: 'x'.repeat(10000), // Large bloom filter
            k: 7,
            m: 1000000,
            version: 'v1',
            meta: {
              count: 10000000,
              capacity: 10000000,
              targetFpr: 0.01,
              expectedFpr: 0.01,
              sizeBytes: 125000,
            },
          },
        },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.batchWriterState.chunkInfos).toHaveLength(1000);
    });

    it('should handle concurrent saves to same job', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      // Simulate concurrent saves
      const saves = Array.from({ length: 10 }, (_, i) =>
        importState.saveCheckpoint(
          createTestCheckpoint({
            byteOffset: i * 1000,
          })
        )
      );

      await Promise.all(saves);

      const loaded = await importState.loadCheckpoint('test-job');
      expect(loaded).toBeDefined();
      // One of the saves should have won
      expect(loaded?.byteOffset).toBeGreaterThanOrEqual(0);
      expect(loaded?.byteOffset).toBeLessThanOrEqual(9000);
    });

    it('should handle empty partial line in lineReaderState', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        lineReaderState: {
          bytesProcessed: 1000,
          linesEmitted: 100,
          partialLine: '',
        },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.lineReaderState.partialLine).toBe('');
    });

    it('should handle zero values', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      const checkpoint = createTestCheckpoint({
        byteOffset: 0,
        linesProcessed: 0,
        triplesWritten: 0,
        lineReaderState: {
          bytesProcessed: 0,
          linesEmitted: 0,
          partialLine: '',
        },
        batchWriterState: {
          triplesWritten: 0,
          chunksUploaded: 0,
          bytesUploaded: 0,
          chunkInfos: [],
          bloomState: {
            filter: '',
            k: 0,
            m: 0,
            version: 'v1',
            meta: {
              count: 0,
              capacity: 0,
              targetFpr: 0,
              expectedFpr: 0,
              sizeBytes: 0,
            },
          },
        },
      });

      await importState.saveCheckpoint(checkpoint);
      const loaded = await importState.loadCheckpoint('test-job');

      expect(loaded?.byteOffset).toBe(0);
      expect(loaded?.triplesWritten).toBe(0);
    });
  });

  describe('Storage Key Isolation', () => {
    it('should use checkpoint prefix for storage keys', async () => {
      const storage = createMockDOStorage();
      const importState = createResumableImportState(storage);

      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'my-job' }));

      // Verify the key has the correct prefix
      const keys = Array.from(storage._data.keys());
      expect(keys).toContain('checkpoint:my-job');
    });

    it('should isolate from non-checkpoint data', async () => {
      const storage = createMockDOStorage();

      // Add non-checkpoint data first
      await storage.put('other-data', { value: 'test' });
      await storage.put('checkpoint:fake', { notACheckpoint: true });

      const importState = createResumableImportState(storage);
      await importState.saveCheckpoint(createTestCheckpoint({ jobId: 'real-job' }));

      const jobs = await importState.listCheckpoints();
      // Should list checkpoint:fake and checkpoint:real-job
      expect(jobs.sort()).toEqual(['fake', 'real-job']);
    });
  });
});
