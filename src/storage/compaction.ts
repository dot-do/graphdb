/**
 * R2 Compaction Pipeline
 *
 * Compacts small WAL chunks into larger files for efficient storage and query.
 * Implements a tiered compaction strategy:
 * - L0 (WAL): Small CDC chunks written by R2Writer
 * - L1: Compacted chunks up to 8MB
 * - L2: Large chunks up to 128MB
 *
 * Features:
 * - Configurable compaction thresholds
 * - Lock-based concurrent compaction safety
 * - Atomic compaction with source deletion after success
 * - Data integrity preservation
 *
 * @packageDocumentation
 */

import type { Namespace } from '../core/types';
import type { Triple } from '../core/triple';
import { encodeGraphCol, decodeGraphCol, getChunkStats } from './graphcol';
import { parseNamespaceToPath, formatDatePath, generateSequence } from './r2-writer';

// ============================================================================
// Types
// ============================================================================

/**
 * Compaction level enumeration
 */
export enum CompactionLevel {
  /** Compact L0 WAL chunks into L1 */
  L0_TO_L1 = 'L0_TO_L1',
  /** Compact L1 chunks into L2 */
  L1_TO_L2 = 'L1_TO_L2',
}

/**
 * Compaction configuration
 */
export interface CompactionConfig {
  /** Maximum size for L1 compacted chunks (default: 8MB) */
  l1ThresholdBytes: number;
  /** Maximum size for L2 compacted chunks (default: 128MB) */
  l2ThresholdBytes: number;
  /** Minimum number of chunks required to trigger compaction */
  minChunksToCompact: number;
  /** Lock timeout in milliseconds (default: 5 minutes) */
  lockTimeoutMs?: number;
}

/**
 * Information about a chunk for compaction selection
 */
export interface CompactionChunkInfo {
  /** R2 key path */
  path: string;
  /** Size in bytes */
  sizeBytes: number;
  /** Number of triples in the chunk */
  tripleCount: number;
  /** Minimum timestamp in the chunk */
  minTimestamp: bigint;
  /** Maximum timestamp in the chunk */
  maxTimestamp: bigint;
}

/**
 * Result of a compaction operation
 */
export interface CompactionResult {
  /** Source chunk paths that were compacted */
  sourcePaths: string[];
  /** Target path of the compacted chunk */
  targetPath: string;
  /** Total bytes of source chunks */
  bytesCompacted: number;
  /** Total number of triples compacted */
  triplesCompacted: number;
  /** Duration of the compaction in milliseconds */
  durationMs: number;
}

/**
 * Lock file content
 */
interface LockFile {
  lockedAt: number;
  owner: string;
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_LOCK_TIMEOUT_MS = 5 * 60 * 1000; // 5 minutes
const LOCK_FILE_NAME = '_compaction.lock';

// ============================================================================
// Chunk Selection
// ============================================================================

/**
 * Select chunks for compaction based on configuration and level
 *
 * Selects chunks that:
 * 1. Meet the minimum count threshold
 * 2. Don't exceed the target size threshold when combined
 * 3. Are sorted by timestamp for contiguous compaction
 *
 * @param chunks Available chunks to select from
 * @param config Compaction configuration
 * @param level Compaction level (determines size threshold)
 * @returns Selected chunks for compaction (empty if insufficient)
 */
export function selectChunksForCompaction(
  chunks: CompactionChunkInfo[],
  config: CompactionConfig,
  level: CompactionLevel
): CompactionChunkInfo[] {
  // Sort by minimum timestamp
  const sorted = [...chunks].sort((a, b) => {
    if (a.minTimestamp < b.minTimestamp) return -1;
    if (a.minTimestamp > b.minTimestamp) return 1;
    return 0;
  });

  // Determine target threshold based on level
  const threshold = level === CompactionLevel.L0_TO_L1
    ? config.l1ThresholdBytes
    : config.l2ThresholdBytes;

  // Select chunks up to threshold
  const selected: CompactionChunkInfo[] = [];
  let totalSize = 0;

  for (const chunk of sorted) {
    // Check if adding this chunk would exceed threshold
    if (totalSize + chunk.sizeBytes > threshold && selected.length >= config.minChunksToCompact) {
      break;
    }

    selected.push(chunk);
    totalSize += chunk.sizeBytes;

    // Stop if we've reached the threshold
    if (totalSize >= threshold) {
      break;
    }
  }

  // Return empty if we don't have enough chunks
  if (selected.length < config.minChunksToCompact) {
    return [];
  }

  return selected;
}

// ============================================================================
// Lock Management
// ============================================================================

/**
 * Try to acquire the compaction lock
 *
 * @param bucket R2 bucket
 * @param namespacePath Namespace path prefix
 * @param timeoutMs Lock timeout in milliseconds
 * @returns True if lock acquired, false if locked by another process
 */
async function tryAcquireLock(
  bucket: R2Bucket,
  namespacePath: string,
  timeoutMs: number
): Promise<boolean> {
  const lockPath = `${namespacePath}/${LOCK_FILE_NAME}`;

  // Check for existing lock
  const existingLock = await bucket.get(lockPath);
  if (existingLock) {
    try {
      const lockData: LockFile = JSON.parse(await existingLock.text());
      const lockAge = Date.now() - lockData.lockedAt;

      // If lock is still valid, don't acquire
      if (lockAge < timeoutMs) {
        return false;
      }
      // Lock is stale, proceed to overwrite
    } catch {
      // Invalid lock file, proceed to overwrite
    }
  }

  // Create new lock
  const newLock: LockFile = {
    lockedAt: Date.now(),
    owner: `worker-${Math.random().toString(36).substring(2, 10)}`,
  };

  await bucket.put(lockPath, JSON.stringify(newLock));
  return true;
}

/**
 * Release the compaction lock
 *
 * @param bucket R2 bucket
 * @param namespacePath Namespace path prefix
 */
async function releaseLock(bucket: R2Bucket, namespacePath: string): Promise<void> {
  const lockPath = `${namespacePath}/${LOCK_FILE_NAME}`;
  await bucket.delete(lockPath);
}

// ============================================================================
// Path Utilities
// ============================================================================

/**
 * Get the directory prefix for a compaction level
 *
 * @param level Compaction level
 * @returns Directory name
 */
function getLevelDirectory(level: CompactionLevel): string {
  switch (level) {
    case CompactionLevel.L0_TO_L1:
      return '_wal';
    case CompactionLevel.L1_TO_L2:
      return '_l1';
    default:
      return '_wal';
  }
}

/**
 * Get the target directory for compaction output
 *
 * @param level Compaction level
 * @returns Target directory name
 */
function getTargetDirectory(level: CompactionLevel): string {
  switch (level) {
    case CompactionLevel.L0_TO_L1:
      return '_l1';
    case CompactionLevel.L1_TO_L2:
      return '_l2';
    default:
      return '_l1';
  }
}

/**
 * Generate the target path for a compacted chunk
 *
 * @param namespacePath Namespace path prefix
 * @param level Compaction level
 * @param timestamp Representative timestamp for the chunk
 * @returns Full R2 key path
 */
function generateCompactedPath(
  namespacePath: string,
  level: CompactionLevel,
  timestamp: bigint
): string {
  const targetDir = getTargetDirectory(level);
  const datePath = formatDatePath(timestamp);
  const sequence = generateSequence(timestamp);

  return `${namespacePath}/${targetDir}/${datePath}/${sequence}.gcol`;
}

// ============================================================================
// Main Compaction Function
// ============================================================================

/**
 * Compact chunks in R2 storage
 *
 * This function:
 * 1. Acquires a compaction lock to prevent concurrent compaction
 * 2. Lists available chunks at the source level
 * 3. Selects chunks for compaction based on configuration
 * 4. Reads and merges all selected chunks
 * 5. Writes the compacted chunk to the target level
 * 6. Deletes source chunks only after successful write
 * 7. Releases the compaction lock
 *
 * @param bucket R2 bucket
 * @param namespace Namespace for the compaction
 * @param config Compaction configuration
 * @param level Compaction level (default: L0_TO_L1)
 * @returns Compaction result or null if no compaction performed
 */
export async function compactChunks(
  bucket: R2Bucket,
  namespace: Namespace,
  config: CompactionConfig,
  level: CompactionLevel = CompactionLevel.L0_TO_L1
): Promise<CompactionResult | null> {
  const startTime = Date.now();
  const namespacePath = parseNamespaceToPath(namespace);
  const lockTimeoutMs = config.lockTimeoutMs ?? DEFAULT_LOCK_TIMEOUT_MS;

  // Try to acquire lock
  const lockAcquired = await tryAcquireLock(bucket, namespacePath, lockTimeoutMs);
  if (!lockAcquired) {
    return null; // Another compaction is in progress
  }

  try {
    // List chunks at source level
    const sourceDir = getLevelDirectory(level);
    const prefix = `${namespacePath}/${sourceDir}/`;

    const listed = await bucket.list({ prefix });
    const chunkPaths = listed.objects
      .filter((obj) => obj.key.endsWith('.gcol'))
      .map((obj) => obj.key);

    if (chunkPaths.length < config.minChunksToCompact) {
      return null;
    }

    // Get chunk info for each path
    const chunkInfos: CompactionChunkInfo[] = [];
    for (const path of chunkPaths) {
      const obj = await bucket.get(path);
      if (!obj) continue;

      const data = new Uint8Array(await obj.arrayBuffer());
      try {
        const stats = getChunkStats(data);
        chunkInfos.push({
          path,
          sizeBytes: data.length,
          tripleCount: stats.tripleCount,
          minTimestamp: stats.timeRange[0],
          maxTimestamp: stats.timeRange[1],
        });
      } catch {
        // Skip invalid chunks
        continue;
      }
    }

    // Select chunks for compaction
    const selected = selectChunksForCompaction(chunkInfos, config, level);
    if (selected.length === 0) {
      return null;
    }

    // Read and merge all selected chunks
    const allTriples: Triple[] = [];
    for (const chunk of selected) {
      const obj = await bucket.get(chunk.path);
      if (!obj) {
        throw new Error(`Failed to read chunk: ${chunk.path}`);
      }

      const data = new Uint8Array(await obj.arrayBuffer());
      const triples = decodeGraphCol(data);
      allTriples.push(...triples);
    }

    // Sort triples by timestamp for optimal compression
    allTriples.sort((a, b) => {
      if (a.timestamp < b.timestamp) return -1;
      if (a.timestamp > b.timestamp) return 1;
      return 0;
    });

    // Encode the compacted chunk
    const compactedData = encodeGraphCol(allTriples, namespace);

    // Determine representative timestamp (use max for the path)
    // selected is guaranteed to be non-empty at this point (we return early if empty)
    const maxTimestamp = selected.reduce(
      (max, chunk) => (chunk.maxTimestamp > max ? chunk.maxTimestamp : max),
      selected[0]!.maxTimestamp
    );

    // Generate target path
    const targetPath = generateCompactedPath(namespacePath, level, maxTimestamp);

    // Write compacted chunk first
    await bucket.put(targetPath, compactedData);

    // Delete source chunks only after successful write
    const sourcePaths = selected.map((c) => c.path);
    await bucket.delete(sourcePaths);

    const totalBytes = selected.reduce((sum, c) => sum + c.sizeBytes, 0);
    const totalTriples = selected.reduce((sum, c) => sum + c.tripleCount, 0);

    return {
      sourcePaths,
      targetPath,
      bytesCompacted: totalBytes,
      triplesCompacted: totalTriples,
      durationMs: Date.now() - startTime,
    };
  } finally {
    // Always release lock
    await releaseLock(bucket, namespacePath);
  }
}

/**
 * Get chunk information for all chunks at a given level
 *
 * @param bucket R2 bucket
 * @param namespace Namespace to list
 * @param level Compaction level to list
 * @returns Array of chunk information
 */
export async function listChunksAtLevel(
  bucket: R2Bucket,
  namespace: Namespace,
  level: CompactionLevel
): Promise<CompactionChunkInfo[]> {
  const namespacePath = parseNamespaceToPath(namespace);
  const sourceDir = getLevelDirectory(level);
  const prefix = `${namespacePath}/${sourceDir}/`;

  const listed = await bucket.list({ prefix });
  const chunks: CompactionChunkInfo[] = [];

  for (const obj of listed.objects) {
    if (!obj.key.endsWith('.gcol')) continue;

    const object = await bucket.get(obj.key);
    if (!object) continue;

    const data = new Uint8Array(await object.arrayBuffer());
    try {
      const stats = getChunkStats(data);
      chunks.push({
        path: obj.key,
        sizeBytes: data.length,
        tripleCount: stats.tripleCount,
        minTimestamp: stats.timeRange[0],
        maxTimestamp: stats.timeRange[1],
      });
    } catch {
      // Skip invalid chunks
    }
  }

  return chunks;
}

/**
 * Get compaction statistics for a namespace
 *
 * @param bucket R2 bucket
 * @param namespace Namespace to analyze
 * @returns Statistics about chunks at each level
 */
export async function getCompactionStats(
  bucket: R2Bucket,
  namespace: Namespace
): Promise<{
  l0: { chunkCount: number; totalBytes: number; totalTriples: number };
  l1: { chunkCount: number; totalBytes: number; totalTriples: number };
  l2: { chunkCount: number; totalBytes: number; totalTriples: number };
}> {
  const l0Chunks = await listChunksAtLevel(bucket, namespace, CompactionLevel.L0_TO_L1);

  // For L1, we need to check the _l1 directory directly
  const namespacePath = parseNamespaceToPath(namespace);

  const l1Listed = await bucket.list({ prefix: `${namespacePath}/_l1/` });
  const l1Chunks: CompactionChunkInfo[] = [];
  for (const obj of l1Listed.objects) {
    if (!obj.key.endsWith('.gcol')) continue;
    const object = await bucket.get(obj.key);
    if (!object) continue;
    const data = new Uint8Array(await object.arrayBuffer());
    try {
      const stats = getChunkStats(data);
      l1Chunks.push({
        path: obj.key,
        sizeBytes: data.length,
        tripleCount: stats.tripleCount,
        minTimestamp: stats.timeRange[0],
        maxTimestamp: stats.timeRange[1],
      });
    } catch {
      // Skip invalid chunks
    }
  }

  const l2Listed = await bucket.list({ prefix: `${namespacePath}/_l2/` });
  const l2Chunks: CompactionChunkInfo[] = [];
  for (const obj of l2Listed.objects) {
    if (!obj.key.endsWith('.gcol')) continue;
    const object = await bucket.get(obj.key);
    if (!object) continue;
    const data = new Uint8Array(await object.arrayBuffer());
    try {
      const stats = getChunkStats(data);
      l2Chunks.push({
        path: obj.key,
        sizeBytes: data.length,
        tripleCount: stats.tripleCount,
        minTimestamp: stats.timeRange[0],
        maxTimestamp: stats.timeRange[1],
      });
    } catch {
      // Skip invalid chunks
    }
  }

  return {
    l0: {
      chunkCount: l0Chunks.length,
      totalBytes: l0Chunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      totalTriples: l0Chunks.reduce((sum, c) => sum + c.tripleCount, 0),
    },
    l1: {
      chunkCount: l1Chunks.length,
      totalBytes: l1Chunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      totalTriples: l1Chunks.reduce((sum, c) => sum + c.tripleCount, 0),
    },
    l2: {
      chunkCount: l2Chunks.length,
      totalBytes: l2Chunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      totalTriples: l2Chunks.reduce((sum, c) => sum + c.tripleCount, 0),
    },
  };
}
