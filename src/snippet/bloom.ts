/**
 * Bloom Filter for GraphDB
 *
 * TypeScript utility to build and manage bloom filters for snippet routing.
 * Optimizes k (hash functions) and m (bits) for target false positive rate.
 *
 * Adapted from graphdb-spikes/snippet-bloom-router
 */

import { toBase64, fromBase64, fnv1aDoubleHash } from '../core';
import {
  DEFAULT_BLOOM_FPR,
  DEFAULT_BLOOM_MAX_SIZE_BYTES,
  TIMESTAMP_ENCODING_BASE,
  BYTES_PER_KB,
  PERCENTAGE_MULTIPLIER,
  DEFAULT_FPR_TEST_COUNT,
} from './constants.js';

export interface BloomFilterConfig {
  /** Number of bits in filter */
  m: number;
  /** Number of hash functions */
  k: number;
  /** Target false positive rate */
  targetFpr: number;
  /** Actual expected FPR based on n entries */
  expectedFpr: number;
  /** Number of entries the filter is sized for */
  capacity: number;
}

export interface BloomFilter {
  /** Raw bits as Uint8Array */
  bits: Uint8Array;
  /** Configuration */
  config: BloomFilterConfig;
  /** Number of entries added */
  count: number;
  /** Version string for cache busting */
  version: string;
}

export interface CreateBloomFilterOptions {
  /** Expected number of entries */
  capacity: number;
  /** Target false positive rate (default: 0.01 = 1%) */
  targetFpr?: number;
  /** Maximum filter size in bytes (default: 16KB to stay under 32KB snippet limit) */
  maxSizeBytes?: number;
  /** Version string (default: timestamp) */
  version?: string;
}

export interface SerializedFilter {
  /** Base64-encoded filter bits */
  filter: string;
  /** Number of hash functions */
  k: number;
  /** Number of bits */
  m: number;
  /** Version for cache busting */
  version: string;
  /** Metadata */
  meta: {
    count: number;
    capacity: number;
    targetFpr: number;
    expectedFpr: number;
    sizeBytes: number;
  };
}

/**
 * Calculate optimal number of bits (m) for given n and target FPR
 * Formula: m = -n * ln(p) / (ln(2)^2)
 */
export function calculateOptimalBits(n: number, targetFpr: number): number {
  const ln2Squared = Math.LN2 * Math.LN2;
  return Math.ceil((-n * Math.log(targetFpr)) / ln2Squared);
}

/**
 * Calculate optimal number of hash functions (k) for given m and n
 * Formula: k = (m/n) * ln(2)
 */
export function calculateOptimalK(m: number, n: number): number {
  return Math.max(1, Math.round((m / n) * Math.LN2));
}

/**
 * Calculate expected false positive rate
 * Formula: p = (1 - e^(-kn/m))^k
 */
export function calculateExpectedFpr(m: number, n: number, k: number): number {
  const exponent = (-k * n) / m;
  return Math.pow(1 - Math.exp(exponent), k);
}

/**
 * Set a bit in the filter
 */
function setBit(bits: Uint8Array, index: number): void {
  const byteIndex = index >>> 3;
  const bitOffset = index & 7;
  const current = bits[byteIndex];
  if (current !== undefined) {
    bits[byteIndex] = current | (1 << bitOffset);
  }
}

/**
 * Check if a bit is set
 */
function getBit(bits: Uint8Array, index: number): boolean {
  const byteIndex = index >>> 3;
  const bitOffset = index & 7;
  return ((bits[byteIndex] ?? 0) & (1 << bitOffset)) !== 0;
}

/**
 * Create a new bloom filter
 */
export function createBloomFilter(options: CreateBloomFilterOptions): BloomFilter {
  const {
    capacity,
    targetFpr = DEFAULT_BLOOM_FPR,
    maxSizeBytes = DEFAULT_BLOOM_MAX_SIZE_BYTES,
    version = Date.now().toString(TIMESTAMP_ENCODING_BASE),
  } = options;

  // Calculate optimal parameters
  let m = calculateOptimalBits(capacity, targetFpr);
  let k = calculateOptimalK(m, capacity);

  // Ensure we don't exceed max size
  const maxBits = maxSizeBytes * 8;
  if (m > maxBits) {
    m = maxBits;
    k = calculateOptimalK(m, capacity);
  }

  // Round m up to nearest byte
  m = Math.ceil(m / 8) * 8;

  const expectedFpr = calculateExpectedFpr(m, capacity, k);

  const config: BloomFilterConfig = {
    m,
    k,
    targetFpr,
    expectedFpr,
    capacity,
  };

  const bits = new Uint8Array(Math.ceil(m / 8));

  return {
    bits,
    config,
    count: 0,
    version,
  };
}

/**
 * Add an entity ID to the bloom filter
 */
export function addToFilter(filter: BloomFilter, entityId: string): void {
  const [h1, h2] = fnv1aDoubleHash(entityId);
  const { m, k } = filter.config;

  for (let i = 0; i < k; i++) {
    const pos = ((h1 + Math.imul(i, h2)) >>> 0) % m;
    setBit(filter.bits, pos);
  }

  filter.count++;
}

/**
 * Add multiple entity IDs to the bloom filter
 */
export function addManyToFilter(filter: BloomFilter, entityIds: string[]): void {
  for (const id of entityIds) {
    addToFilter(filter, id);
  }
}

/**
 * Check if an entity ID might exist in the filter
 * Returns false if definitely not in set, true if might be in set
 */
export function mightExist(filter: BloomFilter, entityId: string): boolean {
  const [h1, h2] = fnv1aDoubleHash(entityId);
  const { m, k } = filter.config;

  for (let i = 0; i < k; i++) {
    const pos = ((h1 + Math.imul(i, h2)) >>> 0) % m;
    if (!getBit(filter.bits, pos)) {
      return false;
    }
  }

  return true;
}

// Base64 encoding/decoding imported from '../core' (toBase64/fromBase64)

/**
 * Serialize bloom filter to transportable format
 */
export function serializeFilter(filter: BloomFilter): SerializedFilter {
  return {
    filter: toBase64(filter.bits),
    k: filter.config.k,
    m: filter.config.m,
    version: filter.version,
    meta: {
      count: filter.count,
      capacity: filter.config.capacity,
      targetFpr: filter.config.targetFpr,
      expectedFpr: filter.config.expectedFpr,
      sizeBytes: filter.bits.length,
    },
  };
}

/**
 * Deserialize bloom filter from base64
 */
export function deserializeFilter(serialized: SerializedFilter): BloomFilter {
  const bits = fromBase64(serialized.filter);

  return {
    bits,
    config: {
      m: serialized.m,
      k: serialized.k,
      targetFpr: serialized.meta.targetFpr,
      expectedFpr: serialized.meta.expectedFpr,
      capacity: serialized.meta.capacity,
    },
    count: serialized.meta.count,
    version: serialized.version,
  };
}

/**
 * Merge two bloom filters (for incremental updates)
 * Both filters must have the same configuration
 */
export function mergeFilters(a: BloomFilter, b: BloomFilter): BloomFilter {
  if (a.config.m !== b.config.m || a.config.k !== b.config.k) {
    throw new Error("Cannot merge filters with different configurations");
  }

  const merged = createBloomFilter({
    capacity: a.config.capacity,
    targetFpr: a.config.targetFpr,
    version: `${a.version}-${b.version}`,
  });

  // Ensure merged has same config as inputs
  merged.config.m = a.config.m;
  merged.config.k = a.config.k;
  merged.bits = new Uint8Array(a.bits.length);

  // OR the bits together
  for (let i = 0; i < a.bits.length; i++) {
    merged.bits[i] = (a.bits[i] ?? 0) | (b.bits[i] ?? 0);
  }

  merged.count = a.count + b.count; // Approximate

  return merged;
}

/**
 * Create an incremental update filter
 * Returns a smaller filter that can be merged with the main filter
 */
export function createIncrementalFilter(
  mainFilter: BloomFilter,
  newEntityIds: string[]
): BloomFilter {
  const incremental = createBloomFilter({
    capacity: mainFilter.config.capacity,
    targetFpr: mainFilter.config.targetFpr,
    version: Date.now().toString(TIMESTAMP_ENCODING_BASE),
  });

  // Copy config to match main filter exactly
  incremental.config.m = mainFilter.config.m;
  incremental.config.k = mainFilter.config.k;
  incremental.bits = new Uint8Array(mainFilter.bits.length);

  // Add new entities
  addManyToFilter(incremental, newEntityIds);

  return incremental;
}

/**
 * Get filter statistics
 */
export function getFilterStats(filter: BloomFilter): {
  sizeBytes: number;
  sizeKB: number;
  bitsSet: number;
  fillRate: number;
  entriesAdded: number;
  capacity: number;
  utilizationPercent: number;
  expectedFpr: number;
  k: number;
  m: number;
} {
  let bitsSet = 0;
  for (let i = 0; i < filter.bits.length; i++) {
    let byte = filter.bits[i];
    while (byte) {
      bitsSet += byte & 1;
      byte >>= 1;
    }
  }

  return {
    sizeBytes: filter.bits.length,
    sizeKB: filter.bits.length / BYTES_PER_KB,
    bitsSet,
    fillRate: bitsSet / filter.config.m,
    entriesAdded: filter.count,
    capacity: filter.config.capacity,
    utilizationPercent: (filter.count / filter.config.capacity) * PERCENTAGE_MULTIPLIER,
    expectedFpr: filter.config.expectedFpr,
    k: filter.config.k,
    m: filter.config.m,
  };
}

/**
 * Estimate actual false positive rate by testing random strings
 */
export function estimateFpr(
  filter: BloomFilter,
  testCount: number = DEFAULT_FPR_TEST_COUNT
): number {
  let falsePositives = 0;

  for (let i = 0; i < testCount; i++) {
    // Generate random string that's unlikely to be in the filter
    const randomId = `__test__${Math.random().toString(TIMESTAMP_ENCODING_BASE)}__${i}`;
    if (mightExist(filter, randomId)) {
      falsePositives++;
    }
  }

  return falsePositives / testCount;
}

/**
 * Extract entity ID from URL pathname
 * Supports patterns:
 * - /entities/{id}
 * - /api/v1/entities/{id}
 * - /graph/{type}/{id}
 */
export function extractEntityId(pathname: string): string | null {
  // Pattern: /entities/{id}
  const entityMatch = pathname.match(/^\/entities\/([^\/]+)$/);
  if (entityMatch) return entityMatch[1] ?? null;

  // Pattern: /api/v1/entities/{id}
  const apiMatch = pathname.match(/^\/api\/v\d+\/entities\/([^\/]+)$/);
  if (apiMatch) return apiMatch[1] ?? null;

  // Pattern: /graph/{type}/{id}
  const graphMatch = pathname.match(/^\/graph\/[^\/]+\/([^\/]+)$/);
  if (graphMatch) return graphMatch[1] ?? null;

  return null;
}
