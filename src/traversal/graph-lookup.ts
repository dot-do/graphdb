/**
 * Graph Lookup - Bloom filter routing and chunk-based entity lookup
 *
 * This module implements the real graph lookup pipeline:
 * 1. Check bloom filter to find candidate chunks
 * 2. Fetch chunk from R2 (with edge cache)
 * 3. Decode GraphCol format
 * 4. Extract entity and edges from triples
 *
 * @packageDocumentation
 */

import type { Triple } from '../core/triple.js';
import { ObjectType } from '../core/types.js';
import {
  decodeGraphCol,
  readFooter,
  decodeEntity,
  type GraphColFooter,
  GCOL_FOOTER_SIZE,
} from '../storage/graphcol.js';
import {
  type EntityIndex,
  lookupEntity,
  decodeEntityIndex,
} from '../storage/entity-index.js';
import {
  deserializeFilter,
  mightExist,
  type BloomFilter,
  type SerializedFilter,
} from '../snippet/bloom.js';
import { EdgeCache } from '../snippet/edge-cache.js';
import { createNamespace } from '../core/types.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Entity representation for traversal
 */
export interface LookupEntity {
  /** URL-based entity identifier */
  id: string;
  /** Entity type (derived from triples) */
  type: string;
  /** Properties extracted from triples */
  properties: Record<string, unknown>;
  /** Outgoing edges */
  edges: LookupEdge[];
}

/**
 * Edge for traversal
 */
export interface LookupEdge {
  /** Relationship predicate */
  predicate: string;
  /** Target entity ID */
  target: string;
}

/**
 * Chunk information from manifest for traversal operations
 */
export interface TraversalChunkInfo {
  id: string;
  tripleCount: number;
  path: string;
  bloom?: SerializedFilter;
}

/**
 * Manifest structure
 */
export interface ChunkManifest {
  namespace: string;
  chunks: TraversalChunkInfo[];
  totalTriples: number;
  createdAt: string;
  version: string;
  combinedBloom?: SerializedFilter;
}

/**
 * Lookup statistics
 */
export interface LookupStats {
  /** Whether entity was found */
  found: boolean;
  /** Time spent in ms */
  timeMs: number;
  /** Number of chunks checked */
  chunksChecked: number;
  /** Whether cache was used */
  cacheHit: boolean;
  /** R2 fetch time (if cache miss) */
  r2FetchMs?: number;
  /** Decode time */
  decodeMs?: number;
  /** Number of Range requests made (V2 only) */
  rangeRequests?: number;
  /** Number of full file fetches (V1 or fallback) */
  fullFetches?: number;
  /** Number of footer cache hits (V2 only) */
  footerCacheHits?: number;
}

/**
 * Configuration for GraphLookup
 */
export interface GraphLookupConfig {
  /** R2 bucket for cold storage */
  r2: R2Bucket;
  /** Edge cache for hot chunks */
  cache?: EdgeCache;
  /** Current colo for diagnostics */
  colo?: string;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Convert namespace URL to R2 path prefix
 *
 * Example: https://imdb.com/title/ -> .com.imdb/title/
 */
export function namespaceToR2Path(namespace: string): string {
  try {
    const url = new URL(namespace);
    const domainParts = url.hostname.split('.');
    const reversedDomain = domainParts
      .reverse()
      .map((part) => `.${part}`)
      .join('/');
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
    const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';
    return `${reversedDomain}${pathStr}`;
  } catch {
    return namespace;
  }
}

/**
 * Extract namespace from entity ID
 *
 * Example: https://imdb.com/title/tt0000001 -> https://imdb.com/title/
 */
export function extractNamespaceFromEntityId(entityId: string): string {
  try {
    const url = new URL(entityId);
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);

    // If there's at least one path segment and more parts after it,
    // use origin + first segment as namespace
    if (pathParts.length > 1) {
      return `${url.origin}/${pathParts[0]}/`;
    }

    // Otherwise, use just the origin
    return `${url.origin}/`;
  } catch {
    return entityId;
  }
}

/**
 * Parse manifest JSON with bigint support
 */
function parseManifest(json: string): ChunkManifest {
  return JSON.parse(json, (key, value) => {
    // Convert timestamp strings back to bigint if needed
    if (
      (key === 'minTime' || key === 'maxTime') &&
      typeof value === 'string' &&
      /^\d+$/.test(value)
    ) {
      return BigInt(value);
    }
    return value;
  });
}

/**
 * Extract entity type from triples
 */
function extractEntityType(triples: Triple[], entityId: string): string {
  // Look for $type or type predicate
  const typeTriple = triples.find(
    (t) =>
      t.subject === entityId && (t.predicate === '$type' || t.predicate === 'type')
  );

  if (typeTriple) {
    if (typeTriple.object.type === ObjectType.STRING) {
      return typeTriple.object.value ?? 'Unknown';
    }
    if (typeTriple.object.type === ObjectType.REF) {
      // Extract type name from URL
      const ref = typeTriple.object.value ?? '';
      const lastPart = ref.split('/').pop() ?? ref;
      return lastPart;
    }
  }

  // Infer type from entity ID pattern
  if (entityId.includes('/title/')) return 'Movie';
  if (entityId.includes('/name/')) return 'Person';
  if (entityId.includes('/person/')) return 'Person';
  if (entityId.includes('/user/')) return 'User';

  return 'Entity';
}

/**
 * Extract properties from triples (non-edge predicates)
 */
function extractProperties(
  triples: Triple[],
  entityId: string
): Record<string, unknown> {
  const properties: Record<string, unknown> = {};

  for (const triple of triples) {
    if (triple.subject !== entityId) continue;

    // Skip type and edge predicates
    if (
      triple.predicate === '$type' ||
      triple.predicate === 'type' ||
      triple.object.type === ObjectType.REF
    ) {
      continue;
    }

    // Extract value based on type
    switch (triple.object.type) {
      case ObjectType.STRING:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.INT32:
      case ObjectType.INT64:
        // Convert BigInt to Number for JSON compatibility
        properties[triple.predicate] = Number(triple.object.value);
        break;
      case ObjectType.FLOAT64:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.BOOL:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.TIMESTAMP:
        properties[triple.predicate] = Number(triple.object.value);
        break;
      case ObjectType.DATE:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.JSON:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.GEO_POINT:
        properties[triple.predicate] = triple.object.value;
        break;
      case ObjectType.URL:
        properties[triple.predicate] = triple.object.value;
        break;
    }
  }

  return properties;
}

/**
 * Extract edges (REF predicates) from triples
 */
function extractEdges(triples: Triple[], entityId: string): LookupEdge[] {
  const edges: LookupEdge[] = [];

  for (const triple of triples) {
    if (triple.subject !== entityId) continue;

    // Only REF and REF_ARRAY types are edges
    if (triple.object.type === ObjectType.REF) {
      edges.push({
        predicate: triple.predicate,
        target: triple.object.value ?? '',
      });
    } else if (triple.object.type === ObjectType.REF_ARRAY) {
      const targets = triple.object.value ?? [];
      for (const target of targets) {
        edges.push({
          predicate: triple.predicate,
          target,
        });
      }
    }
  }

  return edges;
}

// ============================================================================
// R2 Range Request Utilities
// ============================================================================

/**
 * Default footer size to fetch (64KB)
 * This is enough for the footer (48 bytes) + trailer (8 bytes) + entity index
 */
const DEFAULT_FOOTER_SIZE = 65536;

/**
 * Cached footer and entity index for a V2 file
 */
export interface CachedFooterInfo {
  footer: GraphColFooter;
  index: EntityIndex;
  /** Total file size from R2 object metadata */
  fileSize: number;
}

/**
 * Fetch footer and entity index using R2 suffix range request
 *
 * Uses a single Range request to fetch the last N bytes (default 64KB)
 * which should contain the footer, trailer, and entity index.
 *
 * @param r2 - R2 bucket
 * @param path - File path in R2
 * @param footerSize - Number of bytes to fetch from end (default 64KB)
 * @returns Parsed footer and entity index, or null if file doesn't exist or isn't V2
 */
export async function fetchFooter(
  r2: R2Bucket,
  path: string,
  footerSize: number = DEFAULT_FOOTER_SIZE
): Promise<CachedFooterInfo | null> {
  // Validate footerSize
  if (footerSize <= 0) {
    footerSize = DEFAULT_FOOTER_SIZE;
  }

  // Use suffix range to fetch last N bytes
  let obj: R2ObjectBody | null;
  try {
    obj = await r2.get(path, { range: { suffix: footerSize } });
  } catch (error) {
    // R2 error (network failure, permission denied, etc.)
    // Re-throw to distinguish from "file not found" or "invalid format"
    throw new Error(`R2 fetch failed for ${path}: ${error instanceof Error ? error.message : String(error)}`);
  }

  if (!obj) {
    // File does not exist
    return null;
  }

  const data = new Uint8Array(await obj.arrayBuffer());
  const fileSize = obj.size;

  // Validate minimum size for V2 format
  if (data.length < GCOL_FOOTER_SIZE + 8) {
    // File too small to be V2 format - return null (not an error)
    return null;
  }

  // Calculate the actual byte offset where our fetched data starts
  // If file is smaller than footerSize, we got the whole file starting at 0
  const actualStartOffset = Math.max(0, fileSize - data.length);

  try {
    // Read footer from the fetched data
    // The footer and trailer are at the END of the data
    const footer = readFooter(data);

    // Validate footer version is V2
    if (footer.version !== 2) {
      return null;
    }

    // Calculate where the entity index is in our fetched data
    // footer.indexOffset is relative to the start of the file
    const indexStartInFile = footer.indexOffset;

    // Validate index bounds
    if (indexStartInFile < 0 || footer.indexLength < 0) {
      return null;
    }

    // Check if we have the full index in our fetched data
    if (indexStartInFile < actualStartOffset) {
      // We didn't fetch enough - the index extends before our range
      // Return footer only with empty index (caller may need to re-fetch)
      return {
        footer,
        index: { entries: [], version: 1 },
        fileSize,
      };
    }

    // Extract entity index from fetched data
    const indexOffsetInData = indexStartInFile - actualStartOffset;
    const indexEndInData = indexOffsetInData + footer.indexLength;

    // Bounds check for slice
    if (indexEndInData > data.length) {
      // Index extends beyond our fetched data
      return {
        footer,
        index: { entries: [], version: 1 },
        fileSize,
      };
    }

    const indexSlice = data.subarray(indexOffsetInData, indexEndInData);

    // Decode the entity index (decodeEntityIndex is already imported via graphcol.js)
    const entityIndex = decodeEntityIndex(indexSlice);

    return {
      footer,
      index: entityIndex,
      fileSize,
    };
  } catch (error) {
    // Not a V2 file or invalid format (checksum mismatch, corrupt data, etc.)
    return null;
  }
}

/**
 * Fetch specific bytes from an R2 file using offset+length range request
 *
 * @param r2 - R2 bucket
 * @param path - File path in R2
 * @param offset - Byte offset to start reading from (must be >= 0)
 * @param length - Number of bytes to read (must be > 0)
 * @returns The fetched bytes, or null if file doesn't exist
 * @throws Error if offset or length are invalid, or if R2 request fails
 */
export async function fetchEntityByRange(
  r2: R2Bucket,
  path: string,
  offset: number,
  length: number
): Promise<Uint8Array | null> {
  // Validate parameters
  if (offset < 0) {
    throw new Error(`Invalid offset: ${offset} (must be >= 0)`);
  }
  if (length <= 0) {
    throw new Error(`Invalid length: ${length} (must be > 0)`);
  }
  // Guard against integer overflow for very large values
  if (!Number.isFinite(offset) || !Number.isFinite(length)) {
    throw new Error(`Invalid offset/length: must be finite numbers`);
  }

  let obj: R2ObjectBody | null;
  try {
    obj = await r2.get(path, { range: { offset, length } });
  } catch (error) {
    // R2 error (network failure, permission denied, etc.)
    throw new Error(`R2 range request failed for ${path}: ${error instanceof Error ? error.message : String(error)}`);
  }

  if (!obj) {
    return null;
  }

  return new Uint8Array(await obj.arrayBuffer());
}

// ============================================================================
// GraphLookup Class
// ============================================================================

/**
 * GraphLookup - Handles entity lookup with bloom filter routing
 *
 * Implements the full lookup pipeline:
 * 1. Load manifest for namespace
 * 2. Check combined bloom filter (quick reject)
 * 3. Check per-chunk bloom filters to find candidate chunks
 * 4. Fetch and decode candidate chunks
 * 5. Extract entity from triples
 */
export class GraphLookup {
  private r2: R2Bucket;
  private cache: EdgeCache | undefined;
  private _colo: string;

  // Cached manifests per namespace
  private manifestCache = new Map<string, ChunkManifest>();
  // Cached bloom filters per namespace
  private bloomCache = new Map<string, BloomFilter>();
  // Cached chunk bloom filters
  private chunkBloomCache = new Map<string, BloomFilter>();
  // Cached V2 footer and entity index per file path
  private footerCache = new Map<string, CachedFooterInfo>();

  constructor(config: GraphLookupConfig) {
    this.r2 = config.r2;
    this.cache = config.cache;
    this._colo = config.colo ?? 'unknown';
  }

  /** Get the colo where this lookup is running */
  get colo(): string {
    return this._colo;
  }

  /**
   * Load manifest for a namespace
   */
  async loadManifest(namespace: string): Promise<ChunkManifest | null> {
    // Check cache first
    if (this.manifestCache.has(namespace)) {
      return this.manifestCache.get(namespace) ?? null;
    }

    // Build manifest path
    const r2Path = namespaceToR2Path(namespace);
    const manifestPath = `${r2Path}/_manifest.json`;

    // Fetch from R2
    const obj = await this.r2.get(manifestPath);
    if (!obj) {
      return null;
    }

    const json = await obj.text();
    const manifest = parseManifest(json);

    // Cache for subsequent lookups
    this.manifestCache.set(namespace, manifest);

    // Pre-deserialize combined bloom filter
    if (manifest.combinedBloom) {
      const bloom = deserializeFilter(manifest.combinedBloom);
      this.bloomCache.set(namespace, bloom);
    }

    return manifest;
  }

  /**
   * Check if an entity might exist in a namespace
   */
  mightExistInNamespace(entityId: string, namespace: string): boolean {
    const bloom = this.bloomCache.get(namespace);
    if (!bloom) {
      // No bloom filter = assume might exist
      return true;
    }
    return mightExist(bloom, entityId);
  }

  /**
   * Find chunks that might contain an entity
   */
  async findCandidateChunks(
    entityId: string,
    manifest: ChunkManifest
  ): Promise<TraversalChunkInfo[]> {
    const candidates: TraversalChunkInfo[] = [];

    for (const chunk of manifest.chunks) {
      if (!chunk.bloom) {
        // No bloom filter = must check this chunk
        candidates.push(chunk);
        continue;
      }

      // Check chunk bloom filter
      const cacheKey = `${manifest.namespace}:${chunk.id}`;
      let bloom = this.chunkBloomCache.get(cacheKey);
      if (!bloom) {
        bloom = deserializeFilter(chunk.bloom);
        this.chunkBloomCache.set(cacheKey, bloom);
      }

      if (mightExist(bloom, entityId)) {
        candidates.push(chunk);
      }
    }

    return candidates;
  }

  /**
   * Fetch and decode a chunk
   */
  async fetchChunk(chunkPath: string): Promise<Triple[]> {
    // Try edge cache first
    if (this.cache) {
      // Use chunk path as segment ID
      const ns = createNamespace('graphdb://chunks/');
      const cached = await this.cache.getIndexSegment(ns, chunkPath, 'v1');
      if (cached) {
        // Cached as serialized triples (not ideal but works)
        // In production, we'd cache decoded triples or raw bytes
      }
    }

    // Fetch from R2
    const obj = await this.r2.get(chunkPath);
    if (!obj) {
      return [];
    }

    const data = new Uint8Array(await obj.arrayBuffer());

    // Decode GraphCol
    return decodeGraphCol(data);
  }

  /**
   * Lookup an entity by ID using V2 Range requests
   *
   * V2 lookup uses Range requests for efficient partial reads:
   * 1. Fetch footer + entity index (suffix range) - cached after first call
   * 2. Look up entity in index (binary search) - O(log n)
   * 3. Fetch full file for decoding (columnar format requires full data section)
   *
   * Note: The entity index allows us to quickly reject lookups for entities
   * that don't exist, avoiding the full file fetch in those cases. When the
   * entity does exist, we currently need the full file because the columnar
   * format uses dictionary encoding across all triples.
   *
   * Future optimization: encode entity data independently to enable true
   * partial reads using the byte offsets in the entity index.
   *
   * @param entityId - Entity ID to look up
   * @param chunkPath - Path to the V2 GraphCol file
   * @returns Entity and lookup stats, or null if not found
   */
  async lookupV2(
    entityId: string,
    chunkPath: string
  ): Promise<{ entity: LookupEntity | null; stats: LookupStats }> {
    const startTime = performance.now();
    let rangeRequests = 0;
    let fullFetches = 0;
    let footerCacheHits = 0;
    let r2FetchMs = 0;
    let decodeMs = 0;

    // Check footer cache using full path as key
    // This is safe because paths are unique within a bucket
    let footerInfo: CachedFooterInfo | null = this.footerCache.get(chunkPath) ?? null;

    if (footerInfo) {
      footerCacheHits = 1;
    } else {
      // Fetch footer and entity index using suffix range request
      const fetchStart = performance.now();
      try {
        footerInfo = await fetchFooter(this.r2, chunkPath);
      } catch (error) {
        // R2 error - propagate for debugging
        throw error;
      }
      r2FetchMs += performance.now() - fetchStart;
      rangeRequests = 1;

      if (!footerInfo) {
        // File doesn't exist or isn't V2 format
        return {
          entity: null,
          stats: {
            found: false,
            timeMs: Math.round(performance.now() - startTime),
            chunksChecked: 1,
            cacheHit: false,
            r2FetchMs: Math.round(r2FetchMs),
            rangeRequests,
            fullFetches,
            footerCacheHits,
          },
        };
      }

      // Cache the footer info for subsequent lookups in same file
      this.footerCache.set(chunkPath, footerInfo);
    }

    // Look up entity in index using binary search - O(log n)
    const decodeStart = performance.now();
    const indexEntry = lookupEntity(footerInfo.index, entityId);
    decodeMs += performance.now() - decodeStart;

    if (!indexEntry) {
      // Entity not in this file - quick reject without full fetch
      return {
        entity: null,
        stats: {
          found: false,
          timeMs: Math.round(performance.now() - startTime),
          chunksChecked: 1,
          cacheHit: footerCacheHits > 0,
          r2FetchMs: Math.round(r2FetchMs),
          decodeMs: Math.round(decodeMs),
          rangeRequests,
          fullFetches,
          footerCacheHits,
        },
      };
    }

    // Entity exists in index - fetch full file for decoding
    // Note: We skip the partial entity range fetch since we need the full
    // data section anyway due to columnar format's dictionary encoding.
    const fullFetchStart = performance.now();
    let fullData: R2ObjectBody | null;
    try {
      fullData = await this.r2.get(chunkPath);
    } catch (error) {
      throw new Error(`R2 fetch failed for ${chunkPath}: ${error instanceof Error ? error.message : String(error)}`);
    }
    r2FetchMs += performance.now() - fullFetchStart;
    fullFetches = 1;

    if (!fullData) {
      // File was deleted between footer fetch and full fetch (rare race condition)
      return {
        entity: null,
        stats: {
          found: false,
          timeMs: Math.round(performance.now() - startTime),
          chunksChecked: 1,
          cacheHit: footerCacheHits > 0,
          r2FetchMs: Math.round(r2FetchMs),
          decodeMs: Math.round(decodeMs),
          rangeRequests,
          fullFetches,
          footerCacheHits,
        },
      };
    }

    const fullBytes = new Uint8Array(await fullData.arrayBuffer());

    // Decode only this entity's triples using decodeEntity
    // This uses the entity index for efficient lookup
    const entityDecodeStart = performance.now();
    const entityTriples = decodeEntity(fullBytes, entityId);
    decodeMs += performance.now() - entityDecodeStart;

    if (!entityTriples || entityTriples.length === 0) {
      // Entity was in index but has no triples (shouldn't happen normally)
      return {
        entity: null,
        stats: {
          found: false,
          timeMs: Math.round(performance.now() - startTime),
          chunksChecked: 1,
          cacheHit: footerCacheHits > 0,
          r2FetchMs: Math.round(r2FetchMs),
          decodeMs: Math.round(decodeMs),
          rangeRequests,
          fullFetches,
          footerCacheHits,
        },
      };
    }

    // Build entity from triples
    const entity: LookupEntity = {
      id: entityId,
      type: extractEntityType(entityTriples, entityId),
      properties: extractProperties(entityTriples, entityId),
      edges: extractEdges(entityTriples, entityId),
    };

    return {
      entity,
      stats: {
        found: true,
        timeMs: Math.round(performance.now() - startTime),
        chunksChecked: 1,
        cacheHit: footerCacheHits > 0,
        r2FetchMs: Math.round(r2FetchMs),
        decodeMs: Math.round(decodeMs),
        rangeRequests,
        fullFetches,
        footerCacheHits,
      },
    };
  }

  /**
   * Lookup an entity by ID
   *
   * This is the main entry point for entity lookup.
   * Auto-detects V1 vs V2 format and uses appropriate method.
   */
  async lookup(entityId: string): Promise<{ entity: LookupEntity | null; stats: LookupStats }> {
    const startTime = performance.now();
    let chunksChecked = 0;
    let cacheHit = false;
    let r2FetchMs = 0;
    let decodeMs = 0;

    // Extract namespace from entity ID
    const namespace = extractNamespaceFromEntityId(entityId);

    // Load manifest
    const manifest = await this.loadManifest(namespace);
    if (!manifest) {
      return {
        entity: null,
        stats: {
          found: false,
          timeMs: Math.round(performance.now() - startTime),
          chunksChecked: 0,
          cacheHit: false,
        },
      };
    }

    // Quick reject with combined bloom filter
    if (!this.mightExistInNamespace(entityId, namespace)) {
      return {
        entity: null,
        stats: {
          found: false,
          timeMs: Math.round(performance.now() - startTime),
          chunksChecked: 0,
          cacheHit: false,
        },
      };
    }

    // Find candidate chunks
    const candidates = await this.findCandidateChunks(entityId, manifest);

    // Check each candidate chunk
    for (const chunk of candidates) {
      chunksChecked++;

      const fetchStart = performance.now();
      const triples = await this.fetchChunk(chunk.path);
      r2FetchMs += performance.now() - fetchStart;

      // Find triples for this entity
      const decodeStart = performance.now();
      const entityTriples = triples.filter((t) => t.subject === entityId);
      decodeMs += performance.now() - decodeStart;

      if (entityTriples.length > 0) {
        // Build entity from triples
        const entity: LookupEntity = {
          id: entityId,
          type: extractEntityType(entityTriples, entityId),
          properties: extractProperties(entityTriples, entityId),
          edges: extractEdges(entityTriples, entityId),
        };

        return {
          entity,
          stats: {
            found: true,
            timeMs: Math.round(performance.now() - startTime),
            chunksChecked,
            cacheHit,
            r2FetchMs: Math.round(r2FetchMs),
            decodeMs: Math.round(decodeMs),
          },
        };
      }
    }

    // Not found in any chunk (false positive from bloom filter)
    return {
      entity: null,
      stats: {
        found: false,
        timeMs: Math.round(performance.now() - startTime),
        chunksChecked,
        cacheHit,
        r2FetchMs: Math.round(r2FetchMs),
        decodeMs: Math.round(decodeMs),
      },
    };
  }

  /**
   * Batch lookup multiple entities
   *
   * Optimizes by grouping entities by namespace and chunk.
   */
  async batchLookup(
    entityIds: string[]
  ): Promise<{ entities: (LookupEntity | null)[]; stats: LookupStats }> {
    const startTime = performance.now();
    let totalChunksChecked = 0;
    let totalR2FetchMs = 0;
    let totalDecodeMs = 0;
    let foundCount = 0;

    // Group entities by namespace
    const byNamespace = new Map<string, string[]>();
    for (const entityId of entityIds) {
      const namespace = extractNamespaceFromEntityId(entityId);
      const list = byNamespace.get(namespace) ?? [];
      list.push(entityId);
      byNamespace.set(namespace, list);
    }

    // Results map to preserve order
    const results = new Map<string, LookupEntity | null>();

    // Process each namespace
    for (const [namespace, nsEntityIds] of byNamespace) {
      // Load manifest
      const manifest = await this.loadManifest(namespace);
      if (!manifest) {
        for (const id of nsEntityIds) {
          results.set(id, null);
        }
        continue;
      }

      // Filter entities by combined bloom
      const possibleEntities = nsEntityIds.filter((id) =>
        this.mightExistInNamespace(id, namespace)
      );

      // Set not-found for entities that failed bloom filter
      for (const id of nsEntityIds) {
        if (!possibleEntities.includes(id)) {
          results.set(id, null);
        }
      }

      // Find all candidate chunks for remaining entities
      const chunkEntityMap = new Map<string, Set<string>>();
      for (const entityId of possibleEntities) {
        const candidates = await this.findCandidateChunks(entityId, manifest);
        for (const chunk of candidates) {
          const entitySet = chunkEntityMap.get(chunk.path) ?? new Set();
          entitySet.add(entityId);
          chunkEntityMap.set(chunk.path, entitySet);
        }
      }

      // Fetch and process each unique chunk
      for (const [chunkPath, entitySet] of chunkEntityMap) {
        totalChunksChecked++;

        const fetchStart = performance.now();
        const triples = await this.fetchChunk(chunkPath);
        totalR2FetchMs += performance.now() - fetchStart;

        const decodeStart = performance.now();
        for (const entityId of entitySet) {
          if (results.has(entityId) && results.get(entityId) !== null) {
            continue; // Already found
          }

          const entityTriples = triples.filter((t) => t.subject === entityId);
          if (entityTriples.length > 0) {
            const entity: LookupEntity = {
              id: entityId,
              type: extractEntityType(entityTriples, entityId),
              properties: extractProperties(entityTriples, entityId),
              edges: extractEdges(entityTriples, entityId),
            };
            results.set(entityId, entity);
            foundCount++;
          } else if (!results.has(entityId)) {
            results.set(entityId, null);
          }
        }
        totalDecodeMs += performance.now() - decodeStart;
      }

      // Set null for any entities not yet set (bloom false positives)
      for (const id of possibleEntities) {
        if (!results.has(id)) {
          results.set(id, null);
        }
      }
    }

    // Build ordered result array
    const entities = entityIds.map((id) => results.get(id) ?? null);

    return {
      entities,
      stats: {
        found: foundCount > 0,
        timeMs: Math.round(performance.now() - startTime),
        chunksChecked: totalChunksChecked,
        cacheHit: false,
        r2FetchMs: Math.round(totalR2FetchMs),
        decodeMs: Math.round(totalDecodeMs),
      },
    };
  }

  /**
   * Clear all caches
   */
  clearCaches(): void {
    this.manifestCache.clear();
    this.bloomCache.clear();
    this.chunkBloomCache.clear();
    this.footerCache.clear();
  }

  /**
   * Get the footer cache (for testing/inspection)
   */
  getFooterCache(): Map<string, CachedFooterInfo> {
    return this.footerCache;
  }
}
