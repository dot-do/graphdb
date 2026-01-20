/**
 * IndexStore - Unified Index Management for GraphDB
 *
 * Hybrid architecture:
 * - Data lives in GraphCol chunks (R2, immutable, Edge Cache)
 * - Indexes live in JSON/SQLite (DO storage, mutable, in-memory)
 *
 * Index Types:
 * 1. SPO (Subject-Predicate-Object): Entity lookup → built into GraphCol footer
 * 2. POS (Predicate-Object-Subject): Find entities by predicate/value
 * 3. OSP (Object-Subject-Predicate): Reverse reference lookup
 * 4. FTS (Full-Text Search): Inverted text index
 * 5. GEO (Geospatial): Geohash prefix tree
 * 6. VEC (Vector): HNSW graph for embeddings (optional)
 *
 * Storage Strategy:
 * - Hot path: DO SQLite tables (indexed, fast queries)
 * - Cold path: JSON files in R2 (loaded on demand)
 * - Sync: On compaction, merge DO indexes → R2 JSON
 *
 * @packageDocumentation
 */

import type { Triple } from '../core/triple.js';
import type { EntityId, Predicate } from '../core/types.js';
import { ObjectType } from '../core/types.js';
import { fnv1aHash } from '../core/hash.js';
import {
  encodeGeohash as coreEncodeGeohash,
  getGeohashNeighbors as coreGetGeohashNeighbors,
} from '../core/geo.js';

// ============================================================================
// INDEX TYPES
// ============================================================================

/**
 * POS Index Entry - maps predicate+value to subjects
 *
 * JSON format for storage:
 * {
 *   "name": ["https://example.com/person/1", "https://example.com/person/2"],
 *   "age:25": ["https://example.com/person/1"],
 *   "age:30": ["https://example.com/person/2"]
 * }
 */
export interface POSIndex {
  /** Version for cache invalidation */
  version: string;
  /** Map of "predicate" or "predicate:valueHash" → entity IDs */
  entries: Record<string, string[]>;
}

/**
 * OSP Index Entry - maps object references to subjects (reverse lookup)
 *
 * JSON format:
 * {
 *   "https://example.com/company/1": ["https://example.com/person/1", "https://example.com/person/2"]
 * }
 */
export interface OSPIndex {
  version: string;
  /** Map of object reference → entity IDs that reference it */
  entries: Record<string, string[]>;
}

/**
 * FTS Index - inverted index for full-text search
 *
 * JSON format:
 * {
 *   "hello": [{"id": "...", "predicate": "name", "score": 1.0}],
 *   "world": [{"id": "...", "predicate": "description", "score": 0.8}]
 * }
 */
export interface FTSPosting {
  entityId: string;
  predicate: string;
  /** TF-IDF or BM25 score */
  score: number;
}

export interface FTSIndex {
  version: string;
  /** Total document count (for IDF calculation) */
  documentCount: number;
  /** Map of term → postings list */
  terms: Record<string, FTSPosting[]>;
}

/**
 * Geo Index - geohash prefix tree
 *
 * JSON format:
 * {
 *   "9q8y": ["https://example.com/place/sf"],
 *   "9q8yh": ["https://example.com/place/downtown-sf"]
 * }
 */
export interface GeoIndex {
  version: string;
  /** Geohash precision used (default 6) */
  precision: number;
  /** Map of geohash prefix → entity IDs */
  cells: Record<string, string[]>;
}

/**
 * Vector Index Entry - single vector with HNSW connections
 *
 * Each entry represents a node in the HNSW graph with its embedding vector
 * and connections to other nodes organized by layer.
 */
export interface VectorIndexEntry {
  entityId: string;
  predicate: string;
  /** Vector embedding */
  vector: number[];
  /**
   * HNSW layer connections (entity IDs per layer)
   * Layer 0 is the base layer with the most connections.
   * Higher layers have fewer nodes for hierarchical navigation.
   */
  connections: string[][];
}

/**
 * Vector Index - HNSW graph for similarity search
 *
 * Implements Hierarchical Navigable Small World (HNSW) graph structure
 * for approximate nearest neighbor search with O(log n) complexity.
 */
export interface VectorIndex {
  version: string;
  /** Vector dimensions */
  dimensions: number;
  /**
   * HNSW parameter: max connections per node
   * Higher values improve search quality but increase memory usage.
   */
  m: number;
  /**
   * HNSW parameter: construction quality factor
   * Higher values improve construction quality but slow down insertion.
   */
  efConstruction: number;
  /** All vectors and connections */
  entries: VectorIndexEntry[];
}

// ============================================================================
// INDEX STORE INTERFACE
// ============================================================================

/**
 * Index query options
 */
export interface IndexQueryOptions {
  /** Maximum results */
  limit?: number;
  /** Cursor for pagination */
  cursor?: string;
}

/**
 * IndexStore - manages all secondary indexes
 */
export interface IndexStore {
  // --------------------------------------------
  // POS Index (Predicate-Object-Subject)
  // --------------------------------------------

  /**
   * Find entities with a specific predicate
   * @example getByPredicate('name') → all entities with 'name' predicate
   */
  getByPredicate(predicate: Predicate, options?: IndexQueryOptions): Promise<string[]>;

  /**
   * Find entities with predicate matching a value
   * @example getByPredicateValue('age', 25) → entities where age=25
   */
  getByPredicateValue(predicate: Predicate, value: unknown, options?: IndexQueryOptions): Promise<string[]>;

  /**
   * Find entities with predicate in value range
   * @example getByPredicateRange('age', 18, 30) → entities where 18 <= age <= 30
   */
  getByPredicateRange(
    predicate: Predicate,
    min: number | bigint | Date,
    max: number | bigint | Date,
    options?: IndexQueryOptions
  ): Promise<string[]>;

  // --------------------------------------------
  // OSP Index (Object-Subject-Predicate)
  // --------------------------------------------

  /**
   * Find entities that reference a target (reverse lookup)
   * @example getReferencesTo('https://company.com/1') → entities with REF to that URL
   */
  getReferencesTo(targetEntityId: EntityId, options?: IndexQueryOptions): Promise<string[]>;

  /**
   * Find entities that reference a target via specific predicate
   * @example getReferencesToByPredicate('https://company.com/1', 'worksAt')
   */
  getReferencesToByPredicate(
    targetEntityId: EntityId,
    predicate: Predicate,
    options?: IndexQueryOptions
  ): Promise<string[]>;

  // --------------------------------------------
  // FTS Index (Full-Text Search)
  // --------------------------------------------

  /**
   * Full-text search across all STRING fields
   * @example search('hello world') → entities matching text
   */
  search(query: string, options?: IndexQueryOptions): Promise<Array<{ entityId: string; score: number }>>;

  /**
   * Full-text search in specific predicate
   * @example searchInPredicate('description', 'hello world')
   */
  searchInPredicate(
    predicate: Predicate,
    query: string,
    options?: IndexQueryOptions
  ): Promise<Array<{ entityId: string; score: number }>>;

  // --------------------------------------------
  // Geo Index (Geospatial)
  // --------------------------------------------

  /**
   * Find entities within bounding box
   */
  queryGeoBBox(
    minLat: number,
    minLng: number,
    maxLat: number,
    maxLng: number,
    options?: IndexQueryOptions
  ): Promise<string[]>;

  /**
   * Find entities within radius of point
   */
  queryGeoRadius(
    centerLat: number,
    centerLng: number,
    radiusKm: number,
    options?: IndexQueryOptions
  ): Promise<string[]>;

  // --------------------------------------------
  // Vector Index (Similarity Search)
  // --------------------------------------------

  /**
   * Find k nearest neighbors by vector similarity
   *
   * Uses HNSW (Hierarchical Navigable Small World) graph for O(log n) search
   * complexity on larger datasets, falling back to brute force for small datasets.
   *
   * @param predicate - The predicate to search vectors for
   * @param queryVector - The query vector
   * @param k - Number of nearest neighbors to return
   * @param ef - Search beam width (higher = more accurate but slower). Default: max(k, 10)
   * @returns Array of {entityId, similarity} sorted by similarity descending
   */
  queryKNN(
    predicate: Predicate,
    queryVector: number[],
    k: number,
    ef?: number
  ): Promise<Array<{ entityId: string; similarity: number }>>;

  // --------------------------------------------
  // Index Maintenance
  // --------------------------------------------

  /**
   * Index a triple (called on write)
   */
  indexTriple(triple: Triple): Promise<void>;

  /**
   * Index multiple triples (batch)
   */
  indexTriples(triples: Triple[]): Promise<void>;

  /**
   * Remove triple from indexes (on delete)
   */
  unindexTriple(triple: Triple): Promise<void>;

  /**
   * Load indexes from R2 JSON files
   */
  loadFromR2(r2: R2Bucket, namespace: string): Promise<void>;

  /**
   * Save indexes to R2 JSON files
   */
  saveToR2(r2: R2Bucket, namespace: string): Promise<void>;

  /**
   * Get index statistics
   */
  getStats(): IndexStats;
}

export interface IndexStats {
  posEntryCount: number;
  ospEntryCount: number;
  ftsTermCount: number;
  ftsDocumentCount: number;
  geoCellCount: number;
  vectorCount: number;
  lastUpdated: number;
}

// ============================================================================
// SQLITE SCHEMA FOR DO STORAGE
// ============================================================================

/**
 * SQL schema for index tables in DO SQLite
 */
export const INDEX_SCHEMA = `
-- POS Index: predicate-value → subjects
CREATE TABLE IF NOT EXISTS pos_index (
  predicate TEXT NOT NULL,
  value_hash TEXT NOT NULL,       -- hash of value for indexing
  value_type INTEGER NOT NULL,    -- ObjectType enum
  subjects TEXT NOT NULL,         -- JSON array of entity IDs
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (predicate, value_hash)
);
CREATE INDEX IF NOT EXISTS idx_pos_predicate ON pos_index(predicate);

-- OSP Index: object reference → subjects (reverse lookup)
CREATE TABLE IF NOT EXISTS osp_index (
  object_ref TEXT NOT NULL PRIMARY KEY,  -- the referenced entity ID
  subjects TEXT NOT NULL,                 -- JSON array of referencing entity IDs
  updated_at INTEGER NOT NULL
);

-- FTS Index: using SQLite FTS5 for efficiency
CREATE VIRTUAL TABLE IF NOT EXISTS fts_index USING fts5(
  entity_id,
  predicate,
  content,
  tokenize='porter unicode61'
);

-- Geo Index: geohash cells
CREATE TABLE IF NOT EXISTS geo_index (
  geohash TEXT NOT NULL PRIMARY KEY,  -- geohash prefix (6 chars default)
  entities TEXT NOT NULL,             -- JSON array of entity IDs
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_geo_prefix ON geo_index(geohash);

-- Vector Index: stored as BLOB for HNSW
CREATE TABLE IF NOT EXISTS vector_index (
  entity_id TEXT NOT NULL,
  predicate TEXT NOT NULL,
  vector BLOB NOT NULL,           -- Float32Array as bytes
  layer INTEGER NOT NULL,         -- HNSW layer
  connections TEXT NOT NULL,      -- JSON array of connected node IDs
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (entity_id, predicate)
);
`;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Hash a value for POS index lookup
 */
export function hashValue(value: unknown, type: ObjectType): string {
  if (value === null || value === undefined) {
    return '_null';
  }

  switch (type) {
    case ObjectType.STRING:
      // For strings, use first 100 chars + hash
      const str = String(value);
      if (str.length <= 100) return str;
      return str.slice(0, 100) + ':' + simpleHash(str);

    case ObjectType.INT64:
    case ObjectType.FLOAT64:
      return String(value);

    case ObjectType.BOOL:
      return value ? 'true' : 'false';

    case ObjectType.REF:
      return String(value);

    case ObjectType.TIMESTAMP:
      return String(value instanceof Date ? value.getTime() : value);

    case ObjectType.GEO_POINT:
      const geo = value as { lat: number; lng: number };
      return `${geo.lat.toFixed(6)},${geo.lng.toFixed(6)}`;

    default:
      return simpleHash(JSON.stringify(value));
  }
}

/**
 * Simple string hash using FNV-1a from core
 * Returns base36 encoded hash for compact representation
 */
function simpleHash(str: string): string {
  return fnv1aHash(str).toString(36);
}

/**
 * Encode geohash from lat/lng
 * Re-exported from core/geo with default precision of 6 for index compatibility
 */
export function encodeGeohash(lat: number, lng: number, precision: number = 6): string {
  return coreEncodeGeohash(lat, lng, precision);
}

/**
 * Get neighboring geohash cells (for radius queries)
 * Re-exported from core/geo
 */
export function getGeohashNeighbors(geohash: string): string[] {
  return coreGetGeohashNeighbors(geohash);
}

// ============================================================================
// JSON SERIALIZATION
// ============================================================================

/**
 * Serialize POS index to JSON for R2 storage
 */
export function serializePOSIndex(index: POSIndex): string {
  return JSON.stringify(index);
}

/**
 * Deserialize POS index from JSON
 */
export function deserializePOSIndex(json: string): POSIndex {
  return JSON.parse(json) as POSIndex;
}

/**
 * Serialize OSP index to JSON for R2 storage
 */
export function serializeOSPIndex(index: OSPIndex): string {
  return JSON.stringify(index);
}

/**
 * Deserialize OSP index from JSON
 */
export function deserializeOSPIndex(json: string): OSPIndex {
  return JSON.parse(json) as OSPIndex;
}

/**
 * Serialize FTS index to JSON for R2 storage
 */
export function serializeFTSIndex(index: FTSIndex): string {
  return JSON.stringify(index);
}

/**
 * Deserialize FTS index from JSON
 */
export function deserializeFTSIndex(json: string): FTSIndex {
  return JSON.parse(json) as FTSIndex;
}

/**
 * Serialize Geo index to JSON for R2 storage
 */
export function serializeGeoIndex(index: GeoIndex): string {
  return JSON.stringify(index);
}

/**
 * Deserialize Geo index from JSON
 */
export function deserializeGeoIndex(json: string): GeoIndex {
  return JSON.parse(json) as GeoIndex;
}
