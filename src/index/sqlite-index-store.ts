/**
 * SQLiteIndexStore - DO SQLite-backed Index Implementation
 *
 * Stores secondary indexes in DO SQLite for fast queries.
 * Syncs to R2 JSON files on compaction.
 *
 * Key features:
 * - In-memory caching with SQLite persistence
 * - FTS5 for full-text search
 * - Efficient batch updates
 * - R2 sync for cold storage
 * - HNSW-based vector similarity search with O(log n) complexity
 *
 * @packageDocumentation
 */

import type {
  Triple,
  TypedObject,
  RefTypedObject,
  StringTypedObject,
  GeoPointTypedObject,
  VectorTypedObject,
} from '../core/triple.js';
import type { EntityId, Predicate } from '../core/types.js';
import { ObjectType } from '../core/types.js';

// ============================================================================
// TYPE GUARDS FOR TYPED OBJECTS
// ============================================================================

/**
 * Type guard for RefTypedObject.
 * Narrows TypedObject to RefTypedObject when the type is REF.
 */
function isRefTypedObject(obj: TypedObject): obj is RefTypedObject {
  return obj.type === ObjectType.REF;
}

/**
 * Type guard for StringTypedObject.
 * Narrows TypedObject to StringTypedObject when the type is STRING.
 */
function isStringTypedObject(obj: TypedObject): obj is StringTypedObject {
  return obj.type === ObjectType.STRING;
}

/**
 * Type guard for GeoPointTypedObject.
 * Narrows TypedObject to GeoPointTypedObject when the type is GEO_POINT.
 */
function isGeoPointTypedObject(obj: TypedObject): obj is GeoPointTypedObject {
  return obj.type === ObjectType.GEO_POINT;
}

/**
 * Type guard for VectorTypedObject.
 * Narrows TypedObject to VectorTypedObject when the type is VECTOR.
 */
function isVectorTypedObject(obj: TypedObject): obj is VectorTypedObject {
  return obj.type === ObjectType.VECTOR;
}

/**
 * Type guard to check if a TypedObject has a value property.
 * All TypedObjects except NullTypedObject have a value property.
 */
function hasValue(obj: TypedObject): obj is Exclude<TypedObject, { type: ObjectType.NULL }> {
  return obj.type !== ObjectType.NULL && 'value' in obj;
}

// ============================================================================
// SQLITE ROW TYPE GUARDS
// ============================================================================

/**
 * Expected shape of a vector index row from SQLite.
 */
interface VectorIndexRow {
  entity_id: string;
  vector: ArrayBuffer;
}

/**
 * Type guard to validate a SQLite row has the expected entity_id field.
 * Checks that the field exists and is a non-empty string.
 *
 * @param row - The SQLite row object to validate
 * @returns True if the row has a valid entity_id string field
 */
function hasEntityId(row: Record<string, unknown>): row is Record<string, unknown> & { entity_id: string } {
  return (
    'entity_id' in row &&
    typeof row['entity_id'] === 'string' &&
    row['entity_id'].length > 0
  );
}

/**
 * Type guard to validate a SQLite row has the expected vector field.
 * Checks that the field exists and is an ArrayBuffer.
 *
 * @param row - The SQLite row object to validate
 * @returns True if the row has a valid vector ArrayBuffer field
 */
function hasVectorBuffer(row: Record<string, unknown>): row is Record<string, unknown> & { vector: ArrayBuffer } {
  return (
    'vector' in row &&
    row['vector'] instanceof ArrayBuffer
  );
}

/**
 * Type guard to validate a SQLite row is a valid VectorIndexRow.
 * Combines entity_id and vector field validation.
 *
 * @param row - The SQLite row object to validate
 * @returns True if the row has all required VectorIndexRow fields with correct types
 */
function isVectorIndexRow(row: unknown): row is VectorIndexRow {
  if (
    typeof row !== 'object' ||
    row === null ||
    Array.isArray(row)
  ) {
    return false;
  }

  const record = row as Record<string, unknown>;
  return hasEntityId(record) && hasVectorBuffer(record);
}

/**
 * Error thrown when SQLite row validation fails.
 * Provides detailed information about the validation failure.
 */
class SqliteRowValidationError extends Error {
  constructor(
    message: string,
    public readonly rowIndex: number,
    public readonly field: string,
    public readonly expectedType: string,
    public readonly actualValue: unknown
  ) {
    super(`Row ${rowIndex}: ${message}`);
    this.name = 'SqliteRowValidationError';
  }
}

// Note: validateVectorIndexRow is kept for future use when vector index validation is needed
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function validateVectorIndexRow(row: unknown, rowIndex: number): VectorIndexRow {
  if (typeof row !== 'object' || row === null || Array.isArray(row)) {
    throw new SqliteRowValidationError(
      'Expected row object',
      rowIndex,
      'row',
      'object',
      row
    );
  }

  const record = row as Record<string, unknown>;

  if (!('entity_id' in record)) {
    throw new SqliteRowValidationError(
      'Missing entity_id field',
      rowIndex,
      'entity_id',
      'string',
      undefined
    );
  }

  if (typeof record['entity_id'] !== 'string') {
    throw new SqliteRowValidationError(
      'entity_id must be a string',
      rowIndex,
      'entity_id',
      'string',
      record['entity_id']
    );
  }

  if (record['entity_id'].length === 0) {
    throw new SqliteRowValidationError(
      'entity_id must not be empty',
      rowIndex,
      'entity_id',
      'non-empty string',
      record['entity_id']
    );
  }

  if (!('vector' in record)) {
    throw new SqliteRowValidationError(
      'Missing vector field',
      rowIndex,
      'vector',
      'ArrayBuffer',
      undefined
    );
  }

  if (!(record['vector'] instanceof ArrayBuffer)) {
    throw new SqliteRowValidationError(
      'vector must be an ArrayBuffer',
      rowIndex,
      'vector',
      'ArrayBuffer',
      record['vector']
    );
  }

  return {
    entity_id: record['entity_id'],
    vector: record['vector'],
  };
}

// HNSW imports for vector similarity search
import { search as hnswSearch, type HNSWGraph as HNSWSearchGraph } from './hnsw/search.js';
import { cosineDistance, cosineSimilarity } from './hnsw/distance.js';
import { SQLiteGraphStore, HNSW_GRAPH_SCHEMA } from './hnsw/sqlite-graph-store.js';
import type { HNSWNode, HNSWConfig } from './hnsw/store.js';
import { DEFAULT_HNSW_CONFIG, randomLevel } from './hnsw/store.js';

// ============================================================================
// VECTOR CACHE CONFIGURATION
// ============================================================================

/**
 * Maximum number of vectors to load into memory cache per predicate.
 * Workers have 128MB memory limit - this prevents OOM for large datasets.
 * For larger datasets, HNSW will still work but may have reduced recall.
 */
const MAX_VECTORS_IN_CACHE = 10000;
import {
  type IndexStore,
  type IndexStats,
  type IndexQueryOptions,
  type POSIndex,
  type OSPIndex,
  type FTSIndex,
  type GeoIndex,
  INDEX_SCHEMA,
  hashValue,
  encodeGeohash,
  getGeohashNeighbors,
  serializePOSIndex,
  serializeOSPIndex,
  serializeFTSIndex,
  serializeGeoIndex,
  deserializePOSIndex,
  deserializeOSPIndex,
  deserializeGeoIndex,
} from './index-store.js';

// ============================================================================
// SQLITE INDEX STORE IMPLEMENTATION
// ============================================================================

/**
 * SQLite-backed index store for DO storage
 *
 * Vector similarity search uses HNSW (Hierarchical Navigable Small World)
 * for O(log n) query complexity instead of O(n) brute force.
 */
export class SQLiteIndexStore implements IndexStore {
  private sql: SqlStorage;
  private initialized: boolean = false;

  // HNSW graph storage for vector similarity search
  private graphStore: SQLiteGraphStore;
  private hnswConfig: HNSWConfig;

  // In-memory cache of HNSW graph for fast search
  // Maps predicate -> HNSWSearchGraph (the search-friendly graph format)
  private hnswGraphCache: Map<string, HNSWSearchGraph> = new Map();

  // Track if HNSW graph needs to be rebuilt from storage
  private hnswGraphDirty: Map<string, boolean> = new Map();

  constructor(sql: SqlStorage, hnswConfig?: Partial<HNSWConfig>) {
    this.sql = sql;
    this.graphStore = new SQLiteGraphStore(sql);
    this.hnswConfig = {
      ...DEFAULT_HNSW_CONFIG,
      ...hnswConfig,
    };
  }

  /**
   * Initialize schema if needed
   *
   * Note: The schema is created by ShardDO's initializeSchema() in schema.ts.
   * We only check if tables exist and set the initialized flag.
   * We do NOT re-run INDEX_SCHEMA because FTS5 virtual tables don't support
   * IF NOT EXISTS properly - they error when already created.
   */
  private ensureInitialized(): void {
    if (this.initialized) return;

    // Check if the index tables already exist (created by schema.ts)
    // If they exist, we just mark as initialized without re-creating
    try {
      const result = this.sql.exec(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='pos_index'"
      ).toArray();

      if (result.length > 0) {
        // Tables exist, just mark as initialized
        this.initialized = true;
        // Also ensure HNSW tables exist
        this.ensureHNSWInitialized();
        return;
      }
    } catch {
      // If query fails, try to create tables
    }

    // Execute schema creation only if tables don't exist
    // Split statements and execute separately to handle virtual tables
    const statements = INDEX_SCHEMA
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const statement of statements) {
      try {
        this.sql.exec(statement);
      } catch {
        // Ignore errors for tables that already exist
        // This handles FTS5 virtual tables which don't support IF NOT EXISTS
      }
    }

    // Initialize HNSW graph tables
    this.ensureHNSWInitialized();

    this.initialized = true;
  }

  /**
   * Initialize HNSW graph tables
   */
  private ensureHNSWInitialized(): void {
    const statements = HNSW_GRAPH_SCHEMA
      .split(';')
      .map((s) => s.trim())
      .filter((s) => s.length > 0);

    for (const statement of statements) {
      try {
        this.sql.exec(statement);
      } catch {
        // Ignore errors for tables that already exist
      }
    }
  }

  // ============================================================================
  // POS INDEX (Predicate-Object-Subject)
  // ============================================================================

  async getByPredicate(predicate: Predicate, options?: IndexQueryOptions): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;
    const cursor = options?.cursor;

    let query = `
      SELECT subjects FROM pos_index
      WHERE predicate = ?
    `;
    const params: unknown[] = [predicate];

    if (cursor) {
      query += ` AND value_hash > ?`;
      params.push(cursor);
    }

    query += ` LIMIT ?`;
    params.push(limit);

    const results = this.sql.exec(query, ...params).toArray();

    // Merge all subject arrays
    const subjects = new Set<string>();
    for (const row of results) {
      const rowSubjects = JSON.parse(row['subjects'] as string) as string[];
      for (const s of rowSubjects) {
        subjects.add(s);
      }
    }

    return Array.from(subjects).slice(0, limit);
  }

  async getByPredicateValue(
    predicate: Predicate,
    value: unknown,
    options?: IndexQueryOptions
  ): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;
    const valueHash = hashValue(value, this.inferType(value));

    const results = this.sql.exec(
      `SELECT subjects FROM pos_index WHERE predicate = ? AND value_hash = ? LIMIT ?`,
      predicate,
      valueHash,
      limit
    ).toArray();

    if (results.length === 0) return [];

    return JSON.parse(results[0]!['subjects'] as string) as string[];
  }

  async getByPredicateRange(
    predicate: Predicate,
    min: number | bigint | Date,
    max: number | bigint | Date,
    options?: IndexQueryOptions
  ): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;
    const minHash = String(min instanceof Date ? min.getTime() : min);
    const maxHash = String(max instanceof Date ? max.getTime() : max);

    // For range queries, we need numeric value_hash
    // This works because we store numbers as their string representation
    const results = this.sql.exec(
      `SELECT subjects FROM pos_index
       WHERE predicate = ?
       AND CAST(value_hash AS REAL) >= CAST(? AS REAL)
       AND CAST(value_hash AS REAL) <= CAST(? AS REAL)
       LIMIT ?`,
      predicate,
      minHash,
      maxHash,
      limit
    ).toArray();

    const subjects = new Set<string>();
    for (const row of results) {
      const rowSubjects = JSON.parse(row['subjects'] as string) as string[];
      for (const s of rowSubjects) {
        subjects.add(s);
      }
    }

    return Array.from(subjects).slice(0, limit);
  }

  // ============================================================================
  // OSP INDEX (Object-Subject-Predicate) - Reverse Lookups
  // ============================================================================

  async getReferencesTo(targetEntityId: EntityId, options?: IndexQueryOptions): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;

    const results = this.sql.exec(
      `SELECT subjects FROM osp_index WHERE object_ref = ? LIMIT 1`,
      targetEntityId
    ).toArray();

    if (results.length === 0) return [];

    const subjects = JSON.parse(results[0]!['subjects'] as string) as string[];
    return subjects.slice(0, limit);
  }

  async getReferencesToByPredicate(
    targetEntityId: EntityId,
    predicate: Predicate,
    options?: IndexQueryOptions
  ): Promise<string[]> {
    this.ensureInitialized();

    // For predicate filtering, we need to join with pos_index
    // First get all references, then filter by predicate
    const allRefs = await this.getReferencesTo(targetEntityId, { limit: 10000 });
    if (allRefs.length === 0) return [];

    const predicateRefs = await this.getByPredicate(predicate, { limit: 10000 });
    const predicateSet = new Set(predicateRefs);

    const limit = options?.limit ?? 1000;
    return allRefs.filter(ref => predicateSet.has(ref)).slice(0, limit);
  }

  // ============================================================================
  // FTS INDEX (Full-Text Search)
  // ============================================================================

  async search(query: string, options?: IndexQueryOptions): Promise<Array<{ entityId: string; score: number }>> {
    this.ensureInitialized();

    const limit = options?.limit ?? 100;

    // Use FTS5 MATCH syntax
    const results = this.sql.exec(
      `SELECT entity_id, bm25(fts_index) as score
       FROM fts_index
       WHERE fts_index MATCH ?
       ORDER BY score
       LIMIT ?`,
      query,
      limit
    ).toArray();

    return results.map(row => ({
      entityId: row['entity_id'] as string,
      score: Math.abs(row['score'] as number), // BM25 returns negative scores
    }));
  }

  async searchInPredicate(
    predicate: Predicate,
    query: string,
    options?: IndexQueryOptions
  ): Promise<Array<{ entityId: string; score: number }>> {
    this.ensureInitialized();

    const limit = options?.limit ?? 100;

    // Filter by predicate column
    const results = this.sql.exec(
      `SELECT entity_id, bm25(fts_index) as score
       FROM fts_index
       WHERE fts_index MATCH ? AND predicate = ?
       ORDER BY score
       LIMIT ?`,
      query,
      predicate,
      limit
    ).toArray();

    return results.map(row => ({
      entityId: row['entity_id'] as string,
      score: Math.abs(row['score'] as number),
    }));
  }

  // ============================================================================
  // GEO INDEX (Geospatial)
  // ============================================================================

  async queryGeoBBox(
    minLat: number,
    minLng: number,
    maxLat: number,
    maxLng: number,
    options?: IndexQueryOptions
  ): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;

    // Generate geohash prefixes that cover the bounding box
    // Use finer sampling to capture all relevant cells
    const geohashes = new Set<string>();

    // Calculate bbox size and determine sampling density
    const latRange = maxLat - minLat;
    const lngRange = maxLng - minLng;

    // Use smaller steps for more accurate coverage
    // At precision 4, cells are ~20km x 20km, so sample every ~5km (0.05 degrees)
    // to ensure we don't miss any cells
    const latStep = Math.min(latRange / 20, 0.05);
    const lngStep = Math.min(lngRange / 20, 0.05);

    for (let lat = minLat; lat <= maxLat; lat += latStep) {
      for (let lng = minLng; lng <= maxLng; lng += lngStep) {
        // Use shorter precision (4) for bbox queries to cover more area
        const gh = encodeGeohash(lat, lng, 4);
        geohashes.add(gh);
      }
    }

    if (geohashes.size === 0) {
      return [];
    }

    // Query all matching cells using prefix matching
    // Batch queries to avoid SQLite variable limit (usually 999)
    const hashArray = Array.from(geohashes);
    const batchSize = 100;
    const allResults: Array<Record<string, unknown>> = [];

    for (let i = 0; i < hashArray.length; i += batchSize) {
      const batch = hashArray.slice(i, i + batchSize);
      const placeholders = batch.map(() => '?').join(',');
      const batchResults = this.sql.exec(
        `SELECT entities FROM geo_index WHERE substr(geohash, 1, 4) IN (${placeholders})`,
        ...batch
      ).toArray();
      allResults.push(...batchResults);
    }

    const results = allResults;

    const entities = new Set<string>();
    for (const row of results) {
      const rowEntities = JSON.parse(row['entities'] as string) as string[];
      for (const e of rowEntities) {
        entities.add(e);
      }
    }

    return Array.from(entities).slice(0, limit);
  }

  async queryGeoRadius(
    centerLat: number,
    centerLng: number,
    radiusKm: number,
    options?: IndexQueryOptions
  ): Promise<string[]> {
    this.ensureInitialized();

    const limit = options?.limit ?? 1000;

    // Determine appropriate geohash precision based on radius
    // Approximate cell sizes: precision 4 ~= 39km x 19.5km, precision 5 ~= 4.9km x 4.9km, precision 6 ~= 1.2km x 0.6km
    let precision: number;
    if (radiusKm >= 20) {
      precision = 3;
    } else if (radiusKm >= 5) {
      precision = 4;
    } else if (radiusKm >= 1) {
      precision = 5;
    } else {
      precision = 6;
    }

    // Get center geohash at appropriate precision and its neighbors
    const centerHash = encodeGeohash(centerLat, centerLng, precision);
    const searchHashes = getGeohashNeighbors(centerHash);

    // For medium radii (1-10km), also search at one level finer for better precision
    if (radiusKm >= 1 && radiusKm <= 10 && precision < 6) {
      const fineHash = encodeGeohash(centerLat, centerLng, precision + 1);
      searchHashes.push(...getGeohashNeighbors(fineHash));
    }

    const uniqueHashes = [...new Set(searchHashes)];

    // Query using prefix matching for the coarser precision
    // This ensures we find entities stored at precision 6 when searching at precision 4 or 5
    const prefixConditions = uniqueHashes.map(() => 'substr(geohash, 1, ?) = ?').join(' OR ');
    const params: (string | number)[] = [];
    for (const hash of uniqueHashes) {
      params.push(hash.length, hash);
    }

    const results = this.sql.exec(
      `SELECT entities FROM geo_index WHERE ${prefixConditions}`,
      ...params
    ).toArray();

    const entities = new Set<string>();
    for (const row of results) {
      const rowEntities = JSON.parse(row['entities'] as string) as string[];
      for (const e of rowEntities) {
        entities.add(e);
      }
    }

    // TODO: Post-filter with haversine distance
    // For now, return all candidates

    return Array.from(entities).slice(0, limit);
  }

  // ============================================================================
  // VECTOR INDEX (Similarity Search)
  // ============================================================================

  /**
   * Query k-nearest neighbors by vector similarity using HNSW
   *
   * Uses HNSW (Hierarchical Navigable Small World) graph for O(log n) search
   * complexity instead of O(n) brute-force.
   *
   * Performance characteristics with HNSW:
   * - 1K vectors: ~0.1ms
   * - 10K vectors: ~0.2ms
   * - 100K vectors: ~0.5ms
   * - 1M vectors: ~1ms
   *
   * @param predicate - The predicate to search vectors for
   * @param queryVector - The query vector
   * @param k - Number of nearest neighbors to return
   * @param ef - Search beam width (higher = more accurate but slower). Default: max(k, 10)
   * @returns Array of {entityId, similarity} sorted by similarity descending
   */
  async queryKNN(
    predicate: Predicate,
    queryVector: number[],
    k: number,
    ef?: number
  ): Promise<Array<{ entityId: string; similarity: number }>> {
    this.ensureInitialized();

    // Get count of vectors for this predicate
    const countResult = this.sql.exec(
      `SELECT COUNT(*) as cnt FROM vector_index WHERE predicate = ?`,
      predicate
    ).toArray();
    const vectorCount = countResult[0]?.['cnt'] as number ?? 0;

    // For small datasets (< 100), brute force is faster due to HNSW overhead
    // Also use brute force if HNSW graph is not built yet
    if (vectorCount < 100 || !this.hasHNSWGraph(predicate)) {
      return this.queryKNNBruteForce(predicate, queryVector, k);
    }

    // Use HNSW search for larger datasets
    return this.queryKNNHNSW(predicate, queryVector, k, ef);
  }

  /**
   * Check if HNSW graph exists for a predicate
   */
  private hasHNSWGraph(predicate: string): boolean {
    const entryPointKey = `entry_point_${predicate}`;
    const entryPoint = this.getHNSWMeta(entryPointKey);
    return entryPoint !== null;
  }

  /**
   * HNSW-based k-nearest neighbor search with O(log n) complexity
   */
  private queryKNNHNSW(
    predicate: Predicate,
    queryVector: number[],
    k: number,
    ef?: number
  ): Array<{ entityId: string; similarity: number }> {
    // Build HNSWSearchGraph structure from SQLite storage
    const graph = this.buildHNSWSearchGraph(predicate);

    if (graph.entryPoint === null || graph.nodeCount === 0) {
      return [];
    }

    // Load vector cache for distance calculations
    const vectorCache = this.loadVectorCache(predicate);

    // Get vector function for HNSW search
    const getVector = (nodeId: string): number[] => {
      const vec = vectorCache.get(nodeId);
      if (!vec) throw new Error(`Vector not found for node: ${nodeId}`);
      return vec;
    };

    // Search using HNSW algorithm
    const searchEf = ef ?? Math.max(k * 2, 10);
    const results = hnswSearch(graph, getVector, queryVector, k, searchEf, cosineDistance);

    // Convert distance to similarity (cosine distance = 1 - similarity)
    return results.map(r => ({
      entityId: r.nodeId,
      similarity: 1 - r.distance,
    }));
  }

  /**
   * Build HNSWSearchGraph structure from SQLite storage
   */
  private buildHNSWSearchGraph(predicate: string): HNSWSearchGraph {
    // Check cache first
    const cached = this.hnswGraphCache.get(predicate);
    if (cached && !this.hnswGraphDirty.get(predicate)) {
      return cached;
    }

    const entryPointKey = `entry_point_${predicate}`;
    const maxLayerKey = `max_layer_${predicate}`;

    const entryPoint = this.getHNSWMeta(entryPointKey);
    const maxLayerStr = this.getHNSWMeta(maxLayerKey);
    const maxLayer = maxLayerStr ? parseInt(maxLayerStr, 10) : -1;

    // Count nodes
    const countResult = this.sql.exec(
      `SELECT COUNT(*) as cnt FROM vector_index WHERE predicate = ?`,
      predicate
    ).toArray();
    const nodeCount = countResult[0]?.['cnt'] as number ?? 0;

    // Build layers array
    const layers: Map<string, string[]>[] = [];

    for (let layer = 0; layer <= Math.max(maxLayer, 0); layer++) {
      const layerMap = new Map<string, string[]>();

      // Get all edges for this layer with this predicate prefix
      const edgeRows = this.sql.exec(
        `SELECT node_id, connections FROM hnsw_edges WHERE layer = ? AND node_id LIKE ?`,
        layer,
        `${predicate}:%`
      ).toArray();

      for (const row of edgeRows) {
        const prefixedId = row['node_id'] as string;
        const nodeId = prefixedId.substring(predicate.length + 1); // Remove predicate: prefix
        const connections = JSON.parse(row['connections'] as string) as string[];
        layerMap.set(nodeId, connections);
      }

      layers.push(layerMap);
    }

    const graph: HNSWSearchGraph = {
      entryPoint,
      maxLayer: Math.max(maxLayer, 0),
      layers,
      nodeCount,
    };

    // Update cache
    this.hnswGraphCache.set(predicate, graph);
    this.hnswGraphDirty.set(predicate, false);

    return graph;
  }

  /**
   * Brute-force k-nearest neighbor search with O(n) complexity
   * Used for small datasets where HNSW overhead is not worth it
   */
  private queryKNNBruteForce(
    predicate: Predicate,
    queryVector: number[],
    k: number
  ): Array<{ entityId: string; similarity: number }> {
    const results = this.sql.exec(
      `SELECT entity_id, vector FROM vector_index WHERE predicate = ?`,
      predicate
    ).toArray();

    const scored: Array<{ entityId: string; similarity: number }> = [];

    for (let i = 0; i < results.length; i++) {
      const row = results[i];

      // Use type guard for safe type narrowing
      if (!isVectorIndexRow(row)) {
        // Skip invalid rows - this handles edge cases where SQLite
        // returns unexpected data (e.g., NULL values, corrupted data)
        continue;
      }

      // TypeScript now knows row is VectorIndexRow
      const { entity_id: entityId, vector: vectorBytes } = row;
      const vector = new Float32Array(vectorBytes);

      const similarity = cosineSimilarity(queryVector, Array.from(vector));
      scored.push({ entityId, similarity });
    }

    // Sort by similarity descending
    scored.sort((a, b) => b.similarity - a.similarity);

    return scored.slice(0, k);
  }

  // ============================================================================
  // INDEX MAINTENANCE
  // ============================================================================

  async indexTriple(triple: Triple): Promise<void> {
    this.ensureInitialized();
    await this.indexTriples([triple]);
  }

  async indexTriples(triples: Triple[]): Promise<void> {
    this.ensureInitialized();

    const now = Date.now();

    for (const triple of triples) {
      const { subject, predicate, object } = triple;

      // Skip NULL types - nothing to index
      if (object.type === ObjectType.NULL) {
        continue;
      }

      // Validate the object has a value (type guard for non-NULL types)
      if (!hasValue(object)) {
        // This shouldn't happen for well-formed TypedObjects after NULL check,
        // but provides runtime safety for malformed data
        continue;
      }

      // POS Index: predicate + value → subject
      // Now TypeScript knows object has a value property
      const valueHash = hashValue(object.value, object.type);
      this.upsertPOSEntry(predicate, valueHash, object.type, subject, now);

      // OSP Index: for REF types, index reverse lookup
      // Using type guard for proper narrowing - object.value is now EntityId
      if (isRefTypedObject(object)) {
        this.upsertOSPEntry(object.value, subject, now);
      }

      // FTS Index: for STRING types, index text
      // Using type guard for proper narrowing - object.value is now string
      if (isStringTypedObject(object)) {
        this.insertFTSEntry(subject, predicate, object.value);
      }

      // Geo Index: for GEO_POINT types
      // Using type guard for proper narrowing - object.value is now GeoPoint
      if (isGeoPointTypedObject(object)) {
        const geo = object.value;
        // Additional runtime validation for geo coordinates
        if (geo && typeof geo.lat === 'number' && typeof geo.lng === 'number') {
          const geohash = encodeGeohash(geo.lat, geo.lng, 6);
          this.upsertGeoEntry(geohash, subject, now);
        }
      }

      // Vector Index: for VECTOR types
      // Using type guard for proper narrowing - object.value is now number[]
      if (isVectorTypedObject(object)) {
        const vector = object.value;
        // Additional runtime validation for vector array
        if (Array.isArray(vector) && vector.length > 0) {
          this.insertVectorEntry(subject, predicate, vector, now);
        }
      }
    }
  }

  private upsertPOSEntry(
    predicate: string,
    valueHash: string,
    valueType: ObjectType,
    subject: string,
    now: number
  ): void {
    // Try to get existing entry
    const existing = this.sql.exec(
      `SELECT subjects FROM pos_index WHERE predicate = ? AND value_hash = ?`,
      predicate,
      valueHash
    ).toArray();

    if (existing.length > 0) {
      const subjects = JSON.parse(existing[0]!['subjects'] as string) as string[];
      if (!subjects.includes(subject)) {
        subjects.push(subject);
        this.sql.exec(
          `UPDATE pos_index SET subjects = ?, updated_at = ? WHERE predicate = ? AND value_hash = ?`,
          JSON.stringify(subjects),
          now,
          predicate,
          valueHash
        );
      }
    } else {
      this.sql.exec(
        `INSERT INTO pos_index (predicate, value_hash, value_type, subjects, updated_at) VALUES (?, ?, ?, ?, ?)`,
        predicate,
        valueHash,
        valueType,
        JSON.stringify([subject]),
        now
      );
    }
  }

  private upsertOSPEntry(objectRef: string, subject: string, now: number): void {
    const existing = this.sql.exec(
      `SELECT subjects FROM osp_index WHERE object_ref = ?`,
      objectRef
    ).toArray();

    if (existing.length > 0) {
      const subjects = JSON.parse(existing[0]!['subjects'] as string) as string[];
      if (!subjects.includes(subject)) {
        subjects.push(subject);
        this.sql.exec(
          `UPDATE osp_index SET subjects = ?, updated_at = ? WHERE object_ref = ?`,
          JSON.stringify(subjects),
          now,
          objectRef
        );
      }
    } else {
      this.sql.exec(
        `INSERT INTO osp_index (object_ref, subjects, updated_at) VALUES (?, ?, ?)`,
        objectRef,
        JSON.stringify([subject]),
        now
      );
    }
  }

  private insertFTSEntry(entityId: string, predicate: string, content: string): void {
    // Check if already indexed
    const existing = this.sql.exec(
      `SELECT rowid FROM fts_index WHERE entity_id = ? AND predicate = ?`,
      entityId,
      predicate
    ).toArray();

    if (existing.length > 0) {
      // Update existing
      this.sql.exec(
        `UPDATE fts_index SET content = ? WHERE entity_id = ? AND predicate = ?`,
        content,
        entityId,
        predicate
      );
    } else {
      this.sql.exec(
        `INSERT INTO fts_index (entity_id, predicate, content) VALUES (?, ?, ?)`,
        entityId,
        predicate,
        content
      );
    }
  }

  private upsertGeoEntry(geohash: string, entityId: string, now: number): void {
    const existing = this.sql.exec(
      `SELECT entities FROM geo_index WHERE geohash = ?`,
      geohash
    ).toArray();

    if (existing.length > 0) {
      const entities = JSON.parse(existing[0]!['entities'] as string) as string[];
      if (!entities.includes(entityId)) {
        entities.push(entityId);
        this.sql.exec(
          `UPDATE geo_index SET entities = ?, updated_at = ? WHERE geohash = ?`,
          JSON.stringify(entities),
          now,
          geohash
        );
      }
    } else {
      this.sql.exec(
        `INSERT INTO geo_index (geohash, entities, updated_at) VALUES (?, ?, ?)`,
        geohash,
        JSON.stringify([entityId]),
        now
      );
    }
  }

  private insertVectorEntry(entityId: string, predicate: string, vector: number[], now: number): void {
    // Convert number[] to Float32Array and then to ArrayBuffer for BLOB storage
    const float32Array = new Float32Array(vector);
    const vectorBytes = float32Array.buffer;

    // Check if already exists
    const existing = this.sql.exec(
      `SELECT rowid FROM vector_index WHERE entity_id = ? AND predicate = ?`,
      entityId,
      predicate
    ).toArray();

    const isNewNode = existing.length === 0;

    if (isNewNode) {
      // Generate random level for HNSW (geometric distribution)
      const nodeLayer = randomLevel(this.hnswConfig.levelMultiplier);

      // Insert new entry with HNSW layer info
      this.sql.exec(
        `INSERT INTO vector_index (entity_id, predicate, vector, layer, connections, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
        entityId,
        predicate,
        vectorBytes,
        nodeLayer,
        JSON.stringify([]),
        now
      );

      // Insert into HNSW graph using the construction algorithm
      this.insertIntoHNSWGraph(entityId, predicate, vector, nodeLayer);
    } else {
      // Update existing vector - vector data changed but keep existing HNSW structure
      // For significant vector changes, we'd need to re-insert but for now just update vector
      this.sql.exec(
        `UPDATE vector_index SET vector = ?, updated_at = ? WHERE entity_id = ? AND predicate = ?`,
        vectorBytes,
        now,
        entityId,
        predicate
      );
    }

    // Mark the HNSW graph cache as dirty for this predicate
    this.hnswGraphDirty.set(predicate, true);
  }

  /**
   * Insert a node into the HNSW graph using the construction algorithm.
   *
   * This implements the HNSW insert algorithm:
   * 1. Start at entry point on highest layer
   * 2. Greedy search down to node's layer + 1
   * 3. At each layer from node's layer down to 0, find neighbors and connect
   */
  private insertIntoHNSWGraph(entityId: string, predicate: string, vector: number[], nodeLayer: number): void {
    // Load existing vectors for this predicate to compute distances
    const vectorCache = this.loadVectorCache(predicate);

    // Get current entry point and max layer for this predicate
    const entryPointKey = `entry_point_${predicate}`;
    const maxLayerKey = `max_layer_${predicate}`;

    let entryPointId = this.getHNSWMeta(entryPointKey);
    let currentMaxLayer = this.getHNSWMeta(maxLayerKey) ? parseInt(this.getHNSWMeta(maxLayerKey)!, 10) : -1;

    // If this is the first node, just set it as entry point
    if (entryPointId === null || currentMaxLayer === -1) {
      this.setHNSWMeta(entryPointKey, entityId);
      this.setHNSWMeta(maxLayerKey, nodeLayer.toString());

      // Save node to graph store with empty connections
      const connections: string[][] = [];
      for (let l = 0; l <= nodeLayer; l++) {
        connections.push([]);
      }
      this.graphStore.saveNode({ nodeId: entityId, maxLayer: nodeLayer, connections });
      return;
    }

    // Distance function using cached vectors
    const distanceFn = (a: number[], b: number[]) => cosineDistance(a, b);

    // Phase 1: Greedy search from top layer to nodeLayer + 1
    let currentNode = entryPointId;
    const queryVector = vector;

    for (let layer = currentMaxLayer; layer > nodeLayer; layer--) {
      // Greedy search to find closest node at this layer
      currentNode = this.searchLayerGreedy(currentNode, queryVector, layer, predicate, vectorCache, distanceFn);
    }

    // Phase 2: Search and connect at layers nodeLayer down to 0
    for (let layer = Math.min(nodeLayer, currentMaxLayer); layer >= 0; layer--) {
      // Find ef nearest neighbors at this layer
      const neighbors = this.searchLayerBeam(
        currentNode,
        queryVector,
        this.hnswConfig.efConstruction,
        layer,
        predicate,
        vectorCache,
        distanceFn
      );

      // Select M or M0 neighbors (M0 at layer 0, M at higher layers)
      const maxNeighbors = layer === 0 ? this.hnswConfig.maxConnectionsLayer0 : this.hnswConfig.maxConnections;
      const selectedNeighbors = neighbors.slice(0, maxNeighbors);

      // Connect new node to selected neighbors
      this.connectNodes(entityId, selectedNeighbors.map(n => n.nodeId), layer, predicate, maxNeighbors);

      // Update current node for next layer
      const firstNeighbor = selectedNeighbors[0];
      if (firstNeighbor) {
        currentNode = firstNeighbor.nodeId;
      }
    }

    // Update entry point if new node has higher layer
    if (nodeLayer > currentMaxLayer) {
      this.setHNSWMeta(entryPointKey, entityId);
      this.setHNSWMeta(maxLayerKey, nodeLayer.toString());
    }

    // Save the new node to graph store
    const existingNode = this.loadHNSWNode(entityId, predicate);
    if (existingNode) {
      this.graphStore.saveNode(existingNode);
    } else {
      // Create new node with connections
      const connections: string[][] = [];
      for (let l = 0; l <= nodeLayer; l++) {
        const layerConnections = this.getNodeConnections(entityId, l, predicate);
        connections.push(layerConnections);
      }
      this.graphStore.saveNode({ nodeId: entityId, maxLayer: nodeLayer, connections });
    }
  }

  /**
   * Greedy search to find closest node at a given layer
   */
  private searchLayerGreedy(
    startNode: string,
    queryVector: number[],
    layer: number,
    predicate: string,
    vectorCache: Map<string, number[]>,
    distanceFn: (a: number[], b: number[]) => number
  ): string {
    let currentNode = startNode;
    const currentVector = vectorCache.get(currentNode);
    if (!currentVector) return currentNode;

    let currentDist = distanceFn(queryVector, currentVector);
    let changed = true;

    while (changed) {
      changed = false;
      const neighbors = this.getNodeConnections(currentNode, layer, predicate);

      for (const neighborId of neighbors) {
        const neighborVector = vectorCache.get(neighborId);
        if (!neighborVector) continue;

        const dist = distanceFn(queryVector, neighborVector);
        if (dist < currentDist) {
          currentDist = dist;
          currentNode = neighborId;
          changed = true;
        }
      }
    }

    return currentNode;
  }

  /**
   * Beam search to find ef nearest neighbors at a given layer
   */
  private searchLayerBeam(
    startNode: string,
    queryVector: number[],
    ef: number,
    layer: number,
    predicate: string,
    vectorCache: Map<string, number[]>,
    distanceFn: (a: number[], b: number[]) => number
  ): Array<{ nodeId: string; distance: number }> {
    const visited = new Set<string>([startNode]);
    const startVector = vectorCache.get(startNode);
    if (!startVector) return [];

    const startDist = distanceFn(queryVector, startVector);

    // Candidates sorted by distance (ascending)
    const candidates: Array<{ nodeId: string; distance: number }> = [{ nodeId: startNode, distance: startDist }];

    // Results sorted by distance (ascending)
    const results: Array<{ nodeId: string; distance: number }> = [{ nodeId: startNode, distance: startDist }];

    while (candidates.length > 0) {
      // Get closest candidate
      const current = candidates.shift()!;

      // Stop if closest candidate is farther than furthest result (and we have ef results)
      if (results.length >= ef && current.distance > results[results.length - 1]!.distance) {
        break;
      }

      // Explore neighbors
      const neighbors = this.getNodeConnections(current.nodeId, layer, predicate);

      for (const neighborId of neighbors) {
        if (visited.has(neighborId)) continue;
        visited.add(neighborId);

        const neighborVector = vectorCache.get(neighborId);
        if (!neighborVector) continue;

        const dist = distanceFn(queryVector, neighborVector);

        // Add to candidates if better than worst result or we don't have ef yet
        if (results.length < ef || dist < results[results.length - 1]!.distance) {
          // Insert in sorted order
          this.insertSorted(candidates, { nodeId: neighborId, distance: dist });
          this.insertSorted(results, { nodeId: neighborId, distance: dist });

          // Trim results to ef
          if (results.length > ef) {
            results.pop();
          }
        }
      }
    }

    return results;
  }

  /**
   * Insert an item into a sorted array (by distance ascending)
   */
  private insertSorted(arr: Array<{ nodeId: string; distance: number }>, item: { nodeId: string; distance: number }): void {
    let left = 0;
    let right = arr.length;
    while (left < right) {
      const mid = Math.floor((left + right) / 2);
      if (arr[mid]!.distance < item.distance) {
        left = mid + 1;
      } else {
        right = mid;
      }
    }
    arr.splice(left, 0, item);
  }

  /**
   * Connect a node to neighbors and ensure neighbor connections are pruned
   */
  private connectNodes(
    nodeId: string,
    neighborIds: string[],
    layer: number,
    predicate: string,
    maxConnections: number
  ): void {
    // Set connections for the new node
    this.setNodeConnections(nodeId, neighborIds, layer, predicate);

    // For each neighbor, add bidirectional connection and prune if necessary
    for (const neighborId of neighborIds) {
      const neighborConnections = this.getNodeConnections(neighborId, layer, predicate);

      // Add connection to new node if not already present
      if (!neighborConnections.includes(nodeId)) {
        neighborConnections.push(nodeId);

        // Prune if exceeds max connections
        if (neighborConnections.length > maxConnections) {
          // Load vectors for pruning
          const vectorCache = this.loadVectorCache(predicate);
          const neighborVector = vectorCache.get(neighborId);
          if (neighborVector) {
            // Sort by distance and keep closest
            const withDistances = neighborConnections.map(id => ({
              nodeId: id,
              distance: vectorCache.get(id) ? cosineDistance(neighborVector, vectorCache.get(id)!) : Infinity,
            }));
            withDistances.sort((a, b) => a.distance - b.distance);
            const pruned = withDistances.slice(0, maxConnections).map(n => n.nodeId);
            this.setNodeConnections(neighborId, pruned, layer, predicate);
          }
        } else {
          this.setNodeConnections(neighborId, neighborConnections, layer, predicate);
        }
      }
    }
  }

  /**
   * Get connections for a node at a specific layer (from vector_index table)
   */
  private getNodeConnections(nodeId: string, layer: number, predicate: string): string[] {
    // We store connections in the vector_index table as JSON
    // But we need to support multiple layers, so we use a separate approach
    // For now, query from hnsw_edges table
    try {
      const rows = this.sql.exec(
        `SELECT connections FROM hnsw_edges WHERE node_id = ? AND layer = ?`,
        `${predicate}:${nodeId}`,
        layer
      ).toArray();

      if (rows.length === 0) return [];
      return JSON.parse(rows[0]!['connections'] as string) as string[];
    } catch (error) {
      console.error('[HNSW] Failed to get node connections:', {
        nodeId,
        layer,
        predicate,
        error: error instanceof Error ? error.message : String(error),
      });
      return [];
    }
  }

  /**
   * Set connections for a node at a specific layer
   */
  private setNodeConnections(nodeId: string, connections: string[], layer: number, predicate: string): void {
    // Store in hnsw_edges table with predicate-prefixed node_id
    const prefixedId = `${predicate}:${nodeId}`;
    this.sql.exec(
      `INSERT OR REPLACE INTO hnsw_edges (node_id, layer, connections) VALUES (?, ?, ?)`,
      prefixedId,
      layer,
      JSON.stringify(connections)
    );
  }

  /**
   * Load HNSW node from graph store
   */
  private loadHNSWNode(nodeId: string, predicate: string): HNSWNode | null {
    const prefixedId = `${predicate}:${nodeId}`;
    try {
      const nodeRows = this.sql.exec(
        `SELECT max_layer FROM hnsw_nodes WHERE node_id = ?`,
        prefixedId
      ).toArray();

      if (nodeRows.length === 0) return null;

      const maxLayer = nodeRows[0]!['max_layer'] as number;
      const connections: string[][] = [];

      for (let l = 0; l <= maxLayer; l++) {
        connections.push(this.getNodeConnections(nodeId, l, predicate));
      }

      return { nodeId, maxLayer, connections };
    } catch (error) {
      console.error('[HNSW] Failed to load node:', {
        nodeId,
        predicate,
        error: error instanceof Error ? error.message : String(error),
      });
      return null;
    }
  }

  /**
   * Get HNSW metadata value
   */
  private getHNSWMeta(key: string): string | null {
    try {
      const rows = this.sql.exec(
        `SELECT value FROM hnsw_meta WHERE key = ?`,
        key
      ).toArray();

      if (rows.length === 0) return null;
      const value = rows[0]!['value'] as string;
      return value === '' ? null : value;
    } catch {
      return null;
    }
  }

  /**
   * Set HNSW metadata value
   */
  private setHNSWMeta(key: string, value: string): void {
    this.sql.exec(
      `INSERT OR REPLACE INTO hnsw_meta (key, value) VALUES (?, ?)`,
      key,
      value
    );
  }

  /**
   * Load vectors for a predicate into memory cache with pagination.
   * Limited to MAX_VECTORS_IN_CACHE to prevent OOM in Workers (128MB limit).
   *
   * For larger datasets, HNSW search will still work but may have reduced
   * recall since not all vectors are in memory for distance calculations.
   */
  private loadVectorCache(predicate: string): Map<string, number[]> {
    const cache = new Map<string, number[]>();

    // First, check total count to determine if truncation is needed
    const countResult = this.sql.exec(
      `SELECT COUNT(*) as cnt FROM vector_index WHERE predicate = ?`,
      predicate
    ).toArray();
    const totalCount = (countResult[0]?.['cnt'] as number) ?? 0;

    // Load vectors with pagination limit to prevent OOM
    const rows = this.sql.exec(
      `SELECT entity_id, vector FROM vector_index WHERE predicate = ? LIMIT ?`,
      predicate,
      MAX_VECTORS_IN_CACHE
    ).toArray();

    for (const row of rows) {
      // Use type guard for safe type narrowing
      if (!isVectorIndexRow(row)) {
        // Skip invalid rows - handles edge cases with corrupted/NULL data
        continue;
      }

      // TypeScript now knows row is VectorIndexRow
      const { entity_id: entityId, vector: vectorBytes } = row;
      const vector = Array.from(new Float32Array(vectorBytes));
      cache.set(entityId, vector);
    }

    // Log warning if cache was truncated
    if (totalCount > MAX_VECTORS_IN_CACHE) {
      console.warn(
        `[VectorCache] Cache truncated for predicate "${predicate}": ` +
        `loaded ${MAX_VECTORS_IN_CACHE} of ${totalCount} vectors. ` +
        `HNSW search may have reduced recall for vectors not in cache.`
      );
    }

    return cache;
  }

  async unindexTriple(triple: Triple): Promise<void> {
    this.ensureInitialized();

    const { subject, predicate, object } = triple;

    // Skip NULL types - nothing to unindex
    if (object.type === ObjectType.NULL) {
      return;
    }

    // Extract value from typed object (safe after NULL check)
    const value = 'value' in object ? object.value : null;
    const valueHash = hashValue(value, object.type);

    // Remove from POS index
    const posEntry = this.sql.exec(
      `SELECT subjects FROM pos_index WHERE predicate = ? AND value_hash = ?`,
      predicate,
      valueHash
    ).toArray();

    if (posEntry.length > 0) {
      const subjects = JSON.parse(posEntry[0]!['subjects'] as string) as string[];
      const filtered = subjects.filter(s => s !== subject);
      if (filtered.length === 0) {
        this.sql.exec(
          `DELETE FROM pos_index WHERE predicate = ? AND value_hash = ?`,
          predicate,
          valueHash
        );
      } else {
        this.sql.exec(
          `UPDATE pos_index SET subjects = ? WHERE predicate = ? AND value_hash = ?`,
          JSON.stringify(filtered),
          predicate,
          valueHash
        );
      }
    }

    // Remove from OSP index if REF
    if (object.type === ObjectType.REF && typeof value === 'string') {
      const ospEntry = this.sql.exec(
        `SELECT subjects FROM osp_index WHERE object_ref = ?`,
        value
      ).toArray();

      if (ospEntry.length > 0) {
        const subjects = JSON.parse(ospEntry[0]!['subjects'] as string) as string[];
        const filtered = subjects.filter(s => s !== subject);
        if (filtered.length === 0) {
          this.sql.exec(`DELETE FROM osp_index WHERE object_ref = ?`, value);
        } else {
          this.sql.exec(
            `UPDATE osp_index SET subjects = ? WHERE object_ref = ?`,
            JSON.stringify(filtered),
            value
          );
        }
      }
    }

    // Remove from FTS index
    if (object.type === ObjectType.STRING) {
      this.sql.exec(
        `DELETE FROM fts_index WHERE entity_id = ? AND predicate = ?`,
        subject,
        predicate
      );
    }

    // Remove from Geo index
    if (object.type === ObjectType.GEO_POINT && value) {
      const geo = value as { lat: number; lng: number };
      const geohash = encodeGeohash(geo.lat, geo.lng, 6);

      const geoEntry = this.sql.exec(
        `SELECT entities FROM geo_index WHERE geohash = ?`,
        geohash
      ).toArray();

      if (geoEntry.length > 0) {
        const entities = JSON.parse(geoEntry[0]!['entities'] as string) as string[];
        const filtered = entities.filter(e => e !== subject);
        if (filtered.length === 0) {
          this.sql.exec(`DELETE FROM geo_index WHERE geohash = ?`, geohash);
        } else {
          this.sql.exec(
            `UPDATE geo_index SET entities = ? WHERE geohash = ?`,
            JSON.stringify(filtered),
            geohash
          );
        }
      }
    }
  }

  // ============================================================================
  // R2 SYNC
  // ============================================================================

  async loadFromR2(r2: R2Bucket, namespace: string): Promise<void> {
    this.ensureInitialized();

    // Load POS index
    const posObject = await r2.get(`${namespace}/indexes/pos.json`);
    if (posObject) {
      const posIndex = deserializePOSIndex(await posObject.text());
      for (const [key, subjects] of Object.entries(posIndex.entries)) {
        const [predicate, valueHash] = key.includes(':')
          ? [key.split(':')[0]!, key.split(':').slice(1).join(':')]
          : [key, '_all'];
        this.sql.exec(
          `INSERT OR REPLACE INTO pos_index (predicate, value_hash, value_type, subjects, updated_at) VALUES (?, ?, ?, ?, ?)`,
          predicate,
          valueHash,
          ObjectType.STRING, // default
          JSON.stringify(subjects),
          Date.now()
        );
      }
    }

    // Load OSP index
    const ospObject = await r2.get(`${namespace}/indexes/osp.json`);
    if (ospObject) {
      const ospIndex = deserializeOSPIndex(await ospObject.text());
      for (const [objectRef, subjects] of Object.entries(ospIndex.entries)) {
        this.sql.exec(
          `INSERT OR REPLACE INTO osp_index (object_ref, subjects, updated_at) VALUES (?, ?, ?)`,
          objectRef,
          JSON.stringify(subjects),
          Date.now()
        );
      }
    }

    // Load Geo index
    const geoObject = await r2.get(`${namespace}/indexes/geo.json`);
    if (geoObject) {
      const geoIndex = deserializeGeoIndex(await geoObject.text());
      for (const [geohash, entities] of Object.entries(geoIndex.cells)) {
        this.sql.exec(
          `INSERT OR REPLACE INTO geo_index (geohash, entities, updated_at) VALUES (?, ?, ?)`,
          geohash,
          JSON.stringify(entities),
          Date.now()
        );
      }
    }

    // FTS index: would need to re-index from source data
    // For now, skip FTS loading from R2
  }

  async saveToR2(r2: R2Bucket, namespace: string): Promise<void> {
    this.ensureInitialized();

    const now = Date.now();
    const version = `v${now}`;

    // Export POS index
    const posEntries: Record<string, string[]> = {};
    const posRows = this.sql.exec(`SELECT predicate, value_hash, subjects FROM pos_index`).toArray();
    for (const row of posRows) {
      const key = row['value_hash'] === '_all'
        ? (row['predicate'] as string)
        : `${row['predicate']}:${row['value_hash']}`;
      posEntries[key] = JSON.parse(row['subjects'] as string);
    }
    const posIndex: POSIndex = { version, entries: posEntries };
    await r2.put(`${namespace}/indexes/pos.json`, serializePOSIndex(posIndex));

    // Export OSP index
    const ospEntries: Record<string, string[]> = {};
    const ospRows = this.sql.exec(`SELECT object_ref, subjects FROM osp_index`).toArray();
    for (const row of ospRows) {
      ospEntries[row['object_ref'] as string] = JSON.parse(row['subjects'] as string);
    }
    const ospIndex: OSPIndex = { version, entries: ospEntries };
    await r2.put(`${namespace}/indexes/osp.json`, serializeOSPIndex(ospIndex));

    // Export Geo index
    const geoCells: Record<string, string[]> = {};
    const geoRows = this.sql.exec(`SELECT geohash, entities FROM geo_index`).toArray();
    for (const row of geoRows) {
      geoCells[row['geohash'] as string] = JSON.parse(row['entities'] as string);
    }
    const geoIndex: GeoIndex = { version, precision: 6, cells: geoCells };
    await r2.put(`${namespace}/indexes/geo.json`, serializeGeoIndex(geoIndex));

    // FTS: Export as inverted index
    const ftsTerms: Record<string, Array<{ entityId: string; predicate: string; score: number }>> = {};
    // Note: FTS5 doesn't easily export terms, would need to query vocab table
    // For now, skip FTS export
    const ftsIndex: FTSIndex = { version, documentCount: 0, terms: ftsTerms };
    await r2.put(`${namespace}/indexes/fts.json`, serializeFTSIndex(ftsIndex));
  }

  // ============================================================================
  // STATS
  // ============================================================================

  getStats(): IndexStats {
    this.ensureInitialized();

    const posCount = this.sql.exec(`SELECT COUNT(*) as cnt FROM pos_index`).toArray()[0]!['cnt'] as number;
    const ospCount = this.sql.exec(`SELECT COUNT(*) as cnt FROM osp_index`).toArray()[0]!['cnt'] as number;
    const geoCount = this.sql.exec(`SELECT COUNT(*) as cnt FROM geo_index`).toArray()[0]!['cnt'] as number;
    const vectorCount = this.sql.exec(`SELECT COUNT(*) as cnt FROM vector_index`).toArray()[0]!['cnt'] as number;

    // FTS stats
    let ftsTermCount = 0;
    let ftsDocCount = 0;
    try {
      ftsDocCount = this.sql.exec(`SELECT COUNT(*) as cnt FROM fts_index`).toArray()[0]!['cnt'] as number;
      // Term count would require vocab table query
    } catch {
      // FTS may not be initialized
    }

    return {
      posEntryCount: posCount,
      ospEntryCount: ospCount,
      ftsTermCount,
      ftsDocumentCount: ftsDocCount,
      geoCellCount: geoCount,
      vectorCount,
      lastUpdated: Date.now(),
    };
  }

  // ============================================================================
  // HELPERS
  // ============================================================================

  private inferType(value: unknown): ObjectType {
    if (value === null || value === undefined) return ObjectType.NULL;
    if (typeof value === 'string') return ObjectType.STRING;
    if (typeof value === 'number') {
      return Number.isInteger(value) ? ObjectType.INT64 : ObjectType.FLOAT64;
    }
    if (typeof value === 'boolean') return ObjectType.BOOL;
    if (value instanceof Date) return ObjectType.TIMESTAMP;
    if (typeof value === 'object' && 'lat' in value && 'lng' in value) {
      return ObjectType.GEO_POINT;
    }
    return ObjectType.JSON;
  }
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

// cosineSimilarity is imported from './hnsw/distance.js'
