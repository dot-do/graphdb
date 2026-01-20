/**
 * Index module exports for GraphDB
 *
 * Re-exports all index query operations:
 * - Triple indexes (SPO, POS, OSP)
 * - FTS index (Full-Text Search using FTS5)
 * - Geo index (Geospatial using geohash)
 */

// Triple index queries and types
export {
  querySPO,
  queryPOS,
  queryOSP,
  batchQuerySPO,
  batchQueryOSP,
  type SPOQuery,
  type POSQuery,
  type OSPQuery,
  type QueryResult,
} from './triple-indexes';

// FTS (Full-Text Search) index
export {
  FTS_SCHEMA,
  initializeFTS,
  isFTSInitialized,
  searchFTS,
  rebuildFTS,
  FTSQueryError,
  FTSErrorCode,
  type FTSQuery,
  type FTSResult,
  type FTSErrorCodeType,
} from './fts-index';

// Geo (Geospatial) index
export {
  GEO_INDEX_SCHEMA,
  initializeGeoIndex,
  computeGeohashForTriple,
  haversineDistance,
  getGeohashNeighbors,
  queryGeoBBox,
  queryGeoRadius,
  insertGeoPointTriple,
  type GeoQuery,
  type GeoResult,
} from './geo-index';

// Unified Index Store (new hybrid architecture)
export {
  type IndexStore,
  type IndexStats,
  type IndexQueryOptions,
  type POSIndex,
  type OSPIndex,
  type FTSIndex,
  type FTSPosting,
  type GeoIndex,
  type VectorIndex,
  type VectorIndexEntry,
  INDEX_SCHEMA,
  hashValue,
  encodeGeohash,
  getGeohashNeighbors as getGeohashNeighborsNew,
  serializePOSIndex,
  deserializePOSIndex,
  serializeOSPIndex,
  deserializeOSPIndex,
  serializeFTSIndex,
  deserializeFTSIndex,
  serializeGeoIndex,
  deserializeGeoIndex,
} from './index-store';

// SQLite-backed implementation
export { SQLiteIndexStore } from './sqlite-index-store';

// Combined index file format (all indexes in one file)
export {
  GIDX_MAGIC,
  GIDX_VERSION,
  IndexType,
  Compression,
  type IndexDirectoryEntry,
  type CombinedIndexHeader,
  type CombinedIndexData,
  type IndexHeaderInfo,
  encodeCombinedIndex,
  decodeCombinedIndex,
  decodeIndexHeader,
  decodeIndexSection,
  getHeaderRange,
  planRangeRequests,
  coalesceRanges,
  // Quantized vector format (separate .qvec file)
  QVEC_MAGIC,
  VectorQuantization,
  type QuantizedVectorHeader,
  type QuantizedVectorFile,
  encodeQuantizedVectors,
  decodeQuantizedVectors,
  decodeQuantizedVectorHeader,
  getVectorFloat32,
  cosineSimilarity,
  hammingDistance,
} from './combined-index';
