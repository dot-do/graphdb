/**
 * Shard module exports
 *
 * The Shard DO handles SQLite triple storage with typed object columns.
 * Serves as the target for Broker DO subrequests.
 */

export {
  ShardDO,
  type ShardStats,
  type ShardWebSocketAttachment,
  type PendingOperation,
  type MaintenanceTask,
} from './shard-do.js';

// Schema exports
export {
  SCHEMA_VERSION,
  TRIPLES_SCHEMA,
  SCHEMA_META,
  CHUNKS_SCHEMA,
  MIGRATIONS,
  initializeSchema,
  getCurrentVersion,
  migrateToVersion,
  runMigration,
  type Migration,
} from './schema.js';

// CRUD exports (DEPRECATED - uses individual rows, not cost-efficient)
// Use ChunkStore instead for BLOB-only architecture
export {
  createTripleStore,
  tripleToRow,
  rowToTriple,
  type TripleStore,
  type TripleRow,
} from './crud.js';

// Row parser exports (type-safe SQL row parsing)
export {
  parseTripleRow,
  parseChunkRow,
  parseTripleRows,
  parseChunkRows,
  assertTripleRow,
  assertChunkRow,
  RowParseError,
  type TripleRow as ParsedTripleRow,
  type ChunkRow as ParsedChunkRow,
  type ParseOptions,
} from './row-parsers.js';

// ChunkStore exports (BLOB-only architecture)
export {
  createChunkStore,
  initializeChunksSchema,
  TARGET_BUFFER_SIZE,
  MIN_CHUNK_SIZE_FOR_COMPACTION,
  MIN_CHUNKS_FOR_COMPACTION,
  type ChunkStore,
  type ChunkStoreStats,
  type ChunkMetadata,
  // Legacy exports (deprecated)
  HOT_ROW_AGE_MS,
  MIN_ROWS_FOR_COMPACTION,
  MAX_TRIPLES_PER_CHUNK,
} from './chunk-store.js';

// SQL utility exports (typed wrappers for SqlStorage)
export {
  querySql,
  querySqlOne,
  type SqlStorageValue,
} from './sql-utils.js';
