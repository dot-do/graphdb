/**
 * SQLite Schema for GraphDB Shard DO - BLOB-Only Architecture
 *
 * CRITICAL DESIGN: On Cloudflare DO, a 1KB row costs the SAME as a 2MB BLOB.
 * Individual rows are NOT faster - they're the same cost but 10,000x less efficient.
 *
 * Architecture:
 *   Write Request -> In-Memory Buffer -> Flush to 2MB BLOB (only SQLite operation)
 *
 * NO individual triple rows. EVER.
 *
 * The ONLY data table is 'chunks' which stores 2MB GraphCol BLOB chunks.
 * All triple data is serialized into these chunks.
 *
 * @see CLAUDE.md for architecture details
 */

/**
 * Current schema version - BLOB-only architecture with index tables
 */
export const SCHEMA_VERSION = 4;

/**
 * SQL schema for BLOB chunk storage
 *
 * The chunks table stores triples in GraphCol BLOB format.
 * Each chunk contains up to ~50,000 triples encoded as a binary blob.
 *
 * Cost optimization:
 * - 1 chunk row = 1 read/write operation (regardless of size up to 2MB)
 * - 50,000 individual rows = 50,000 read/write operations
 * - Cost savings: 50,000x reduction in DO storage operations
 *
 * Indexes support:
 * - Namespace-based filtering
 * - Time-range queries for relevant chunks
 */
export const CHUNKS_SCHEMA = `
CREATE TABLE IF NOT EXISTS chunks (
  id TEXT PRIMARY KEY,
  namespace TEXT NOT NULL,
  triple_count INTEGER NOT NULL,
  min_timestamp INTEGER NOT NULL,
  max_timestamp INTEGER NOT NULL,
  data BLOB NOT NULL,
  size_bytes INTEGER NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chunks_ns ON chunks(namespace);
CREATE INDEX IF NOT EXISTS idx_chunks_time ON chunks(min_timestamp, max_timestamp);
`;

/**
 * SQL schema for triples table
 * Added back in v4 to support both BLOB-optimized chunk storage AND
 * individual triple writes for index integration.
 */
export const TRIPLES_TABLE_SCHEMA = `
CREATE TABLE IF NOT EXISTS triples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject TEXT NOT NULL,
  predicate TEXT NOT NULL,
  obj_type INTEGER NOT NULL,
  obj_ref TEXT,
  obj_string TEXT,
  obj_int64 INTEGER,
  obj_float64 REAL,
  obj_bool INTEGER,
  obj_timestamp INTEGER,
  obj_lat REAL,
  obj_lng REAL,
  obj_binary BLOB,
  timestamp INTEGER NOT NULL,
  tx_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spo ON triples(subject, predicate, obj_type);
CREATE INDEX IF NOT EXISTS idx_pos ON triples(predicate, obj_type, subject);
CREATE INDEX IF NOT EXISTS idx_osp ON triples(obj_ref, subject, predicate) WHERE obj_type = 10;
CREATE INDEX IF NOT EXISTS idx_timestamp ON triples(timestamp);
CREATE INDEX IF NOT EXISTS idx_tx ON triples(tx_id);
`;

/**
 * SQL schema for secondary indexes (POS, OSP, FTS, Geo, Vector)
 * These indexes enable fast queries by predicate, reverse lookups,
 * full-text search, and geospatial queries.
 */
export const INDEX_TABLES_SCHEMA = `
-- POS Index: predicate-value -> subjects
CREATE TABLE IF NOT EXISTS pos_index (
  predicate TEXT NOT NULL,
  value_hash TEXT NOT NULL,
  value_type INTEGER NOT NULL,
  subjects TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (predicate, value_hash)
);
CREATE INDEX IF NOT EXISTS idx_pos_predicate ON pos_index(predicate);

-- OSP Index: object reference -> subjects (reverse lookup)
CREATE TABLE IF NOT EXISTS osp_index (
  object_ref TEXT NOT NULL PRIMARY KEY,
  subjects TEXT NOT NULL,
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
  geohash TEXT NOT NULL PRIMARY KEY,
  entities TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_geo_prefix ON geo_index(geohash);

-- Vector Index: stored as BLOB for HNSW
CREATE TABLE IF NOT EXISTS vector_index (
  entity_id TEXT NOT NULL,
  predicate TEXT NOT NULL,
  vector BLOB NOT NULL,
  layer INTEGER NOT NULL,
  connections TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (entity_id, predicate)
);
`;

/**
 * Schema versioning table
 */
export const SCHEMA_META = `
CREATE TABLE IF NOT EXISTS schema_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
`;

/**
 * Migration definition
 */
export interface Migration {
  /** Migration version number */
  version: number;
  /** SQL to run for upgrading */
  up: string;
  /** SQL to run for downgrading */
  down: string;
}

/**
 * All migrations in order
 *
 * Version 3: BLOB-only architecture - removes triples table entirely.
 * Individual triple rows defeat the cost optimization.
 */
export const MIGRATIONS: Migration[] = [
  {
    version: 1,
    // Note: This is legacy - version 1 originally created triples table
    // For fresh installs, we skip to version 3 directly
    up: SCHEMA_META,
    down: 'DROP TABLE IF EXISTS schema_meta;',
  },
  {
    version: 2,
    // Legacy: chunks table addition (kept for reference)
    up: CHUNKS_SCHEMA,
    down: 'DROP INDEX IF EXISTS idx_chunks_time; DROP INDEX IF EXISTS idx_chunks_ns; DROP TABLE IF EXISTS chunks;',
  },
  {
    version: 3,
    // BLOB-only: Remove triples table entirely
    // Individual rows defeat the cost optimization
    up: 'DROP TABLE IF EXISTS triples; DROP INDEX IF EXISTS idx_spo; DROP INDEX IF EXISTS idx_pos; DROP INDEX IF EXISTS idx_osp; DROP INDEX IF EXISTS idx_timestamp; DROP INDEX IF EXISTS idx_tx;',
    down: `
CREATE TABLE IF NOT EXISTS triples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subject TEXT NOT NULL,
  predicate TEXT NOT NULL,
  obj_type INTEGER NOT NULL,
  obj_ref TEXT,
  obj_string TEXT,
  obj_int64 INTEGER,
  obj_float64 REAL,
  obj_bool INTEGER,
  obj_timestamp INTEGER,
  obj_lat REAL,
  obj_lng REAL,
  obj_binary BLOB,
  timestamp INTEGER NOT NULL,
  tx_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_spo ON triples(subject, predicate, obj_type);
CREATE INDEX IF NOT EXISTS idx_pos ON triples(predicate, obj_type, subject);
CREATE INDEX IF NOT EXISTS idx_osp ON triples(obj_ref, subject, predicate) WHERE obj_type = 10;
CREATE INDEX IF NOT EXISTS idx_timestamp ON triples(timestamp);
CREATE INDEX IF NOT EXISTS idx_tx ON triples(tx_id);
`,
  },
  {
    version: 4,
    // Add triples table back + index tables for index integration
    // This enables both BLOB-optimized chunk storage AND individual triple indexing
    up: TRIPLES_TABLE_SCHEMA + INDEX_TABLES_SCHEMA,
    down: `
DROP INDEX IF EXISTS idx_spo;
DROP INDEX IF EXISTS idx_pos;
DROP INDEX IF EXISTS idx_osp;
DROP INDEX IF EXISTS idx_timestamp;
DROP INDEX IF EXISTS idx_tx;
DROP TABLE IF EXISTS triples;
DROP INDEX IF EXISTS idx_pos_predicate;
DROP TABLE IF EXISTS pos_index;
DROP TABLE IF EXISTS osp_index;
DROP TABLE IF EXISTS fts_index;
DROP INDEX IF EXISTS idx_geo_prefix;
DROP TABLE IF EXISTS geo_index;
DROP TABLE IF EXISTS vector_index;
`,
  },
];

/**
 * Initialize the schema on a SQLite storage instance
 *
 * For fresh databases, creates only the BLOB-only schema (chunks + schema_meta).
 * For existing databases, migrates to remove the triples table.
 *
 * @param sql - SqlStorage instance from DurableObjectState
 */
export function initializeSchema(sql: SqlStorage): void {
  const currentVersion = getCurrentVersion(sql);

  if (currentVersion === 0) {
    // Fresh database - initialize with BLOB-only schema directly
    initializeFreshSchema(sql);
  } else if (currentVersion < SCHEMA_VERSION) {
    // Existing database - migrate
    migrateToVersion(sql, SCHEMA_VERSION);
  }
}

/**
 * Initialize a fresh database with full schema
 *
 * Creates chunks table, triples table, and index tables.
 */
function initializeFreshSchema(sql: SqlStorage): void {
  // Create schema_meta first
  sql.exec(SCHEMA_META);

  // Create chunks table
  const chunksStatements = CHUNKS_SCHEMA
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  for (const statement of chunksStatements) {
    sql.exec(statement);
  }

  // Create triples table
  const triplesStatements = TRIPLES_TABLE_SCHEMA
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  for (const statement of triplesStatements) {
    sql.exec(statement);
  }

  // Create index tables
  const indexStatements = INDEX_TABLES_SCHEMA
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  for (const statement of indexStatements) {
    sql.exec(statement);
  }

  // Set version to current
  setSchemaVersion(sql, SCHEMA_VERSION);
}

/**
 * Get the current schema version from the database
 *
 * Returns 0 if the schema has not been initialized.
 *
 * @param sql - SqlStorage instance
 * @returns Current schema version number
 */
export function getCurrentVersion(sql: SqlStorage): number {
  try {
    // Check if schema_meta table exists
    const tableCheck = sql.exec(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
    );
    const tables = [...tableCheck];

    if (tables.length === 0) {
      return 0;
    }

    // Get version from schema_meta
    const result = sql.exec("SELECT value FROM schema_meta WHERE key='schema_version'");
    const rows = [...result];

    if (rows.length === 0) {
      return 0;
    }

    return parseInt(rows[0]!['value'] as string, 10);
  } catch {
    // Table doesn't exist
    return 0;
  }
}

/**
 * Migrate the schema to a target version
 *
 * Runs all migrations between current version and target version in order.
 *
 * @param sql - SqlStorage instance
 * @param targetVersion - Target schema version
 */
export function migrateToVersion(sql: SqlStorage, targetVersion: number): void {
  const currentVersion = getCurrentVersion(sql);

  if (targetVersion === currentVersion) {
    return;
  }

  if (targetVersion > currentVersion) {
    // Upgrade
    for (const migration of MIGRATIONS) {
      if (migration.version > currentVersion && migration.version <= targetVersion) {
        runMigration(sql, migration, 'up');
        setSchemaVersion(sql, migration.version);
      }
    }
  } else {
    // Downgrade (run in reverse order)
    const reverseMigrations = [...MIGRATIONS].reverse();
    for (const migration of reverseMigrations) {
      if (migration.version <= currentVersion && migration.version > targetVersion) {
        runMigration(sql, migration, 'down');
        setSchemaVersion(sql, migration.version - 1);
      }
    }
  }
}

/**
 * Run a single migration
 *
 * @param sql - SqlStorage instance
 * @param migration - Migration to run
 * @param direction - 'up' for upgrade, 'down' for downgrade
 */
export function runMigration(sql: SqlStorage, migration: Migration, direction: 'up' | 'down'): void {
  const sqlScript = direction === 'up' ? migration.up : migration.down;

  // Split the SQL script into individual statements and execute each
  const statements = sqlScript
    .split(';')
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  for (const statement of statements) {
    sql.exec(statement);
  }
}

/**
 * Set the schema version in the database
 *
 * @param sql - SqlStorage instance
 * @param version - Version number to set
 */
function setSchemaVersion(sql: SqlStorage, version: number): void {
  // Ensure schema_meta table exists
  try {
    sql.exec(SCHEMA_META);
  } catch {
    // Table already exists
  }

  // Use INSERT OR REPLACE to upsert
  sql.exec(
    "INSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', ?)",
    String(version)
  );
}

// ============================================================================
// Legacy exports for backwards compatibility (deprecated)
// ============================================================================

/**
 * @deprecated The triples table has been removed in favor of BLOB-only architecture.
 * This constant is kept only for migration purposes.
 */
export const TRIPLES_SCHEMA = '';
