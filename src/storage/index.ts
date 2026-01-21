/**
 * Storage Module
 *
 * Exports GraphCol encoder/decoder for columnar storage of graph triples,
 * and R2 CDC writer for streaming changes to R2 storage.
 *
 * @packageDocumentation
 */

export {
  // Encoder/Decoder functions
  encodeGraphCol,
  decodeGraphCol,
  getChunkStats,
  createEncoder,

  // Constants
  GCOL_MAGIC,
  GCOL_VERSION,
  HEADER_SIZE,

  // Types
  type GraphColHeader,
  type GraphColChunk,
  type GraphColEncoder,
  type ChunkStats,
  type PredicateMeta,
  type ColumnOffset,
} from './graphcol';

export {
  // R2 Writer
  createR2Writer,
  getCDCPath,
  listCDCFiles,
  readCDCFile,
  deleteCDCFile,
  getCDCFileMetadata,

  // Path utilities
  parseNamespaceToPath,
  formatDatePath,
  generateSequence,
  parseCDCPath,

  // Types
  type R2WriterConfig,
  type R2WriterStats,
  type R2Writer,
  type ListCDCFilesOptions,
} from './r2-writer';

export {
  // Compaction
  compactChunks,
  selectChunksForCompaction,
  listChunksAtLevel,
  getCompactionStats,
  CompactionLevel,

  // Types
  type CompactionConfig,
  type CompactionResult,
  type CompactionChunkInfo,
} from './compaction';

export {
  // CDC Types and Buffer
  createCDCBuffer,

  // Types
  type CDCEvent,
  type CDCBuffer,
} from './cdc-types';

export {
  // R2 Restore functions
  listBackups,
  getBackupMetadata,
  findBackupBeforeTimestamp,
  restoreFromBackup,
  restoreFromSnapshot,
  estimateRestoreDuration,
  validateBackup,
  getBackupSize,
  countBackupEvents,
  estimateEventCount,

  // Types
  type BackupSnapshot,
  type ListBackupsOptions,
  type RestoreOptions,
  type RestoreProgress,
  type RestoreResult,
  type RestoreEventHandler,
} from './r2-restore';
