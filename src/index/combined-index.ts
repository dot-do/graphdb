/**
 * Combined Index File Format
 *
 * Single file containing all secondary indexes with offset header.
 * Enables efficient Range requests to load only needed indexes.
 *
 * IMPORTANT: Vectors are stored EXTERNALLY (separate file or service)
 * because they're 10-100x larger than other indexes combined.
 * - Other indexes: 200MB - 1GB for 1M entities
 * - Vectors: 1.5GB - 6GB for 1M entities (384-1536 dimensions)
 *
 * Vector storage options:
 * 1. Separate .vec file in R2 (same Range request pattern)
 * 2. External service (Vectorize, Pinecone, etc.)
 * 3. On-demand loading (don't cache, fetch per query)
 *
 * File Layout:
 * ┌────────────────────────────────────────────────────────────┐
 * │ HEADER (fixed 64 bytes)                                    │
 * │ ├── Magic: "GIDX" (4 bytes)                               │
 * │ ├── Version: uint16 (2 bytes)                             │
 * │ ├── Flags: uint16 (2 bytes)                               │
 * │ ├── Index Count: uint32 (4 bytes)                         │
 * │ ├── Total Size: uint64 (8 bytes)                          │
 * │ ├── Created At: uint64 (8 bytes)                          │
 * │ ├── Namespace Length: uint16 (2 bytes)                    │
 * │ ├── Reserved (34 bytes)                                   │
 * ├────────────────────────────────────────────────────────────┤
 * │ NAMESPACE (variable, padded to 8-byte boundary)           │
 * ├────────────────────────────────────────────────────────────┤
 * │ INDEX DIRECTORY (24 bytes per index)                      │
 * │ ├── Index Type: uint8                                     │
 * │ ├── Compression: uint8                                    │
 * │ ├── Reserved: uint16                                      │
 * │ ├── Offset: uint64                                        │
 * │ ├── Compressed Size: uint64                               │
 * │ ├── Uncompressed Size: uint32                             │
 * ├────────────────────────────────────────────────────────────┤
 * │ INDEX DATA SECTIONS                                        │
 * │ ├── POS Index (JSON, optionally gzipped)                  │
 * │ ├── OSP Index (JSON, optionally gzipped)                  │
 * │ ├── FTS Index (JSON, optionally gzipped)                  │
 * │ ├── GEO Index (JSON, optionally gzipped)                  │
 * │ └── VEC Index (binary, optionally compressed)             │
 * ├────────────────────────────────────────────────────────────┤
 * │ FOOTER (16 bytes)                                          │
 * │ ├── Header Offset: uint64 (always 0, for verification)    │
 * │ ├── CRC32: uint32                                         │
 * │ └── Magic: "XDIG" (4 bytes, reversed)                     │
 * └────────────────────────────────────────────────────────────┘
 *
 * @packageDocumentation
 */

import type { POSIndex, OSPIndex, FTSIndex, GeoIndex, VectorIndex } from './index-store.js';
import { cosineSimilarity } from './hnsw/distance.js';

// Re-export cosineSimilarity from canonical location
export { cosineSimilarity };

// ============================================================================
// CONSTANTS
// ============================================================================

/** Magic bytes: "GIDX" (Graph Index) */
export const GIDX_MAGIC = 0x58444947; // "GIDX" in little-endian
export const GIDX_MAGIC_FOOTER = 0x47494458; // "XDIG" reversed
export const GIDX_VERSION = 1;
export const HEADER_SIZE = 64;
export const DIRECTORY_ENTRY_SIZE = 24;
export const FOOTER_SIZE = 16;

/** Index type identifiers */
export enum IndexType {
  POS = 1,      // Predicate-Object-Subject
  OSP = 2,      // Object-Subject-Predicate (reverse refs)
  FTS = 3,      // Full-Text Search
  GEO = 4,      // Geospatial
  VEC = 5,      // Vector embeddings
  BLOOM = 6,    // Bloom filter for entity existence
  MANIFEST = 7, // Entity manifest (offsets into GraphCol)
}

/** Compression methods */
export enum Compression {
  NONE = 0,
  GZIP = 1,
  ZSTD = 2,  // Future: better compression ratio
  LZ4 = 3,   // Future: faster decompression
}

// ============================================================================
// TYPES
// ============================================================================

/**
 * Index directory entry - describes one index section
 */
export interface IndexDirectoryEntry {
  /** Index type (POS, OSP, FTS, GEO, VEC) */
  type: IndexType;
  /** Compression method */
  compression: Compression;
  /** Byte offset from start of file */
  offset: number;
  /** Size in bytes (compressed) */
  compressedSize: number;
  /** Size in bytes (uncompressed) */
  uncompressedSize: number;
}

/**
 * Combined index file header
 */
export interface CombinedIndexHeader {
  /** Magic bytes verification */
  magic: number;
  /** Format version */
  version: number;
  /** Feature flags */
  flags: number;
  /** Number of index sections */
  indexCount: number;
  /** Total file size */
  totalSize: number;
  /** Creation timestamp */
  createdAt: number;
  /** Namespace this index covers */
  namespace: string;
  /** Index directory */
  directory: IndexDirectoryEntry[];
}

/**
 * All indexes combined
 */
export interface CombinedIndexData {
  pos?: POSIndex;
  osp?: OSPIndex;
  fts?: FTSIndex;
  geo?: GeoIndex;
  vec?: VectorIndex;
}

/**
 * Result of reading the header (for Range request planning)
 */
export interface IndexHeaderInfo {
  header: CombinedIndexHeader;
  /** Byte range needed for each index type */
  ranges: Map<IndexType, { offset: number; length: number }>;
  /** Total header + directory size (data starts after this) */
  headerSize: number;
}

// ============================================================================
// ENCODING
// ============================================================================

/**
 * Encode combined index file
 */
export async function encodeCombinedIndex(
  namespace: string,
  indexes: CombinedIndexData,
  options?: {
    compression?: Compression;
    includeVectors?: boolean;  // Default FALSE - vectors stored separately
  }
): Promise<Uint8Array> {
  const compression = options?.compression ?? Compression.GZIP; // Default to gzip
  const includeVectors = options?.includeVectors ?? false; // Vectors excluded by default

  // Encode each index to JSON/binary
  const sections: Array<{ type: IndexType; data: Uint8Array }> = [];

  if (indexes.pos) {
    const json = JSON.stringify(indexes.pos);
    const data = await compress(new TextEncoder().encode(json), compression);
    sections.push({ type: IndexType.POS, data });
  }

  if (indexes.osp) {
    const json = JSON.stringify(indexes.osp);
    const data = await compress(new TextEncoder().encode(json), compression);
    sections.push({ type: IndexType.OSP, data });
  }

  if (indexes.fts) {
    const json = JSON.stringify(indexes.fts);
    const data = await compress(new TextEncoder().encode(json), compression);
    sections.push({ type: IndexType.FTS, data });
  }

  if (indexes.geo) {
    const json = JSON.stringify(indexes.geo);
    const data = await compress(new TextEncoder().encode(json), compression);
    sections.push({ type: IndexType.GEO, data });
  }

  if (indexes.vec && includeVectors) {
    // Vector index uses binary format for efficiency
    const binary = encodeVectorIndexBinary(indexes.vec);
    const data = await compress(binary, compression);
    sections.push({ type: IndexType.VEC, data });
  }

  // Calculate sizes
  const namespaceBytes = new TextEncoder().encode(namespace);
  const namespacePadded = Math.ceil(namespaceBytes.length / 8) * 8;
  const directorySize = sections.length * DIRECTORY_ENTRY_SIZE;
  const headerTotalSize = HEADER_SIZE + namespacePadded + directorySize;

  // Calculate offsets
  let currentOffset = headerTotalSize;
  const directory: IndexDirectoryEntry[] = [];

  for (const section of sections) {
    directory.push({
      type: section.type,
      compression,
      offset: currentOffset,
      compressedSize: section.data.length,
      uncompressedSize: section.data.length, // TODO: track uncompressed
    });
    currentOffset += section.data.length;
  }

  const totalSize = currentOffset + FOOTER_SIZE;

  // Allocate buffer
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Write header
  view.setUint32(offset, GIDX_MAGIC, true); offset += 4;
  view.setUint16(offset, GIDX_VERSION, true); offset += 2;
  view.setUint16(offset, 0, true); offset += 2; // flags
  view.setUint32(offset, sections.length, true); offset += 4;
  view.setBigUint64(offset, BigInt(totalSize), true); offset += 8;
  view.setBigUint64(offset, BigInt(Date.now()), true); offset += 8;
  view.setUint16(offset, namespaceBytes.length, true); offset += 2;
  offset += 34; // reserved

  // Write namespace
  buffer.set(namespaceBytes, offset);
  offset = HEADER_SIZE + namespacePadded;

  // Write directory
  for (const entry of directory) {
    view.setUint8(offset, entry.type); offset += 1;
    view.setUint8(offset, entry.compression); offset += 1;
    view.setUint16(offset, 0, true); offset += 2; // reserved
    view.setBigUint64(offset, BigInt(entry.offset), true); offset += 8;
    view.setBigUint64(offset, BigInt(entry.compressedSize), true); offset += 8;
    view.setUint32(offset, entry.uncompressedSize, true); offset += 4;
  }

  // Write index data sections
  for (const section of sections) {
    buffer.set(section.data, directory.find(d => d.type === section.type)!.offset);
  }

  // Write footer
  const footerOffset = totalSize - FOOTER_SIZE;
  view.setBigUint64(footerOffset, BigInt(0), true); // header offset
  view.setUint32(footerOffset + 8, calculateCRC32(buffer.subarray(0, footerOffset)), true);
  view.setUint32(footerOffset + 12, GIDX_MAGIC_FOOTER, true);

  return buffer;
}

/**
 * Decode just the header (for Range request planning)
 * Only reads first ~1KB to get directory
 */
export function decodeIndexHeader(headerBytes: Uint8Array): IndexHeaderInfo {
  const view = new DataView(headerBytes.buffer, headerBytes.byteOffset);
  let offset = 0;

  // Read header
  const magic = view.getUint32(offset, true); offset += 4;
  if (magic !== GIDX_MAGIC) {
    throw new Error(`Invalid magic: expected ${GIDX_MAGIC}, got ${magic}`);
  }

  const version = view.getUint16(offset, true); offset += 2;
  const flags = view.getUint16(offset, true); offset += 2;
  const indexCount = view.getUint32(offset, true); offset += 4;
  const totalSize = Number(view.getBigUint64(offset, true)); offset += 8;
  const createdAt = Number(view.getBigUint64(offset, true)); offset += 8;
  const namespaceLength = view.getUint16(offset, true); offset += 2;
  offset += 34; // skip reserved

  // Read namespace
  const namespaceBytes = headerBytes.subarray(offset, offset + namespaceLength);
  const namespace = new TextDecoder().decode(namespaceBytes);
  const namespacePadded = Math.ceil(namespaceLength / 8) * 8;
  offset = HEADER_SIZE + namespacePadded;

  // Read directory
  const directory: IndexDirectoryEntry[] = [];
  const ranges = new Map<IndexType, { offset: number; length: number }>();

  for (let i = 0; i < indexCount; i++) {
    const type = view.getUint8(offset) as IndexType; offset += 1;
    const compression = view.getUint8(offset) as Compression; offset += 1;
    offset += 2; // skip reserved
    const entryOffset = Number(view.getBigUint64(offset, true)); offset += 8;
    const compressedSize = Number(view.getBigUint64(offset, true)); offset += 8;
    const uncompressedSize = view.getUint32(offset, true); offset += 4;

    directory.push({ type, compression, offset: entryOffset, compressedSize, uncompressedSize });
    ranges.set(type, { offset: entryOffset, length: compressedSize });
  }

  return {
    header: {
      magic,
      version,
      flags,
      indexCount,
      totalSize,
      createdAt,
      namespace,
      directory,
    },
    ranges,
    headerSize: offset,
  };
}

/**
 * Decode a single index section using Range request result
 */
export async function decodeIndexSection<T>(
  data: Uint8Array,
  entry: IndexDirectoryEntry
): Promise<T> {
  const decompressed = await decompress(data, entry.compression);

  if (entry.type === IndexType.VEC) {
    return decodeVectorIndexBinary(decompressed) as T;
  }

  const json = new TextDecoder().decode(decompressed);
  return JSON.parse(json) as T;
}

/**
 * Decode entire combined index file
 */
export async function decodeCombinedIndex(data: Uint8Array): Promise<{
  header: CombinedIndexHeader;
  indexes: CombinedIndexData;
}> {
  const { header } = decodeIndexHeader(data);
  const indexes: CombinedIndexData = {};

  for (const entry of header.directory) {
    const sectionData = data.subarray(entry.offset, entry.offset + entry.compressedSize);

    switch (entry.type) {
      case IndexType.POS:
        indexes.pos = await decodeIndexSection<POSIndex>(sectionData, entry);
        break;
      case IndexType.OSP:
        indexes.osp = await decodeIndexSection<OSPIndex>(sectionData, entry);
        break;
      case IndexType.FTS:
        indexes.fts = await decodeIndexSection<FTSIndex>(sectionData, entry);
        break;
      case IndexType.GEO:
        indexes.geo = await decodeIndexSection<GeoIndex>(sectionData, entry);
        break;
      case IndexType.VEC:
        indexes.vec = await decodeIndexSection<VectorIndex>(sectionData, entry);
        break;
    }
  }

  return { header, indexes };
}

// ============================================================================
// RANGE REQUEST HELPERS
// ============================================================================

/**
 * Get the byte range needed to read just the header + directory
 * Use this for the first Range request
 */
export function getHeaderRange(maxNamespaceLength: number = 256, maxIndexes: number = 10): {
  offset: number;
  length: number;
} {
  const namespacePadded = Math.ceil(maxNamespaceLength / 8) * 8;
  const directorySize = maxIndexes * DIRECTORY_ENTRY_SIZE;
  return {
    offset: 0,
    length: HEADER_SIZE + namespacePadded + directorySize,
  };
}

/**
 * Plan Range requests for loading specific indexes
 */
export function planRangeRequests(
  headerInfo: IndexHeaderInfo,
  indexTypes: IndexType[]
): Array<{ type: IndexType; offset: number; length: number }> {
  const requests: Array<{ type: IndexType; offset: number; length: number }> = [];

  for (const type of indexTypes) {
    const range = headerInfo.ranges.get(type);
    if (range) {
      requests.push({ type, ...range });
    }
  }

  // Sort by offset for potential request coalescing
  requests.sort((a, b) => a.offset - b.offset);

  return requests;
}

/**
 * Coalesce adjacent ranges to minimize requests
 * If two ranges are within `gap` bytes, merge them
 */
export function coalesceRanges(
  ranges: Array<{ type: IndexType; offset: number; length: number }>,
  maxGap: number = 4096
): Array<{ types: IndexType[]; offset: number; length: number }> {
  if (ranges.length === 0) return [];

  const sorted = [...ranges].sort((a, b) => a.offset - b.offset);
  const coalesced: Array<{ types: IndexType[]; offset: number; length: number }> = [];

  let current = {
    types: [sorted[0]!.type],
    offset: sorted[0]!.offset,
    length: sorted[0]!.length,
  };

  for (let i = 1; i < sorted.length; i++) {
    const range = sorted[i]!;
    const currentEnd = current.offset + current.length;
    const gap = range.offset - currentEnd;

    if (gap <= maxGap) {
      // Coalesce
      current.types.push(range.type);
      current.length = (range.offset + range.length) - current.offset;
    } else {
      // Start new range
      coalesced.push(current);
      current = {
        types: [range.type],
        offset: range.offset,
        length: range.length,
      };
    }
  }

  coalesced.push(current);
  return coalesced;
}

// ============================================================================
// VECTOR INDEX BINARY FORMAT
// ============================================================================

/**
 * Flag to suppress experimental vector index warnings (for testing)
 * @internal - This state is managed by test utilities in test/index/test-utils.ts
 */
// Note: Vector warning suppression has been moved to test utilities.
// The warning functionality was test-only and should not be part of the public API.

/**
 * Encode vector index to compact binary format
 *
 * Format:
 * [4 bytes: entry count]
 * [4 bytes: dimensions]
 * [4 bytes: M (HNSW parameter)]
 * [4 bytes: efConstruction]
 * [entries...]
 *   [2 bytes: entity_id length]
 *   [N bytes: entity_id]
 *   [2 bytes: predicate length]
 *   [N bytes: predicate]
 *   [dimensions * 4 bytes: vector floats]
 *   [1 byte: layer count (maxLayer + 1)]
 *   [layers...]
 *     [1 byte: connection count]
 *     [connections...]
 *       [2 bytes: connection ID length]
 *       [N bytes: connection ID (string)]
 */
function encodeVectorIndexBinary(index: VectorIndex): Uint8Array {
  // Calculate size - need to account for variable-length connection IDs
  let size = 16; // header: entry count (4) + dimensions (4) + M (4) + efConstruction (4)

  for (const entry of index.entries) {
    const entityIdBytes = new TextEncoder().encode(entry.entityId);
    const predicateBytes = new TextEncoder().encode(entry.predicate);

    size += 2 + entityIdBytes.length;  // entity ID
    size += 2 + predicateBytes.length; // predicate
    size += index.dimensions * 4;       // vector data
    size += 1;                          // layer count

    // For each layer, calculate connection data size
    for (const layer of entry.connections) {
      size += 1; // connection count for this layer (uint8)
      for (const connId of layer) {
        const connIdBytes = new TextEncoder().encode(connId);
        size += 2 + connIdBytes.length; // length prefix + string
      }
    }
  }

  const buffer = new Uint8Array(size);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Header
  view.setUint32(offset, index.entries.length, true); offset += 4;
  view.setUint32(offset, index.dimensions, true); offset += 4;
  view.setUint32(offset, index.m, true); offset += 4;
  view.setUint32(offset, index.efConstruction, true); offset += 4;

  // Entries
  for (const entry of index.entries) {
    // Entity ID
    const entityIdBytes = new TextEncoder().encode(entry.entityId);
    view.setUint16(offset, entityIdBytes.length, true); offset += 2;
    buffer.set(entityIdBytes, offset); offset += entityIdBytes.length;

    // Predicate
    const predicateBytes = new TextEncoder().encode(entry.predicate);
    view.setUint16(offset, predicateBytes.length, true); offset += 2;
    buffer.set(predicateBytes, offset); offset += predicateBytes.length;

    // Vector
    for (let i = 0; i < entry.vector.length; i++) {
      view.setFloat32(offset, entry.vector[i]!, true); offset += 4;
    }

    // Connections - properly encode HNSW graph
    // Layer count (maxLayer + 1, i.e., number of layers this node exists in)
    view.setUint8(offset, entry.connections.length); offset += 1;

    for (const layer of entry.connections) {
      // Connection count for this layer
      view.setUint8(offset, layer.length); offset += 1;

      // Each connection ID as length-prefixed string
      for (const connId of layer) {
        const connIdBytes = new TextEncoder().encode(connId);
        view.setUint16(offset, connIdBytes.length, true); offset += 2;
        buffer.set(connIdBytes, offset); offset += connIdBytes.length;
      }
    }
  }

  return buffer.subarray(0, offset);
}

/**
 * Decode vector index from binary format
 *
 * Format matches encodeVectorIndexBinary():
 * [4 bytes: entry count]
 * [4 bytes: dimensions]
 * [4 bytes: M (HNSW parameter)]
 * [4 bytes: efConstruction]
 * [entries...]
 *   [2 bytes: entity_id length]
 *   [N bytes: entity_id]
 *   [2 bytes: predicate length]
 *   [N bytes: predicate]
 *   [dimensions * 4 bytes: vector floats]
 *   [1 byte: layer count]
 *   [layers...]
 *     [1 byte: connection count]
 *     [connections...]
 *       [2 bytes: connection ID length]
 *       [N bytes: connection ID (string)]
 */
function decodeVectorIndexBinary(data: Uint8Array): VectorIndex {
  const view = new DataView(data.buffer, data.byteOffset);
  let offset = 0;

  // Header
  const entryCount = view.getUint32(offset, true); offset += 4;
  const dimensions = view.getUint32(offset, true); offset += 4;
  const m = view.getUint32(offset, true); offset += 4;
  const efConstruction = view.getUint32(offset, true); offset += 4;

  const entries: VectorIndex['entries'] = [];

  for (let i = 0; i < entryCount; i++) {
    // Entity ID
    const entityIdLength = view.getUint16(offset, true); offset += 2;
    const entityId = new TextDecoder().decode(data.subarray(offset, offset + entityIdLength));
    offset += entityIdLength;

    // Predicate
    const predicateLength = view.getUint16(offset, true); offset += 2;
    const predicate = new TextDecoder().decode(data.subarray(offset, offset + predicateLength));
    offset += predicateLength;

    // Vector
    const vector: number[] = [];
    for (let j = 0; j < dimensions; j++) {
      vector.push(view.getFloat32(offset, true)); offset += 4;
    }

    // Connections - properly decode HNSW graph
    const layerCount = view.getUint8(offset); offset += 1;
    const connections: string[][] = [];

    for (let j = 0; j < layerCount; j++) {
      // Connection count for this layer
      const connCount = view.getUint8(offset); offset += 1;
      const layer: string[] = [];

      // Each connection ID as length-prefixed string
      for (let k = 0; k < connCount; k++) {
        const connIdLength = view.getUint16(offset, true); offset += 2;
        const connId = new TextDecoder().decode(data.subarray(offset, offset + connIdLength));
        offset += connIdLength;
        layer.push(connId);
      }

      connections.push(layer);
    }

    entries.push({ entityId, predicate, vector, connections });
  }

  return { version: 'v1', dimensions, m, efConstruction, entries };
}

// ============================================================================
// COMPRESSION HELPERS
// ============================================================================

/**
 * Compress data using specified method
 */
async function compress(data: Uint8Array, method: Compression): Promise<Uint8Array> {
  if (method === Compression.NONE) {
    return data;
  }

  if (method === Compression.GZIP) {
    // Use CompressionStream (available in Workers)
    const stream = new CompressionStream('gzip');
    const writer = stream.writable.getWriter();
    writer.write(data);
    writer.close();

    const chunks: Uint8Array[] = [];
    const reader = stream.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result;
  }

  throw new Error(`Unsupported compression method: ${method}`);
}

/**
 * Decompress data using specified method
 */
async function decompress(data: Uint8Array, method: Compression): Promise<Uint8Array> {
  if (method === Compression.NONE) {
    return data;
  }

  if (method === Compression.GZIP) {
    const stream = new DecompressionStream('gzip');
    const writer = stream.writable.getWriter();
    writer.write(data);
    writer.close();

    const chunks: Uint8Array[] = [];
    const reader = stream.readable.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    const totalLength = chunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    return result;
  }

  throw new Error(`Unsupported compression method: ${method}`);
}

// ============================================================================
// CRC32 (same as entity-index.ts)
// ============================================================================

const CRC32_TABLE = new Uint32Array(256);
(function initCRC32Table() {
  const polynomial = 0xEDB88320;
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = (crc & 1) ? (crc >>> 1) ^ polynomial : crc >>> 1;
    }
    CRC32_TABLE[i] = crc;
  }
})();

function calculateCRC32(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ data[i]!) & 0xFF]!;
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

// ============================================================================
// QUANTIZED VECTOR FILE FORMAT (separate .qvec file)
// ============================================================================
//
// For vectors, we use a separate file with optional quantization.
// This keeps the main index file small (~200MB) while vectors can be
// stored separately or in an external service.
//
// File Layout (namespace.qvec):
// ┌────────────────────────────────────────────────────────────┐
// │ HEADER (32 bytes)                                          │
// │ ├── Magic: "QVEC" (4 bytes)                               │
// │ ├── Version: uint16                                        │
// │ ├── Quantization: uint8 (0=float32, 1=int8, 2=binary)     │
// │ ├── Reserved: uint8                                        │
// │ ├── Dimensions: uint32                                     │
// │ ├── Vector Count: uint32                                   │
// │ ├── Scale: float32 (for int8 quantization)                │
// │ ├── Offset: float32 (for int8 quantization)               │
// │ └── Reserved: uint8[4]                                    │
// ├────────────────────────────────────────────────────────────┤
// │ ID TABLE (vector_count * id_entry_size)                   │
// │ ├── [2 bytes: entity_id length]                           │
// │ └── [N bytes: entity_id]                                  │
// ├────────────────────────────────────────────────────────────┤
// │ VECTOR DATA (vector_count * dimensions * type_size)       │
// │ └── Contiguous array of vectors (row-major)               │
// └────────────────────────────────────────────────────────────┘
//
// Size comparison for 1M vectors @ 384 dimensions:
// - Float32: 1M * 384 * 4 = 1.5 GB
// - Int8:    1M * 384 * 1 = 384 MB (4x smaller!)
// - Binary:  1M * 384 / 8 = 48 MB  (32x smaller, for binary embeddings)

export const QVEC_MAGIC = 0x43455651; // "QVEC"
export const QVEC_HEADER_SIZE = 32;

export enum VectorQuantization {
  FLOAT32 = 0,  // Full precision (4 bytes per dim)
  INT8 = 1,     // Quantized to int8 (1 byte per dim, 4x compression)
  BINARY = 2,   // Binary hash (1 bit per dim, 32x compression)
}

export interface QuantizedVectorHeader {
  magic: number;
  version: number;
  quantization: VectorQuantization;
  dimensions: number;
  vectorCount: number;
  scale: number;    // For INT8: value = (int8_value / 127) * scale + offset
  offset: number;
}

export interface QuantizedVectorFile {
  header: QuantizedVectorHeader;
  /** Entity IDs in order */
  ids: string[];
  /** Vector data (interpretation depends on quantization) */
  vectors: Float32Array | Int8Array | Uint8Array;
}

/**
 * Encode vectors to quantized format
 */
export function encodeQuantizedVectors(
  vectors: Array<{ entityId: string; vector: number[] }>,
  quantization: VectorQuantization = VectorQuantization.INT8
): Uint8Array {
  if (vectors.length === 0) {
    throw new Error('Cannot encode empty vector array');
  }

  const dimensions = vectors[0]!.vector.length;
  const vectorCount = vectors.length;

  // Calculate quantization parameters for INT8
  let scale = 1.0;
  let offset = 0.0;

  if (quantization === VectorQuantization.INT8) {
    // Find min/max across all vectors
    let min = Infinity;
    let max = -Infinity;
    for (const v of vectors) {
      for (const val of v.vector) {
        if (val < min) min = val;
        if (val > max) max = val;
      }
    }
    scale = (max - min) / 254; // Leave room for rounding
    offset = min;
  }

  // Encode IDs to calculate total size
  const idBuffers: Uint8Array[] = [];
  let idTableSize = 0;
  for (const v of vectors) {
    const idBytes = new TextEncoder().encode(v.entityId);
    idBuffers.push(idBytes);
    idTableSize += 2 + idBytes.length; // 2 bytes length + id
  }

  // Calculate vector data size
  let vectorDataSize: number;
  switch (quantization) {
    case VectorQuantization.FLOAT32:
      vectorDataSize = vectorCount * dimensions * 4;
      break;
    case VectorQuantization.INT8:
      vectorDataSize = vectorCount * dimensions;
      break;
    case VectorQuantization.BINARY:
      vectorDataSize = vectorCount * Math.ceil(dimensions / 8);
      break;
  }

  const totalSize = QVEC_HEADER_SIZE + idTableSize + vectorDataSize;
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let writeOffset = 0;

  // Write header
  view.setUint32(writeOffset, QVEC_MAGIC, true); writeOffset += 4;
  view.setUint16(writeOffset, 1, true); writeOffset += 2; // version
  view.setUint8(writeOffset, quantization); writeOffset += 1;
  view.setUint8(writeOffset, 0); writeOffset += 1; // reserved
  view.setUint32(writeOffset, dimensions, true); writeOffset += 4;
  view.setUint32(writeOffset, vectorCount, true); writeOffset += 4;
  view.setFloat32(writeOffset, scale, true); writeOffset += 4;
  view.setFloat32(writeOffset, offset, true); writeOffset += 4;
  writeOffset = QVEC_HEADER_SIZE; // Skip reserved

  // Write ID table
  for (const idBytes of idBuffers) {
    view.setUint16(writeOffset, idBytes.length, true); writeOffset += 2;
    buffer.set(idBytes, writeOffset); writeOffset += idBytes.length;
  }

  // Write vector data
  for (const v of vectors) {
    switch (quantization) {
      case VectorQuantization.FLOAT32:
        for (const val of v.vector) {
          view.setFloat32(writeOffset, val, true); writeOffset += 4;
        }
        break;

      case VectorQuantization.INT8:
        for (const val of v.vector) {
          // Quantize: map [min, max] to [-127, 127]
          // normalized = (val - offset) / scale  (gives 0 to 254)
          // int8 = normalized - 127              (gives -127 to 127)
          const quantized = Math.round((val - offset) / scale - 127);
          view.setInt8(writeOffset, Math.max(-127, Math.min(127, quantized)));
          writeOffset += 1;
        }
        break;

      case VectorQuantization.BINARY:
        // Pack 8 dimensions per byte
        for (let i = 0; i < dimensions; i += 8) {
          let byte = 0;
          for (let j = 0; j < 8 && i + j < dimensions; j++) {
            if (v.vector[i + j]! > 0) {
              byte |= (1 << j);
            }
          }
          view.setUint8(writeOffset, byte);
          writeOffset += 1;
        }
        break;
    }
  }

  return buffer;
}

/**
 * Decode just the header from quantized vector file
 */
export function decodeQuantizedVectorHeader(data: Uint8Array): QuantizedVectorHeader {
  const view = new DataView(data.buffer, data.byteOffset);

  const magic = view.getUint32(0, true);
  if (magic !== QVEC_MAGIC) {
    throw new Error(`Invalid QVEC magic: expected ${QVEC_MAGIC}, got ${magic}`);
  }

  return {
    magic,
    version: view.getUint16(4, true),
    quantization: view.getUint8(6) as VectorQuantization,
    dimensions: view.getUint32(8, true),
    vectorCount: view.getUint32(12, true),
    scale: view.getFloat32(16, true),
    offset: view.getFloat32(20, true),
  };
}

/**
 * Decode full quantized vector file
 */
export function decodeQuantizedVectors(data: Uint8Array): QuantizedVectorFile {
  const header = decodeQuantizedVectorHeader(data);
  const view = new DataView(data.buffer, data.byteOffset);
  let readOffset = QVEC_HEADER_SIZE;

  // Read ID table
  const ids: string[] = [];
  for (let i = 0; i < header.vectorCount; i++) {
    const idLength = view.getUint16(readOffset, true); readOffset += 2;
    const idBytes = data.subarray(readOffset, readOffset + idLength);
    ids.push(new TextDecoder().decode(idBytes));
    readOffset += idLength;
  }

  // Read vector data
  const { dimensions, vectorCount, quantization } = header;
  let vectors: Float32Array | Int8Array | Uint8Array;

  switch (quantization) {
    case VectorQuantization.FLOAT32: {
      // Float32Array requires 4-byte alignment. The readOffset may not be aligned
      // after reading variable-length entity IDs, so we copy to an aligned buffer.
      const byteLength = vectorCount * dimensions * 4;
      const alignedBuffer = new ArrayBuffer(byteLength);
      new Uint8Array(alignedBuffer).set(data.subarray(readOffset, readOffset + byteLength));
      vectors = new Float32Array(alignedBuffer);
      break;
    }

    case VectorQuantization.INT8: {
      // Int8Array technically doesn't require alignment, but for consistency
      // and to avoid any potential issues, we copy to a fresh buffer.
      const byteLength = vectorCount * dimensions;
      const int8Buffer = new ArrayBuffer(byteLength);
      new Uint8Array(int8Buffer).set(data.subarray(readOffset, readOffset + byteLength));
      vectors = new Int8Array(int8Buffer);
      break;
    }

    case VectorQuantization.BINARY: {
      const bytesPerVector = Math.ceil(dimensions / 8);
      const byteLength = vectorCount * bytesPerVector;
      vectors = new Uint8Array(data.subarray(readOffset, readOffset + byteLength));
      break;
    }
  }

  return { header, ids, vectors };
}

/**
 * Get a single vector by index, dequantized to float32
 */
export function getVectorFloat32(
  file: QuantizedVectorFile,
  index: number
): Float32Array {
  const { header, vectors } = file;
  const { dimensions, quantization, scale, offset } = header;

  const result = new Float32Array(dimensions);

  switch (quantization) {
    case VectorQuantization.FLOAT32:
      const f32 = vectors as Float32Array;
      for (let i = 0; i < dimensions; i++) {
        result[i] = f32[index * dimensions + i]!;
      }
      break;

    case VectorQuantization.INT8:
      const i8 = vectors as Int8Array;
      for (let i = 0; i < dimensions; i++) {
        // Dequantize: reverse of int8 = (val - offset) / scale - 127
        // val = (int8 + 127) * scale + offset
        result[i] = (i8[index * dimensions + i]! + 127) * scale + offset;
      }
      break;

    case VectorQuantization.BINARY:
      const bytesPerVector = Math.ceil(dimensions / 8);
      const u8 = vectors as Uint8Array;
      const baseOffset = index * bytesPerVector;
      for (let i = 0; i < dimensions; i++) {
        const byteIdx = Math.floor(i / 8);
        const bitIdx = i % 8;
        result[i] = (u8[baseOffset + byteIdx]! & (1 << bitIdx)) ? 1.0 : -1.0;
      }
      break;
  }

  return result;
}

/**
 * Compute Hamming distance for binary vectors
 */
export function hammingDistance(a: Uint8Array, b: Uint8Array): number {
  let distance = 0;
  for (let i = 0; i < a.length; i++) {
    // Count differing bits
    let xor = a[i]! ^ b[i]!;
    while (xor) {
      distance += xor & 1;
      xor >>>= 1;
    }
  }
  return distance;
}
