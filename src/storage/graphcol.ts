/**
 * GraphCol Encoder/Decoder
 *
 * GraphCol (Graph Columnar) is a cost-optimized columnar storage format
 * for graph triples. It organizes data by predicate (column-oriented)
 * for efficient compression and fast predicate-filtered queries.
 *
 * Features:
 * - Dictionary encoding for subjects, predicates, refs, strings
 * - Delta encoding for timestamps and integers
 * - RLE encoding for object types
 * - Bitpacking for booleans
 * - Per-predicate column organization
 *
 * @packageDocumentation
 */

import type { Triple, TypedObject } from '../core/triple';
import type { Namespace } from '../core/types';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../core/types';
import type { GeoPoint } from '../core/geo';
import {
  encodeVarint,
  decodeVarint,
  encodeSignedVarint,
  decodeSignedVarint,
} from '../core/encoding';

// ============================================================================
// CONSTANTS
// ============================================================================

/** Magic bytes: "GCOL" */
export const GCOL_MAGIC = 0x4C4F4347; // "GCOL" in little-endian
export const GCOL_VERSION = 1;
export const HEADER_SIZE = 64;

// ============================================================================
// MEMORY SAFETY LIMITS
// ============================================================================

/**
 * Maximum number of elements allowed in a single decoded array.
 * This prevents OOM attacks from malicious/corrupted files claiming huge counts.
 * 1 million elements is generous for legitimate use cases while preventing abuse.
 * Can be increased if needed for specific use cases.
 */
export const MAX_DECODE_ARRAY_SIZE = 1_000_000;

/**
 * Maximum total bytes that can be allocated during a single decode operation.
 * This is a secondary safeguard against memory exhaustion.
 * 256MB is large enough for legitimate chunks but prevents runaway allocations.
 */
export const MAX_DECODE_TOTAL_BYTES = 256 * 1024 * 1024;

/**
 * Maximum number of elements allowed in a single encoded array.
 * This prevents OOM from callers passing excessively large arrays to encode functions.
 * 100,000 elements is conservative - can be increased if needed for specific use cases.
 */
export const MAX_ENCODE_ARRAY_SIZE = 100_000;

/**
 * Maximum total bytes that can be allocated during a single encode operation.
 * 64MB is conservative for encoding - legitimate use cases rarely need more.
 */
export const MAX_ENCODE_TOTAL_BYTES = 64 * 1024 * 1024;

/**
 * Validate that a count read from untrusted input is within safe bounds.
 * Throws an error if the count would cause unsafe memory allocation.
 *
 * @param count - The count value read from the data
 * @param context - Description of what is being decoded (for error messages)
 * @param elementSize - Size in bytes of each element (for total bytes check)
 * @throws Error if count exceeds safe limits
 */
function validateDecodeCount(count: number, context: string, elementSize: number = 1): void {
  if (count < 0) {
    throw new Error(`Invalid ${context} count: negative value ${count}`);
  }
  if (count > MAX_DECODE_ARRAY_SIZE) {
    throw new Error(
      `${context} count ${count} exceeds maximum allowed (${MAX_DECODE_ARRAY_SIZE}). ` +
      `This may indicate a corrupted or malicious file.`
    );
  }
  const totalBytes = count * elementSize;
  if (totalBytes > MAX_DECODE_TOTAL_BYTES) {
    throw new Error(
      `${context} would allocate ${totalBytes} bytes, exceeding maximum (${MAX_DECODE_TOTAL_BYTES}). ` +
      `This may indicate a corrupted or malicious file.`
    );
  }
}

/**
 * Validate that an array to be encoded is within safe bounds.
 * Throws an error if the array is too large to safely encode.
 *
 * @param count - The number of elements to encode
 * @param context - Description of what is being encoded (for error messages)
 * @param elementSize - Size in bytes of each element (for total bytes check)
 * @throws Error if count exceeds safe limits
 */
function validateEncodeCount(count: number, context: string, elementSize: number = 1): void {
  if (count < 0) {
    throw new Error(`Invalid ${context} count: negative value ${count}`);
  }
  if (count > MAX_ENCODE_ARRAY_SIZE) {
    throw new Error(
      `${context} count ${count} exceeds maximum allowed (${MAX_ENCODE_ARRAY_SIZE}). ` +
      `Consider chunking the data or increasing MAX_ENCODE_ARRAY_SIZE.`
    );
  }
  const totalBytes = count * elementSize;
  if (totalBytes > MAX_ENCODE_TOTAL_BYTES) {
    throw new Error(
      `${context} would allocate ${totalBytes} bytes, exceeding maximum (${MAX_ENCODE_TOTAL_BYTES}). ` +
      `Consider chunking the data or increasing MAX_ENCODE_TOTAL_BYTES.`
    );
  }
}

// ============================================================================
// TYPES
// ============================================================================

/**
 * Metadata about a predicate column in the chunk
 */
export interface PredicateMeta {
  /** Predicate ID (index in predicate dictionary) */
  id: number;
  /** Predicate name */
  name: string;
  /** Primary object type for this predicate */
  primaryType: number;
  /** Number of triples with this predicate */
  tripleCount: number;
  /** Column IDs for this predicate's data */
  columnIds: number[];
}

/**
 * Column offset and encoding info
 */
export interface ColumnOffset {
  /** Column ID */
  columnId: number;
  /** Object type for this column */
  objectType: number;
  /** Byte offset in data section */
  offset: number;
  /** Byte length */
  length: number;
  /** Encoding method */
  encoding: 'dictionary' | 'delta' | 'rle' | 'plain';
}

/**
 * GraphCol chunk header
 */
export interface GraphColHeader {
  /** Magic bytes (GCOL) */
  magic: 'GCOL';
  /** Format version */
  version: 1;
  /** Namespace for this chunk */
  namespace: Namespace;
  /** Number of unique predicates */
  predicateCount: number;
  /** Predicate metadata */
  predicates: PredicateMeta[];
  /** Total number of triples */
  totalTriples: number;
  /** Minimum timestamp in chunk */
  minTimestamp: bigint;
  /** Maximum timestamp in chunk */
  maxTimestamp: bigint;
  /** Column directory */
  columnDirectory: ColumnOffset[];
}

/**
 * GraphCol chunk structure
 */
export interface GraphColChunk {
  header: GraphColHeader;
  data: Uint8Array;
}

/**
 * Chunk statistics (without full decode)
 */
export interface ChunkStats {
  tripleCount: number;
  predicates: string[];
  timeRange: [bigint, bigint];
  sizeBytes: number;
}

/**
 * Streaming encoder interface for CDC
 */
export interface GraphColEncoder {
  /** Add a triple to the encoder buffer */
  addTriple(triple: Triple): void;
  /** Flush accumulated triples to encoded bytes */
  flush(): Uint8Array;
  /** Reset encoder state */
  reset(): void;
}

// ============================================================================
// ENCODING HELPERS
// ============================================================================

/**
 * Encode a string array as dictionary + varint indices
 */
function encodeDictionary(values: string[]): { dictionary: string[]; indices: number[] } {
  const dict = new Map<string, number>();
  const indices: number[] = [];

  for (const v of values) {
    if (!dict.has(v)) {
      dict.set(v, dict.size);
    }
    indices.push(dict.get(v)!);
  }

  return { dictionary: Array.from(dict.keys()), indices };
}

/**
 * Serialize dictionary to bytes
 */
function serializeDictionary(dictionary: string[], indices: number[]): Uint8Array {
  // Validate dictionary size
  validateEncodeCount(dictionary.length, 'dictionary', 32);
  // Validate indices count
  validateEncodeCount(indices.length, 'dictionary indices', 5);

  const encoder = new TextEncoder();
  const encodedStrings = dictionary.map(s => encoder.encode(s));
  const dictBytes = encodedStrings.reduce((sum, e) => sum + e.length + 4, 0);
  const maxVarintBytes = indices.length * 5;

  // Validate total allocation size
  const totalSize = 4 + dictBytes + maxVarintBytes;
  if (totalSize > MAX_ENCODE_TOTAL_BYTES) {
    throw new Error(
      `Dictionary serialization would allocate ${totalSize} bytes, exceeding maximum (${MAX_ENCODE_TOTAL_BYTES}). ` +
      `Consider chunking the data or increasing MAX_ENCODE_TOTAL_BYTES.`
    );
  }

  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Dictionary size
  view.setUint32(offset, dictionary.length, true);
  offset += 4;

  // Dictionary entries (length-prefixed)
  for (const e of encodedStrings) {
    view.setUint32(offset, e.length, true);
    offset += 4;
    buffer.set(e, offset);
    offset += e.length;
  }

  // Varint-encoded indices
  for (const idx of indices) {
    offset = encodeVarint(idx, buffer, offset);
  }

  return buffer.subarray(0, offset);
}

/**
 * Deserialize dictionary from bytes
 */
function deserializeDictionary(data: Uint8Array, count: number): { dictionary: string[]; indices: number[] } {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const decoder = new TextDecoder();
  let offset = 0;

  // Dictionary size
  const dictSize = view.getUint32(offset, true);
  offset += 4;

  // Validate dictionary size before allocation (estimate 32 bytes avg per string entry)
  validateDecodeCount(dictSize, 'dictionary', 32);

  // Validate indices count before allocation (4 bytes per index)
  validateDecodeCount(count, 'dictionary indices', 4);

  // Dictionary entries
  const dictionary: string[] = [];
  let totalStringBytes = 0;
  for (let i = 0; i < dictSize; i++) {
    const len = view.getUint32(offset, true);
    offset += 4;

    // Validate individual string length
    if (len > MAX_DECODE_TOTAL_BYTES) {
      throw new Error(
        `Dictionary string ${i} length ${len} exceeds maximum allowed (${MAX_DECODE_TOTAL_BYTES}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }

    // Track total bytes to prevent cumulative overflow
    totalStringBytes += len;
    if (totalStringBytes > MAX_DECODE_TOTAL_BYTES) {
      throw new Error(
        `Dictionary total string size ${totalStringBytes} exceeds maximum allowed (${MAX_DECODE_TOTAL_BYTES}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }

    dictionary.push(decoder.decode(data.subarray(offset, offset + len)));
    offset += len;
  }

  // Varint-encoded indices
  const indices: number[] = [];
  for (let i = 0; i < count; i++) {
    const result = decodeVarint(data, offset);
    indices.push(result.value);
    offset = result.newOffset;
  }

  return { dictionary, indices };
}

/**
 * RLE encode uint8 array
 */
interface RLERun {
  value: number;
  count: number;
}

function encodeRLE(values: Uint8Array): RLERun[] {
  if (values.length === 0) return [];

  const runs: RLERun[] = [];
  let currentValue = values[0]!;
  let currentCount = 1;

  for (let i = 1; i < values.length; i++) {
    if (values[i] === currentValue && currentCount < 65535) {
      currentCount++;
    } else {
      runs.push({ value: currentValue, count: currentCount });
      currentValue = values[i]!;
      currentCount = 1;
    }
  }

  runs.push({ value: currentValue, count: currentCount });
  return runs;
}

function serializeRLE(runs: RLERun[]): Uint8Array {
  const buffer = new Uint8Array(4 + runs.length * 3);
  const view = new DataView(buffer.buffer);

  view.setUint32(0, runs.length, true);
  let offset = 4;

  for (const run of runs) {
    buffer[offset++] = run.value;
    view.setUint16(offset, run.count, true);
    offset += 2;
  }

  return buffer.subarray(0, offset);
}

function deserializeRLE(data: Uint8Array): Uint8Array {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const runCount = view.getUint32(0, true);

  // Validate run count before allocation (3 bytes per run: 1 value + 2 count)
  validateDecodeCount(runCount, 'RLE runs', 3);

  let offset = 4;

  // Calculate total count
  let totalCount = 0;
  const runs: RLERun[] = [];
  for (let i = 0; i < runCount; i++) {
    const value = data[offset++]!;
    const count = view.getUint16(offset, true);
    offset += 2;
    runs.push({ value, count });
    totalCount += count;

    // Check expanded size incrementally to fail fast
    if (totalCount > MAX_DECODE_ARRAY_SIZE) {
      throw new Error(
        `RLE expanded size ${totalCount} exceeds maximum allowed (${MAX_DECODE_ARRAY_SIZE}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }
  }

  // Validate total expanded size before allocation
  validateDecodeCount(totalCount, 'RLE expanded array', 1);

  // Expand runs
  const result = new Uint8Array(totalCount);
  let resultOffset = 0;
  for (const run of runs) {
    result.fill(run.value, resultOffset, resultOffset + run.count);
    resultOffset += run.count;
  }

  return result;
}

/**
 * CRC32 lookup table (IEEE 802.3 polynomial)
 */
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

/**
 * Calculate CRC32 checksum (IEEE 802.3 standard)
 * Used for data integrity validation in GraphCol format
 */
function calculateChecksum(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ data[i]!) & 0xFF]!;
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}

// ============================================================================
// MAIN ENCODER
// ============================================================================

/**
 * Encode triples to GraphCol format
 */
export function encodeGraphCol(triples: Triple[], namespace: Namespace): Uint8Array {
  if (triples.length === 0) {
    return createEmptyChunk(namespace);
  }

  // Validate input size to prevent OOM
  // Estimate ~200 bytes per triple for the encoded format
  validateEncodeCount(triples.length, 'triples', 200);

  // Extract data into columnar arrays
  const subjects: string[] = [];
  const predicates: string[] = [];
  const objTypes: number[] = [];
  const timestamps: bigint[] = [];
  const txIds: string[] = [];

  // Object values by type
  const stringValues: string[] = [];
  const intValues: bigint[] = [];
  const floatValues: number[] = [];
  const boolValues: boolean[] = [];
  const timestampObjValues: bigint[] = [];
  const refValues: string[] = [];
  const geoPointValues: { lat: number; lng: number }[] = [];
  const binaryValues: Uint8Array[] = [];
  const jsonValues: string[] = [];
  const dateValues: number[] = [];
  const durationValues: string[] = [];
  const urlValues: string[] = [];
  const refArrayValues: string[][] = [];

  // Map to track which object value index corresponds to which triple
  const objValueIndices: number[] = [];

  for (const triple of triples) {
    subjects.push(triple.subject);
    predicates.push(triple.predicate);
    objTypes.push(triple.object.type);
    timestamps.push(triple.timestamp);
    txIds.push(triple.txId);

    // Extract object value by type
    switch (triple.object.type) {
      case ObjectType.NULL:
        objValueIndices.push(-1);
        break;
      case ObjectType.BOOL:
        objValueIndices.push(boolValues.length);
        boolValues.push(triple.object.value ?? false);
        break;
      case ObjectType.INT32:
      case ObjectType.INT64:
        objValueIndices.push(intValues.length);
        intValues.push(triple.object.value ?? 0n);
        break;
      case ObjectType.FLOAT64:
        objValueIndices.push(floatValues.length);
        floatValues.push(triple.object.value ?? 0);
        break;
      case ObjectType.STRING:
        objValueIndices.push(stringValues.length);
        stringValues.push(triple.object.value ?? '');
        break;
      case ObjectType.BINARY:
        objValueIndices.push(binaryValues.length);
        binaryValues.push(triple.object.value ?? new Uint8Array(0));
        break;
      case ObjectType.TIMESTAMP:
        objValueIndices.push(timestampObjValues.length);
        timestampObjValues.push(triple.object.value ?? 0n);
        break;
      case ObjectType.DATE:
        objValueIndices.push(dateValues.length);
        dateValues.push(triple.object.value ?? 0);
        break;
      case ObjectType.DURATION:
        objValueIndices.push(durationValues.length);
        durationValues.push(triple.object.value ?? '');
        break;
      case ObjectType.REF:
        objValueIndices.push(refValues.length);
        refValues.push(triple.object.value ?? '');
        break;
      case ObjectType.REF_ARRAY:
        objValueIndices.push(refArrayValues.length);
        refArrayValues.push(triple.object.value ?? []);
        break;
      case ObjectType.JSON:
        objValueIndices.push(jsonValues.length);
        jsonValues.push(JSON.stringify(triple.object.value));
        break;
      case ObjectType.GEO_POINT:
        objValueIndices.push(geoPointValues.length);
        geoPointValues.push(triple.object.value ?? { lat: 0, lng: 0 });
        break;
      case ObjectType.URL:
        objValueIndices.push(urlValues.length);
        urlValues.push(triple.object.value ?? '');
        break;
      default:
        objValueIndices.push(-1);
    }
  }

  // Encode columns
  const columns: Uint8Array[] = [];

  // 1. Subjects (dictionary encoded)
  const subjectDict = encodeDictionary(subjects);
  const subjectData = serializeDictionary(subjectDict.dictionary, subjectDict.indices);
  columns.push(subjectData);

  // 2. Predicates (dictionary encoded)
  const predicateDict = encodeDictionary(predicates);
  const predicateData = serializeDictionary(predicateDict.dictionary, predicateDict.indices);
  columns.push(predicateData);

  // 3. Object types (RLE encoded)
  const objTypesRLE = encodeRLE(new Uint8Array(objTypes));
  const objTypesData = serializeRLE(objTypesRLE);
  columns.push(objTypesData);

  // 4. Object value indices (varint)
  const objIndicesBuffer = new Uint8Array(objValueIndices.length * 5 + 4);
  const objIndicesView = new DataView(objIndicesBuffer.buffer);
  objIndicesView.setUint32(0, objValueIndices.length, true);
  let objIndicesOffset = 4;
  for (const idx of objValueIndices) {
    // Encode -1 as 0, others as idx + 1
    objIndicesOffset = encodeVarint(idx + 1, objIndicesBuffer, objIndicesOffset);
  }
  columns.push(objIndicesBuffer.subarray(0, objIndicesOffset));

  // 5. Timestamps (delta + varint)
  const timestampsBuffer = encodeTimestamps(timestamps);
  columns.push(timestampsBuffer);

  // 6. TxIds (dictionary encoded)
  const txIdDict = encodeDictionary(txIds);
  const txIdData = serializeDictionary(txIdDict.dictionary, txIdDict.indices);
  columns.push(txIdData);

  // 7. Object values by type
  // String values
  if (stringValues.length > 0) {
    const strDict = encodeDictionary(stringValues);
    const strData = serializeDictionary(strDict.dictionary, strDict.indices);
    columns.push(encodeTypeMarker(ObjectType.STRING, strData));
  }

  // Int values (delta + zigzag)
  if (intValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.INT64, encodeInt64Array(intValues)));
  }

  // Float values
  if (floatValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.FLOAT64, encodeFloat64Array(floatValues)));
  }

  // Bool values
  if (boolValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.BOOL, encodeBoolArray(boolValues)));
  }

  // Timestamp object values
  if (timestampObjValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.TIMESTAMP, encodeTimestamps(timestampObjValues)));
  }

  // Ref values
  if (refValues.length > 0) {
    const refDict = encodeDictionary(refValues);
    const refData = serializeDictionary(refDict.dictionary, refDict.indices);
    columns.push(encodeTypeMarker(ObjectType.REF, refData));
  }

  // GeoPoint values
  if (geoPointValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.GEO_POINT, encodeGeoPoints(geoPointValues)));
  }

  // Binary values
  if (binaryValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.BINARY, encodeBinaryArray(binaryValues)));
  }

  // JSON values
  if (jsonValues.length > 0) {
    const jsonDict = encodeDictionary(jsonValues);
    const jsonData = serializeDictionary(jsonDict.dictionary, jsonDict.indices);
    columns.push(encodeTypeMarker(ObjectType.JSON, jsonData));
  }

  // Date values
  if (dateValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.DATE, encodeInt32Array(dateValues)));
  }

  // Duration values
  if (durationValues.length > 0) {
    const durDict = encodeDictionary(durationValues);
    const durData = serializeDictionary(durDict.dictionary, durDict.indices);
    columns.push(encodeTypeMarker(ObjectType.DURATION, durData));
  }

  // URL values
  if (urlValues.length > 0) {
    const urlDict = encodeDictionary(urlValues);
    const urlData = serializeDictionary(urlDict.dictionary, urlDict.indices);
    columns.push(encodeTypeMarker(ObjectType.URL, urlData));
  }

  // REF_ARRAY values
  if (refArrayValues.length > 0) {
    columns.push(encodeTypeMarker(ObjectType.REF_ARRAY, encodeRefArrays(refArrayValues)));
  }

  // Calculate min/max timestamps
  let minTs = timestamps[0]!;
  let maxTs = timestamps[0]!;
  for (let i = 1; i < timestamps.length; i++) {
    if (timestamps[i]! < minTs) minTs = timestamps[i]!;
    if (timestamps[i]! > maxTs) maxTs = timestamps[i]!;
  }

  // Build the full chunk
  const encoder = new TextEncoder();
  const namespaceBytes = encoder.encode(namespace);

  // Calculate total size
  let totalColumnsSize = 0;
  for (const col of columns) {
    totalColumnsSize += col.length;
  }

  // Header: magic(4) + version(2) + tripleCount(4) + flags(2) + minTs(8) + maxTs(8) +
  //         namespaceLen(2) + namespace + predicateCount(2) + predicateNames + columnCount(2) + columnOffsets
  const headerSize = HEADER_SIZE + namespaceBytes.length + 2 +
    predicateDict.dictionary.reduce((sum, p) => sum + encoder.encode(p).length + 2, 0) +
    2 + columns.length * 8;

  const totalSize = headerSize + totalColumnsSize + 4; // +4 for footer checksum
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Write header
  view.setUint32(offset, GCOL_MAGIC, true);
  offset += 4;
  view.setUint16(offset, GCOL_VERSION, true);
  offset += 2;
  view.setUint32(offset, triples.length, true);
  offset += 4;
  view.setUint16(offset, 0, true); // flags
  offset += 2;
  view.setBigInt64(offset, minTs, true);
  offset += 8;
  view.setBigInt64(offset, maxTs, true);
  offset += 8;

  // Namespace
  view.setUint16(offset, namespaceBytes.length, true);
  offset += 2;
  buffer.set(namespaceBytes, offset);
  offset += namespaceBytes.length;

  // Predicate names
  view.setUint16(offset, predicateDict.dictionary.length, true);
  offset += 2;
  for (const pred of predicateDict.dictionary) {
    const predBytes = encoder.encode(pred);
    view.setUint16(offset, predBytes.length, true);
    offset += 2;
    buffer.set(predBytes, offset);
    offset += predBytes.length;
  }

  // Column count and offsets
  view.setUint16(offset, columns.length, true);
  offset += 2;

  let dataOffset = offset + columns.length * 8;
  for (const col of columns) {
    view.setUint32(offset, dataOffset, true);
    offset += 4;
    view.setUint32(offset, col.length, true);
    offset += 4;
    dataOffset += col.length;
  }

  // Write column data
  for (const col of columns) {
    buffer.set(col, offset);
    offset += col.length;
  }

  // Write checksum
  const checksum = calculateChecksum(buffer.subarray(0, offset));
  view.setUint32(offset, checksum, true);
  offset += 4;

  return buffer.subarray(0, offset);
}

function createEmptyChunk(namespace: Namespace): Uint8Array {
  const encoder = new TextEncoder();
  const namespaceBytes = encoder.encode(namespace);

  // Calculate actual size: header fields + namespace + predicate count + column count + checksum
  // Header: magic(4) + version(2) + tripleCount(4) + flags(2) + minTs(8) + maxTs(8) = 28
  // + namespaceLen(2) + namespace + predicateCount(2) + columnCount(2) + checksum(4)
  const size = 28 + 2 + namespaceBytes.length + 2 + 2 + 4;
  const buffer = new Uint8Array(size);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  view.setUint32(offset, GCOL_MAGIC, true);
  offset += 4;
  view.setUint16(offset, GCOL_VERSION, true);
  offset += 2;
  view.setUint32(offset, 0, true); // tripleCount = 0
  offset += 4;
  view.setUint16(offset, 0, true); // flags
  offset += 2;
  view.setBigInt64(offset, 0n, true); // minTs
  offset += 8;
  view.setBigInt64(offset, 0n, true); // maxTs
  offset += 8;

  // Namespace
  view.setUint16(offset, namespaceBytes.length, true);
  offset += 2;
  buffer.set(namespaceBytes, offset);
  offset += namespaceBytes.length;

  // Predicate count = 0
  view.setUint16(offset, 0, true);
  offset += 2;

  // Column count = 0
  view.setUint16(offset, 0, true);
  offset += 2;

  // Checksum (of all data up to this point)
  const checksum = calculateChecksum(buffer.subarray(0, offset));
  view.setUint32(offset, checksum, true);
  offset += 4;

  return buffer.subarray(0, offset);
}

function encodeTypeMarker(type: number, data: Uint8Array): Uint8Array {
  const buffer = new Uint8Array(1 + data.length);
  buffer[0] = type;
  buffer.set(data, 1);
  return buffer;
}

function encodeTimestamps(timestamps: bigint[]): Uint8Array {
  if (timestamps.length === 0) {
    const buf = new Uint8Array(4);
    new DataView(buf.buffer).setUint32(0, 0, true);
    return buf;
  }

  // Validate input size to prevent OOM (10 bytes max per varint-encoded timestamp)
  validateEncodeCount(timestamps.length, 'timestamps', 10);

  // Delta encoding
  const buffer = new Uint8Array(4 + timestamps.length * 10);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, timestamps.length, true);
  let offset = 4;

  // First value stored as-is
  offset = encodeSignedVarint(timestamps[0]!, buffer, offset);

  // Subsequent values as deltas
  for (let i = 1; i < timestamps.length; i++) {
    const delta = timestamps[i]! - timestamps[i - 1]!;
    offset = encodeSignedVarint(delta, buffer, offset);
  }

  return buffer.subarray(0, offset);
}

function decodeTimestamps(data: Uint8Array): bigint[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);
  if (count === 0) return [];

  // Validate count before allocation (8 bytes per bigint)
  validateDecodeCount(count, 'timestamps', 8);

  const timestamps: bigint[] = new Array(count);
  let offset = 4;

  // First value
  let result = decodeSignedVarint(data, offset);
  timestamps[0] = result.value;
  offset = result.newOffset;

  // Subsequent values are deltas
  for (let i = 1; i < count; i++) {
    result = decodeSignedVarint(data, offset);
    timestamps[i] = timestamps[i - 1]! + result.value;
    offset = result.newOffset;
  }

  return timestamps;
}

function encodeInt64Array(values: bigint[]): Uint8Array {
  // Validate input size to prevent OOM (10 bytes max per varint-encoded value)
  validateEncodeCount(values.length, 'int64 array', 10);

  const buffer = new Uint8Array(4 + values.length * 10);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, values.length, true);
  let offset = 4;

  for (const value of values) {
    offset = encodeSignedVarint(value, buffer, offset);
  }

  return buffer.subarray(0, offset);
}

function decodeInt64Array(data: Uint8Array): bigint[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (8 bytes per bigint)
  validateDecodeCount(count, 'int64 array', 8);

  const values: bigint[] = new Array(count);
  let offset = 4;

  for (let i = 0; i < count; i++) {
    const result = decodeSignedVarint(data, offset);
    values[i] = result.value;
    offset = result.newOffset;
  }

  return values;
}

function encodeInt32Array(values: number[]): Uint8Array {
  // Validate input size to prevent OOM (5 bytes max per varint-encoded value)
  validateEncodeCount(values.length, 'int32 array', 5);

  const buffer = new Uint8Array(4 + values.length * 5);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, values.length, true);
  let offset = 4;

  for (const value of values) {
    // ZigZag for signed values
    const zigzag = value >= 0 ? value << 1 : ((-value) << 1) - 1;
    offset = encodeVarint(zigzag >>> 0, buffer, offset);
  }

  return buffer.subarray(0, offset);
}

function decodeInt32Array(data: Uint8Array): number[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (4 bytes per number)
  validateDecodeCount(count, 'int32 array', 4);

  const values: number[] = new Array(count);
  let offset = 4;

  for (let i = 0; i < count; i++) {
    const result = decodeVarint(data, offset);
    // ZigZag decode
    values[i] = (result.value >>> 1) ^ (-(result.value & 1));
    offset = result.newOffset;
  }

  return values;
}

function encodeFloat64Array(values: number[]): Uint8Array {
  // Validate input size to prevent OOM (8 bytes per float64)
  validateEncodeCount(values.length, 'float64 array', 8);

  const buffer = new Uint8Array(4 + values.length * 8);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, values.length, true);

  for (let i = 0; i < values.length; i++) {
    view.setFloat64(4 + i * 8, values[i]!, true);
  }

  return buffer;
}

function decodeFloat64Array(data: Uint8Array): number[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (8 bytes per float64)
  validateDecodeCount(count, 'float64 array', 8);

  const values: number[] = new Array(count);

  for (let i = 0; i < count; i++) {
    values[i] = view.getFloat64(4 + i * 8, true);
  }

  return values;
}

function encodeBoolArray(values: boolean[]): Uint8Array {
  // Validate input size to prevent OOM (1 bit per boolean, but still count elements)
  validateEncodeCount(values.length, 'bool array', 1);

  const byteCount = Math.ceil(values.length / 8);
  const buffer = new Uint8Array(4 + byteCount);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, values.length, true);

  for (let i = 0; i < values.length; i++) {
    if (values[i]!) {
      buffer[4 + (i >>> 3)]! |= 1 << (i & 7);
    }
  }

  return buffer;
}

function decodeBoolArray(data: Uint8Array): boolean[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (1 byte per boolean, packed in bits but allocated as booleans)
  validateDecodeCount(count, 'bool array', 1);

  const values: boolean[] = new Array(count);

  for (let i = 0; i < count; i++) {
    values[i] = (data[4 + (i >>> 3)]! & (1 << (i & 7))) !== 0;
  }

  return values;
}

function encodeGeoPoints(points: { lat: number; lng: number }[]): Uint8Array {
  // Validate input size to prevent OOM (16 bytes per geo point: 2 float64s)
  validateEncodeCount(points.length, 'geo points', 16);

  const buffer = new Uint8Array(4 + points.length * 16);
  const view = new DataView(buffer.buffer);
  view.setUint32(0, points.length, true);

  for (let i = 0; i < points.length; i++) {
    view.setFloat64(4 + i * 16, points[i]!.lat, true);
    view.setFloat64(4 + i * 16 + 8, points[i]!.lng, true);
  }

  return buffer;
}

function decodeGeoPoints(data: Uint8Array): { lat: number; lng: number }[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (16 bytes per geo point: 2 float64s)
  validateDecodeCount(count, 'geo points', 16);

  const points: { lat: number; lng: number }[] = new Array(count);

  for (let i = 0; i < count; i++) {
    points[i] = {
      lat: view.getFloat64(4 + i * 16, true),
      lng: view.getFloat64(4 + i * 16 + 8, true),
    };
  }

  return points;
}

function encodeBinaryArray(values: Uint8Array[]): Uint8Array {
  // Validate array count first
  validateEncodeCount(values.length, 'binary array', 8);

  // Calculate total length and validate
  const totalLen = values.reduce((sum, v) => sum + v.length + 4, 4);
  if (totalLen > MAX_ENCODE_TOTAL_BYTES) {
    throw new Error(
      `Binary array total size ${totalLen} exceeds maximum allowed (${MAX_ENCODE_TOTAL_BYTES}). ` +
      `Consider chunking the data or increasing MAX_ENCODE_TOTAL_BYTES.`
    );
  }

  const buffer = new Uint8Array(totalLen);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  view.setUint32(offset, values.length, true);
  offset += 4;

  for (const v of values) {
    view.setUint32(offset, v.length, true);
    offset += 4;
    buffer.set(v, offset);
    offset += v.length;
  }

  return buffer;
}

function decodeBinaryArray(data: Uint8Array): Uint8Array[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const count = view.getUint32(0, true);

  // Validate count before allocation (estimate 8 bytes per reference in array)
  validateDecodeCount(count, 'binary array', 8);

  const values: Uint8Array[] = [];
  let offset = 4;
  let totalBinaryBytes = 0;

  for (let i = 0; i < count; i++) {
    const len = view.getUint32(offset, true);
    offset += 4;

    // Validate individual binary length
    if (len > MAX_DECODE_TOTAL_BYTES) {
      throw new Error(
        `Binary element ${i} length ${len} exceeds maximum allowed (${MAX_DECODE_TOTAL_BYTES}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }

    // Track total bytes to prevent cumulative overflow
    totalBinaryBytes += len;
    if (totalBinaryBytes > MAX_DECODE_TOTAL_BYTES) {
      throw new Error(
        `Binary array total size ${totalBinaryBytes} exceeds maximum allowed (${MAX_DECODE_TOTAL_BYTES}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }

    values.push(data.slice(offset, offset + len));
    offset += len;
  }

  return values;
}

function encodeRefArrays(arrays: string[][]): Uint8Array {
  // Validate outer array count
  validateEncodeCount(arrays.length, 'ref arrays', 8);

  // Flatten all refs and encode as dictionary + length-prefixed indices
  const allRefs: string[] = [];
  const arraySizes: number[] = [];

  for (const arr of arrays) {
    arraySizes.push(arr.length);
    allRefs.push(...arr);
  }

  // Validate total refs count
  validateEncodeCount(allRefs.length, 'ref arrays total refs', 32);

  const refDict = encodeDictionary(allRefs);
  const refData = serializeDictionary(refDict.dictionary, refDict.indices);

  // Prepend array count and sizes
  const sizesBuffer = new Uint8Array(4 + arraySizes.length * 4);
  const sizesView = new DataView(sizesBuffer.buffer);
  sizesView.setUint32(0, arrays.length, true);
  for (let i = 0; i < arraySizes.length; i++) {
    sizesView.setUint32(4 + i * 4, arraySizes[i]!, true);
  }

  const result = new Uint8Array(sizesBuffer.length + refData.length);
  result.set(sizesBuffer, 0);
  result.set(refData, sizesBuffer.length);

  return result;
}

function decodeRefArrays(data: Uint8Array): string[][] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const arrayCount = view.getUint32(0, true);

  // Validate array count before allocation
  validateDecodeCount(arrayCount, 'ref arrays', 8);

  let offset = 4;

  const arraySizes: number[] = [];
  let totalRefs = 0;
  for (let i = 0; i < arrayCount; i++) {
    const size = view.getUint32(offset, true);
    arraySizes.push(size);
    totalRefs += size;
    offset += 4;

    // Check totalRefs incrementally to fail fast
    if (totalRefs > MAX_DECODE_ARRAY_SIZE) {
      throw new Error(
        `Ref arrays total refs ${totalRefs} exceeds maximum allowed (${MAX_DECODE_ARRAY_SIZE}). ` +
        `This may indicate a corrupted or malicious file.`
      );
    }
  }

  // Validate total refs
  validateDecodeCount(totalRefs, 'ref arrays total refs', 8);

  const { dictionary, indices } = deserializeDictionary(data.subarray(offset), totalRefs);

  const arrays: string[][] = [];
  let refIdx = 0;
  for (const size of arraySizes) {
    const arr: string[] = [];
    for (let i = 0; i < size; i++) {
      arr.push(dictionary[indices[refIdx++]!]!);
    }
    arrays.push(arr);
  }

  return arrays;
}

// ============================================================================
// DECODER
// ============================================================================

/**
 * Options for decoding GraphCol data
 */
export interface DecodeGraphColOptions {
  /**
   * List of predicate/column names to decode.
   * If provided and non-empty, only triples with these predicates will be returned.
   * If omitted or empty, all triples are returned.
   */
  columns?: string[];
}

// ============================================================================
// V2 FORMAT DETECTION CONSTANTS
// ============================================================================

/**
 * V2 footer size in bytes (excluding trailer).
 * Layout: version(4) + dataLength(4) + indexOffset(4) + indexLength(4) +
 *         entityCount(4) + minTs(8) + maxTs(8) + crc32(4) + reserved(8) = 48 bytes
 */
const V2_FOOTER_SIZE = 48;

/** V2 trailer size: footer_offset(4) + magic(4) = 8 bytes */
const V2_TRAILER_SIZE = 8;

/** Minimum valid V2 file size */
const V2_MIN_SIZE = V2_FOOTER_SIZE + V2_TRAILER_SIZE;

/**
 * Check if data is in V2 format by looking for magic bytes at the end.
 *
 * V2 format has the magic bytes in the trailer at the end of the file,
 * whereas V1 has magic bytes only at the beginning.
 *
 * @param data - The encoded GraphCol data
 * @returns true if the data appears to be V2 format
 */
function isV2Format(data: Uint8Array): boolean {
  if (data.length < V2_MIN_SIZE) {
    return false;
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const trailerOffset = data.length - V2_TRAILER_SIZE;
  const trailingMagic = view.getUint32(trailerOffset + 4, true);

  return trailingMagic === GCOL_MAGIC;
}

/**
 * Decode V2 format data section (internal helper to avoid recursion).
 *
 * This extracts the V1-encoded data section from a V2 file and decodes it
 * using the V1 decoder directly, avoiding potential infinite recursion.
 *
 * @param data - Full V2 encoded data
 * @param options - Optional decoding options
 * @returns Decoded triples
 */
function decodeV2DataSection(data: Uint8Array, options?: DecodeGraphColOptions): Triple[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  // Read trailer to locate footer
  const trailerOffset = data.length - V2_TRAILER_SIZE;
  const footerOffsetFromEnd = view.getUint32(trailerOffset, true);
  const footerStart = data.length - footerOffsetFromEnd;

  // Validate footer start position
  if (footerStart < 0 || footerStart + V2_FOOTER_SIZE > data.length - V2_TRAILER_SIZE) {
    throw new Error(`Invalid V2 footer offset: footerStart=${footerStart}, fileSize=${data.length}`);
  }

  // Read data length from footer (offset 4 within footer)
  const dataLength = view.getUint32(footerStart + 4, true);

  // Read entity count from footer (offset 16 within footer)
  const entityCount = view.getUint32(footerStart + 16, true);

  if (entityCount === 0) {
    return [];
  }

  // Validate data length
  if (dataLength > footerStart) {
    throw new Error(`Invalid data length: ${dataLength} exceeds footer start ${footerStart}`);
  }

  // Extract and decode the V1 data section
  const dataSection = data.subarray(0, dataLength);
  return decodeGraphColV1(dataSection, options);
}

/**
 * Decode V1 GraphCol format (internal, non-recursive)
 */
function decodeGraphColV1(data: Uint8Array, options?: DecodeGraphColOptions): Triple[] {
  if (data.length < 32) {
    throw new Error('Invalid GraphCol chunk: too small');
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // Verify magic
  const magic = view.getUint32(offset, true);
  if (magic !== GCOL_MAGIC) {
    throw new Error(`Invalid GraphCol magic: expected ${GCOL_MAGIC}, got ${magic}`);
  }
  offset += 4;

  // Verify version
  const version = view.getUint16(offset, true);
  if (version !== GCOL_VERSION) {
    throw new Error(`Unsupported GraphCol version: ${version}`);
  }
  offset += 2;

  // Validate checksum (stored in last 4 bytes)
  if (data.length < 36) {
    throw new Error('Invalid GraphCol chunk: missing checksum');
  }
  const storedChecksum = view.getUint32(data.length - 4, true);
  const calculatedChecksum = calculateChecksum(data.subarray(0, data.length - 4));
  if (storedChecksum !== calculatedChecksum) {
    throw new Error(
      `GraphCol checksum mismatch: stored=${storedChecksum.toString(16)}, calculated=${calculatedChecksum.toString(16)}`
    );
  }

  const tripleCount = view.getUint32(offset, true);
  offset += 4;

  if (tripleCount === 0) {
    return [];
  }

  // Skip flags
  offset += 2;

  // Skip minTs, maxTs
  offset += 16;

  // Read namespace
  const namespaceLen = view.getUint16(offset, true);
  offset += 2;
  // Skip namespace data (namespace is stored in header for metadata)
  offset += namespaceLen;

  // Skip predicate names in header (we use the dictionary-encoded predicate column)
  const predicateCount = view.getUint16(offset, true);
  offset += 2;
  for (let i = 0; i < predicateCount; i++) {
    const predLen = view.getUint16(offset, true);
    offset += 2;
    offset += predLen;
  }

  // Read column directory
  const columnCount = view.getUint16(offset, true);
  offset += 2;
  const columnOffsets: { offset: number; length: number }[] = [];
  for (let i = 0; i < columnCount; i++) {
    columnOffsets.push({
      offset: view.getUint32(offset, true),
      length: view.getUint32(offset + 4, true),
    });
    offset += 8;
  }

  // Read columns
  let colIdx = 0;

  // Helper function to get column slice safely
  const getColumn = (idx: number): Uint8Array => {
    const colOffset = columnOffsets[idx]!;
    return data.subarray(colOffset.offset, colOffset.offset + colOffset.length);
  };

  // 1. Subjects (dictionary encoded)
  const subjectCol = getColumn(colIdx);
  const { dictionary: subjectDict, indices: subjectIndices } = deserializeDictionary(subjectCol, tripleCount);
  colIdx++;

  // 2. Predicates (dictionary encoded)
  const predicateCol = getColumn(colIdx);
  const { dictionary: predDict, indices: predicateIndices } = deserializeDictionary(predicateCol, tripleCount);
  colIdx++;

  // 3. Object types
  const objTypesCol = getColumn(colIdx);
  const objTypes = deserializeRLE(objTypesCol);
  colIdx++;

  // 4. Object value indices
  const objIndicesCol = getColumn(colIdx);
  const objIndicesView = new DataView(objIndicesCol.buffer, objIndicesCol.byteOffset, objIndicesCol.byteLength);
  const objIndicesCount = objIndicesView.getUint32(0, true);
  const objValueIndices: number[] = [];
  let indOffset = 4;
  for (let i = 0; i < objIndicesCount; i++) {
    const result = decodeVarint(objIndicesCol, indOffset);
    objValueIndices.push(result.value - 1);
    indOffset = result.newOffset;
  }
  colIdx++;

  // 5. Timestamps
  const timestampsCol = getColumn(colIdx);
  const timestamps = decodeTimestamps(timestampsCol);
  colIdx++;

  // 6. TxIds
  const txIdCol = getColumn(colIdx);
  const { dictionary: txIdDict, indices: txIdIndices } = deserializeDictionary(txIdCol, tripleCount);
  colIdx++;

  // 7+ Object values by type
  const objectValuesByType = new Map<number, unknown[]>();

  while (colIdx < columnCount) {
    const col = getColumn(colIdx);
    const type = col[0]!;
    const colData = col.subarray(1);

    switch (type) {
      case ObjectType.STRING: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.STRING));
        objectValuesByType.set(ObjectType.STRING, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.INT64:
      case ObjectType.INT32: {
        const values = decodeInt64Array(colData);
        objectValuesByType.set(type, values);
        break;
      }
      case ObjectType.FLOAT64: {
        const values = decodeFloat64Array(colData);
        objectValuesByType.set(ObjectType.FLOAT64, values);
        break;
      }
      case ObjectType.BOOL: {
        const values = decodeBoolArray(colData);
        objectValuesByType.set(ObjectType.BOOL, values);
        break;
      }
      case ObjectType.TIMESTAMP: {
        const values = decodeTimestamps(colData);
        objectValuesByType.set(ObjectType.TIMESTAMP, values);
        break;
      }
      case ObjectType.REF: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.REF));
        objectValuesByType.set(ObjectType.REF, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.GEO_POINT: {
        const points = decodeGeoPoints(colData);
        objectValuesByType.set(ObjectType.GEO_POINT, points);
        break;
      }
      case ObjectType.BINARY: {
        const values = decodeBinaryArray(colData);
        objectValuesByType.set(ObjectType.BINARY, values);
        break;
      }
      case ObjectType.JSON: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.JSON));
        objectValuesByType.set(ObjectType.JSON, indices.map(i => JSON.parse(dictionary[i]!)));
        break;
      }
      case ObjectType.DATE: {
        const values = decodeInt32Array(colData);
        objectValuesByType.set(ObjectType.DATE, values);
        break;
      }
      case ObjectType.DURATION: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.DURATION));
        objectValuesByType.set(ObjectType.DURATION, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.URL: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.URL));
        objectValuesByType.set(ObjectType.URL, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.REF_ARRAY: {
        const arrays = decodeRefArrays(colData);
        objectValuesByType.set(ObjectType.REF_ARRAY, arrays);
        break;
      }
    }

    colIdx++;
  }

  // Build column filter set if columns option is provided
  const columnFilter = options?.columns && options.columns.length > 0
    ? new Set(options.columns)
    : null;

  // Reconstruct triples with optional column pruning
  const triples: Triple[] = [];
  const typeValueIndices = new Map<number, number>();

  for (let i = 0; i < tripleCount; i++) {
    const objType = objTypes[i]!;
    const objValueIdx = objValueIndices[i]!;
    const predicateName = predDict[predicateIndices[i]!]!;

    // Always increment type value index to maintain correct offsets
    if (objValueIdx !== -1) {
      const currentIdx = typeValueIndices.get(objType) ?? 0;
      typeValueIndices.set(objType, currentIdx + 1);
    }

    // Skip this triple if column pruning is active and predicate is not requested
    if (columnFilter && !columnFilter.has(predicateName)) {
      continue;
    }

    let object: TypedObject;
    if (objValueIdx === -1) {
      object = { type: ObjectType.NULL };
    } else {
      const typeValues = objectValuesByType.get(objType);
      const localIdx = (typeValueIndices.get(objType) ?? 1) - 1;

      switch (objType) {
        case ObjectType.BOOL:
          object = { type: ObjectType.BOOL, value: typeValues?.[localIdx] as boolean };
          break;
        case ObjectType.INT32:
          object = { type: ObjectType.INT32, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.INT64:
          object = { type: ObjectType.INT64, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.FLOAT64:
          object = { type: ObjectType.FLOAT64, value: typeValues?.[localIdx] as number };
          break;
        case ObjectType.STRING:
          object = { type: ObjectType.STRING, value: typeValues?.[localIdx] as string };
          break;
        case ObjectType.BINARY:
          object = { type: ObjectType.BINARY, value: typeValues?.[localIdx] as Uint8Array };
          break;
        case ObjectType.TIMESTAMP:
          object = { type: ObjectType.TIMESTAMP, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.DATE:
          object = { type: ObjectType.DATE, value: typeValues?.[localIdx] as number };
          break;
        case ObjectType.DURATION:
          object = { type: ObjectType.DURATION, value: typeValues?.[localIdx] as string };
          break;
        case ObjectType.REF:
          object = { type: ObjectType.REF, value: createEntityId(typeValues?.[localIdx] as string) };
          break;
        case ObjectType.REF_ARRAY:
          object = { type: ObjectType.REF_ARRAY, value: (typeValues?.[localIdx] as string[]).map(s => createEntityId(s)) };
          break;
        case ObjectType.JSON:
          object = { type: ObjectType.JSON, value: typeValues?.[localIdx] };
          break;
        case ObjectType.GEO_POINT:
          object = { type: ObjectType.GEO_POINT, value: typeValues?.[localIdx] as GeoPoint };
          break;
        case ObjectType.URL:
          object = { type: ObjectType.URL, value: typeValues?.[localIdx] as string };
          break;
        default:
          object = { type: ObjectType.NULL };
      }
    }

    triples.push({
      subject: createEntityId(subjectDict[subjectIndices[i]!]!),
      predicate: createPredicate(predicateName),
      object,
      timestamp: timestamps[i]!,
      txId: createTransactionId(txIdDict[txIdIndices[i]!]!),
    });
  }

  return triples;
}

/**
 * Decode GraphCol to triples
 * Supports both V1 (header-based) and V2 (footer-based) formats.
 * Auto-detects the format based on trailing magic bytes.
 *
 * @param data - The encoded GraphCol data
 * @param options - Optional decoding options for column pruning
 */
export function decodeGraphCol(data: Uint8Array, options?: DecodeGraphColOptions): Triple[] {
  if (data.length < 32) {
    throw new Error('Invalid GraphCol chunk: too small');
  }

  // Check for V2 format (trailing magic)
  // V2 has magic at the end, V1 has magic at the beginning
  if (isV2Format(data)) {
    return decodeV2DataSection(data, options);
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // Verify magic (V1 has magic at the beginning)
  const magic = view.getUint32(offset, true);
  if (magic !== GCOL_MAGIC) {
    throw new Error(`Invalid GraphCol magic: expected ${GCOL_MAGIC}, got ${magic}`);
  }
  offset += 4;

  // Verify version
  const version = view.getUint16(offset, true);
  if (version !== GCOL_VERSION) {
    throw new Error(`Unsupported GraphCol version: ${version}`);
  }
  offset += 2;

  // Validate checksum (stored in last 4 bytes)
  // Minimum valid chunk: header (at least 32 bytes) + checksum (4 bytes)
  if (data.length < 36) {
    throw new Error('Invalid GraphCol chunk: missing checksum');
  }
  const storedChecksum = view.getUint32(data.length - 4, true);
  const calculatedChecksum = calculateChecksum(data.subarray(0, data.length - 4));
  if (storedChecksum !== calculatedChecksum) {
    throw new Error(
      `GraphCol checksum mismatch: stored=${storedChecksum.toString(16)}, calculated=${calculatedChecksum.toString(16)}`
    );
  }

  const tripleCount = view.getUint32(offset, true);
  offset += 4;

  if (tripleCount === 0) {
    return [];
  }

  // Skip flags
  offset += 2;

  // Skip minTs, maxTs
  offset += 16;

  // Read namespace
  const namespaceLen = view.getUint16(offset, true);
  offset += 2;
  // Skip namespace data (namespace is stored in header for metadata)
  offset += namespaceLen;

  // Skip predicate names in header (we use the dictionary-encoded predicate column)
  const predicateCount = view.getUint16(offset, true);
  offset += 2;
  for (let i = 0; i < predicateCount; i++) {
    const predLen = view.getUint16(offset, true);
    offset += 2;
    offset += predLen;
  }

  // Read column directory
  const columnCount = view.getUint16(offset, true);
  offset += 2;
  const columnOffsets: { offset: number; length: number }[] = [];
  for (let i = 0; i < columnCount; i++) {
    columnOffsets.push({
      offset: view.getUint32(offset, true),
      length: view.getUint32(offset + 4, true),
    });
    offset += 8;
  }

  // Read columns
  let colIdx = 0;

  // Helper function to get column slice safely
  const getColumnSlice = (idx: number): Uint8Array => {
    const colOffset = columnOffsets[idx]!;
    return data.subarray(colOffset.offset, colOffset.offset + colOffset.length);
  };

  // 1. Subjects (dictionary encoded)
  const subjectCol = getColumnSlice(colIdx);
  const { dictionary: subjectDict, indices: subjectIndices } = deserializeDictionary(subjectCol, tripleCount);
  colIdx++;

  // 2. Predicates (dictionary encoded)
  const predicateCol = getColumnSlice(colIdx);
  const { dictionary: predDict, indices: predicateIndices } = deserializeDictionary(predicateCol, tripleCount);
  colIdx++;

  // 3. Object types
  const objTypesCol = getColumnSlice(colIdx);
  const objTypes = deserializeRLE(objTypesCol);
  colIdx++;

  // 4. Object value indices
  const objIndicesCol = getColumnSlice(colIdx);
  const objIndicesView = new DataView(objIndicesCol.buffer, objIndicesCol.byteOffset, objIndicesCol.byteLength);
  const objIndicesCount = objIndicesView.getUint32(0, true);
  const objValueIndices: number[] = [];
  let indOffset = 4;
  for (let i = 0; i < objIndicesCount; i++) {
    const result = decodeVarint(objIndicesCol, indOffset);
    objValueIndices.push(result.value - 1); // Decode: value - 1 (0 means -1)
    indOffset = result.newOffset;
  }
  colIdx++;

  // 5. Timestamps
  const timestampsCol = getColumnSlice(colIdx);
  const timestamps = decodeTimestamps(timestampsCol);
  colIdx++;

  // 6. TxIds
  const txIdCol = getColumnSlice(colIdx);
  const { dictionary: txIdDict, indices: txIdIndices } = deserializeDictionary(txIdCol, tripleCount);
  colIdx++;

  // 7+ Object values by type
  const objectValuesByType = new Map<number, unknown[]>();

  while (colIdx < columnCount) {
    const col = getColumnSlice(colIdx);
    const type = col[0]!;
    const colData = col.subarray(1);

    switch (type) {
      case ObjectType.STRING: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.STRING));
        objectValuesByType.set(ObjectType.STRING, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.INT64:
      case ObjectType.INT32: {
        const values = decodeInt64Array(colData);
        objectValuesByType.set(type, values);
        break;
      }
      case ObjectType.FLOAT64: {
        const values = decodeFloat64Array(colData);
        objectValuesByType.set(ObjectType.FLOAT64, values);
        break;
      }
      case ObjectType.BOOL: {
        const values = decodeBoolArray(colData);
        objectValuesByType.set(ObjectType.BOOL, values);
        break;
      }
      case ObjectType.TIMESTAMP: {
        const values = decodeTimestamps(colData);
        objectValuesByType.set(ObjectType.TIMESTAMP, values);
        break;
      }
      case ObjectType.REF: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.REF));
        objectValuesByType.set(ObjectType.REF, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.GEO_POINT: {
        const points = decodeGeoPoints(colData);
        objectValuesByType.set(ObjectType.GEO_POINT, points);
        break;
      }
      case ObjectType.BINARY: {
        const values = decodeBinaryArray(colData);
        objectValuesByType.set(ObjectType.BINARY, values);
        break;
      }
      case ObjectType.JSON: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.JSON));
        objectValuesByType.set(ObjectType.JSON, indices.map(i => JSON.parse(dictionary[i]!)));
        break;
      }
      case ObjectType.DATE: {
        const values = decodeInt32Array(colData);
        objectValuesByType.set(ObjectType.DATE, values);
        break;
      }
      case ObjectType.DURATION: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.DURATION));
        objectValuesByType.set(ObjectType.DURATION, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.URL: {
        const { dictionary, indices } = deserializeDictionary(colData, countObjType(objTypes, ObjectType.URL));
        objectValuesByType.set(ObjectType.URL, indices.map(i => dictionary[i]!));
        break;
      }
      case ObjectType.REF_ARRAY: {
        const arrays = decodeRefArrays(colData);
        objectValuesByType.set(ObjectType.REF_ARRAY, arrays);
        break;
      }
    }

    colIdx++;
  }

  // Build column filter set if columns option is provided
  const columnFilter = options?.columns && options.columns.length > 0
    ? new Set(options.columns)
    : null;

  // Reconstruct triples with optional column pruning
  const triples: Triple[] = [];
  const typeValueIndices = new Map<number, number>();

  for (let i = 0; i < tripleCount; i++) {
    const objType = objTypes[i]!;
    const objValueIdx = objValueIndices[i]!;
    const predicateName = predDict[predicateIndices[i]!]!;

    // Always increment type value index to maintain correct offsets
    // This is critical: even if we skip a triple, we must advance the type index
    if (objValueIdx !== -1) {
      const currentIdx = typeValueIndices.get(objType) ?? 0;
      typeValueIndices.set(objType, currentIdx + 1);
    }

    // Skip this triple if column pruning is active and predicate is not requested
    if (columnFilter && !columnFilter.has(predicateName)) {
      continue;
    }

    let object: TypedObject;
    if (objValueIdx === -1) {
      object = { type: ObjectType.NULL };
    } else {
      const typeValues = objectValuesByType.get(objType);
      // We already incremented the index above, so we need to get the value at (current - 1)
      const localIdx = (typeValueIndices.get(objType) ?? 1) - 1;

      switch (objType) {
        case ObjectType.BOOL:
          object = { type: ObjectType.BOOL, value: typeValues?.[localIdx] as boolean };
          break;
        case ObjectType.INT32:
          object = { type: ObjectType.INT32, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.INT64:
          object = { type: ObjectType.INT64, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.FLOAT64:
          object = { type: ObjectType.FLOAT64, value: typeValues?.[localIdx] as number };
          break;
        case ObjectType.STRING:
          object = { type: ObjectType.STRING, value: typeValues?.[localIdx] as string };
          break;
        case ObjectType.BINARY:
          object = { type: ObjectType.BINARY, value: typeValues?.[localIdx] as Uint8Array };
          break;
        case ObjectType.TIMESTAMP:
          object = { type: ObjectType.TIMESTAMP, value: typeValues?.[localIdx] as bigint };
          break;
        case ObjectType.DATE:
          object = { type: ObjectType.DATE, value: typeValues?.[localIdx] as number };
          break;
        case ObjectType.DURATION:
          object = { type: ObjectType.DURATION, value: typeValues?.[localIdx] as string };
          break;
        case ObjectType.REF:
          object = { type: ObjectType.REF, value: createEntityId(typeValues?.[localIdx] as string) };
          break;
        case ObjectType.REF_ARRAY:
          object = { type: ObjectType.REF_ARRAY, value: (typeValues?.[localIdx] as string[]).map(s => createEntityId(s)) };
          break;
        case ObjectType.JSON:
          object = { type: ObjectType.JSON, value: typeValues?.[localIdx] };
          break;
        case ObjectType.GEO_POINT:
          object = { type: ObjectType.GEO_POINT, value: typeValues?.[localIdx] as GeoPoint };
          break;
        case ObjectType.URL:
          object = { type: ObjectType.URL, value: typeValues?.[localIdx] as string };
          break;
        default:
          object = { type: ObjectType.NULL };
      }
    }

    triples.push({
      subject: createEntityId(subjectDict[subjectIndices[i]!]!),
      predicate: createPredicate(predicateName),
      object,
      timestamp: timestamps[i]!,
      txId: createTransactionId(txIdDict[txIdIndices[i]!]!),
    });
  }

  return triples;
}

function countObjType(types: Uint8Array, targetType: number): number {
  let count = 0;
  for (let i = 0; i < types.length; i++) {
    if (types[i]! === targetType) count++;
  }
  return count;
}

// ============================================================================
// STATS
// ============================================================================

/**
 * Get chunk statistics without full decode
 */
export function getChunkStats(data: Uint8Array): ChunkStats {
  if (data.length < 32) {
    throw new Error('Invalid GraphCol chunk: too small');
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  let offset = 0;

  // Verify magic
  const magic = view.getUint32(offset, true);
  if (magic !== GCOL_MAGIC) {
    throw new Error('Invalid GraphCol magic');
  }
  offset += 4;

  // Skip version
  offset += 2;

  const tripleCount = view.getUint32(offset, true);
  offset += 4;

  // Skip flags
  offset += 2;

  const minTs = view.getBigInt64(offset, true);
  offset += 8;
  const maxTs = view.getBigInt64(offset, true);
  offset += 8;

  // Read namespace length and skip
  const namespaceLen = view.getUint16(offset, true);
  offset += 2;
  offset += namespaceLen;

  // Read predicate names
  const predicateCount = view.getUint16(offset, true);
  offset += 2;
  const predicates: string[] = [];
  const decoder = new TextDecoder();
  for (let i = 0; i < predicateCount; i++) {
    const predLen = view.getUint16(offset, true);
    offset += 2;
    predicates.push(decoder.decode(data.subarray(offset, offset + predLen)));
    offset += predLen;
  }

  return {
    tripleCount,
    predicates,
    timeRange: [minTs, maxTs],
    sizeBytes: data.length,
  };
}

// ============================================================================
// STREAMING ENCODER
// ============================================================================

/**
 * Create a streaming encoder for CDC
 */
export function createEncoder(namespace: Namespace): GraphColEncoder {
  let triples: Triple[] = [];

  return {
    addTriple(triple: Triple): void {
      triples.push(triple);
    },

    flush(): Uint8Array {
      const encoded = encodeGraphCol(triples, namespace);
      triples = [];
      return encoded;
    },

    reset(): void {
      triples = [];
    },
  };
}

// ============================================================================
// GRAPHCOL V2 - FOOTER-BASED LAYOUT
// ============================================================================

import {
  type EntityIndex,
  type EntityIndexEntry,
  encodeEntityIndex,
  decodeEntityIndex,
  lookupEntity,
} from './entity-index';

/** GraphCol V2 version number */
export const GCOL_VERSION_2 = 2;

/** Fixed footer size in bytes (excluding 8-byte trailer) */
export const GCOL_FOOTER_SIZE = 48;

/** Total trailer size (footer_offset + magic) */
const GCOL_TRAILER_SIZE = 8;

/**
 * GraphCol V2 Footer structure
 *
 * Footer layout (48 bytes total):
 * - 4 bytes: version (0x02)          [offset 0]
 * - 4 bytes: data_length             [offset 4]
 * - 4 bytes: index_offset            [offset 8]
 * - 4 bytes: index_length            [offset 12]
 * - 4 bytes: entity_count            [offset 16]
 * - 8 bytes: min_timestamp           [offset 20]
 * - 8 bytes: max_timestamp           [offset 28]
 * - 4 bytes: CRC32 of bytes 0-35     [offset 36]
 * - 8 bytes: reserved/padding        [offset 40]
 *
 * Trailer (8 bytes, immediately after footer):
 * - 4 bytes: footer_offset from end (always 56 for standard layout)
 * - 4 bytes: magic GCOL (0x4C4F4347)
 */
export interface GraphColFooter {
  /** Format version (should be 2) */
  version: number;
  /** Length of the data section in bytes */
  dataLength: number;
  /** Byte offset where entity index starts */
  indexOffset: number;
  /** Length of entity index in bytes */
  indexLength: number;
  /** Number of unique entities in the file */
  entityCount: number;
  /** Minimum timestamp in the data */
  minTimestamp: bigint;
  /** Maximum timestamp in the data */
  maxTimestamp: bigint;
  /** CRC32 checksum of footer fields */
  checksum: number;
}

/**
 * Encode triples to GraphCol V2 format with footer-based layout.
 *
 * V2 adds an entity index for O(log n) entity lookup, sorted data for
 * efficient range access, and a footer with metadata for Range request
 * optimization.
 *
 * File Layout:
 * ```
 * +-------------------+
 * | Data Section      |  <- V1-encoded triples (sorted by entity)
 * +-------------------+
 * | Entity Index      |  <- Binary searchable index
 * +-------------------+
 * | Footer (48 bytes) |  <- Version, offsets, timestamps, CRC32
 * +-------------------+
 * | Trailer (8 bytes) |  <- footer_offset + magic
 * +-------------------+
 * ```
 *
 * @param triples - Array of triples to encode
 * @param namespace - Namespace for this chunk
 * @returns Encoded V2 GraphCol data
 */
export function encodeGraphColV2(triples: Triple[], namespace: Namespace): Uint8Array {
  if (triples.length === 0) {
    return createEmptyChunkV2(namespace);
  }

  // Validate input size to prevent OOM
  // Estimate ~200 bytes per triple for the encoded format
  validateEncodeCount(triples.length, 'triples (V2)', 200);

  // Sort triples by subject (entity ID) for efficient range access
  // This enables binary search in the entity index
  const sortedTriples = [...triples].sort((a, b) =>
    a.subject.localeCompare(b.subject)
  );

  // Calculate min/max timestamps for footer metadata
  let minTimestamp = sortedTriples[0]!.timestamp;
  let maxTimestamp = sortedTriples[0]!.timestamp;
  for (const triple of sortedTriples) {
    if (triple.timestamp < minTimestamp) minTimestamp = triple.timestamp;
    if (triple.timestamp > maxTimestamp) maxTimestamp = triple.timestamp;
  }

  // Encode data section using V1 encoder (includes V1 header for internal decoding)
  const dataSection = encodeGraphColDataOnly(sortedTriples, namespace);

  // Build entity index for O(log n) lookup
  const entityIndex = buildEntityIndex(sortedTriples, dataSection);
  const encodedIndex = encodeEntityIndex(entityIndex);

  // Calculate offsets
  const dataLength = dataSection.length;
  const indexOffset = dataLength;
  const indexLength = encodedIndex.length;
  const entityCount = entityIndex.entries.length;

  // Build footer (44 bytes of data + 4 bytes CRC)
  const footerBuffer = new Uint8Array(GCOL_FOOTER_SIZE);
  const footerView = new DataView(footerBuffer.buffer);
  let offset = 0;

  // Version
  footerView.setUint32(offset, GCOL_VERSION_2, true);
  offset += 4;

  // Data length
  footerView.setUint32(offset, dataLength, true);
  offset += 4;

  // Index offset
  footerView.setUint32(offset, indexOffset, true);
  offset += 4;

  // Index length
  footerView.setUint32(offset, indexLength, true);
  offset += 4;

  // Entity count
  footerView.setUint32(offset, entityCount, true);
  offset += 4;

  // Min timestamp
  footerView.setBigInt64(offset, minTimestamp, true);
  offset += 8;

  // Max timestamp
  footerView.setBigInt64(offset, maxTimestamp, true);
  offset += 8;

  // Calculate CRC32 of footer fields (36 bytes: version through max_timestamp)
  const footerDataForChecksum = footerBuffer.subarray(0, 36);
  const footerChecksum = calculateChecksum(footerDataForChecksum);
  footerView.setUint32(offset, footerChecksum, true);
  offset += 4;

  // Reserved/padding (8 bytes to make total 48 bytes)
  footerView.setUint32(offset, 0, true);
  offset += 4;
  footerView.setUint32(offset, 0, true);
  offset += 4;

  // Build trailer
  const trailerBuffer = new Uint8Array(GCOL_TRAILER_SIZE);
  const trailerView = new DataView(trailerBuffer.buffer);

  // Footer offset from end (how far back to find the footer)
  const footerOffsetFromEnd = GCOL_FOOTER_SIZE + GCOL_TRAILER_SIZE;
  trailerView.setUint32(0, footerOffsetFromEnd, true);

  // Magic bytes
  trailerView.setUint32(4, GCOL_MAGIC, true);

  // Combine all sections
  const totalSize = dataLength + indexLength + GCOL_FOOTER_SIZE + GCOL_TRAILER_SIZE;
  const result = new Uint8Array(totalSize);

  let writeOffset = 0;
  result.set(dataSection, writeOffset);
  writeOffset += dataLength;

  result.set(encodedIndex, writeOffset);
  writeOffset += indexLength;

  result.set(footerBuffer, writeOffset);
  writeOffset += GCOL_FOOTER_SIZE;

  result.set(trailerBuffer, writeOffset);

  return result;
}

/**
 * Create empty V2 chunk
 */
function createEmptyChunkV2(namespace: Namespace): Uint8Array {
  // For empty chunk, we still need valid structure
  const emptyDataSection = createEmptyDataSection(namespace);
  const emptyIndex = encodeEntityIndex({ entries: [], version: 1 });

  const dataLength = emptyDataSection.length;
  const indexOffset = dataLength;
  const indexLength = emptyIndex.length;

  // Build footer
  const footerBuffer = new Uint8Array(GCOL_FOOTER_SIZE);
  const footerView = new DataView(footerBuffer.buffer);
  let offset = 0;

  footerView.setUint32(offset, GCOL_VERSION_2, true);
  offset += 4;
  footerView.setUint32(offset, dataLength, true);
  offset += 4;
  footerView.setUint32(offset, indexOffset, true);
  offset += 4;
  footerView.setUint32(offset, indexLength, true);
  offset += 4;
  footerView.setUint32(offset, 0, true); // entity count
  offset += 4;
  footerView.setBigInt64(offset, 0n, true); // min timestamp
  offset += 8;
  footerView.setBigInt64(offset, 0n, true); // max timestamp
  offset += 8;

  // CRC32 covers 36 bytes
  const footerChecksum = calculateChecksum(footerBuffer.subarray(0, 36));
  footerView.setUint32(offset, footerChecksum, true);
  offset += 4;
  // Reserved (8 bytes)
  footerView.setUint32(offset, 0, true);
  offset += 4;
  footerView.setUint32(offset, 0, true);
  offset += 4;

  // Build trailer
  const trailerBuffer = new Uint8Array(GCOL_TRAILER_SIZE);
  const trailerView = new DataView(trailerBuffer.buffer);
  trailerView.setUint32(0, GCOL_FOOTER_SIZE + GCOL_TRAILER_SIZE, true);
  trailerView.setUint32(4, GCOL_MAGIC, true);

  // Combine
  const totalSize = dataLength + indexLength + GCOL_FOOTER_SIZE + GCOL_TRAILER_SIZE;
  const result = new Uint8Array(totalSize);

  let writeOffset = 0;
  result.set(emptyDataSection, writeOffset);
  writeOffset += dataLength;
  result.set(emptyIndex, writeOffset);
  writeOffset += indexLength;
  result.set(footerBuffer, writeOffset);
  writeOffset += GCOL_FOOTER_SIZE;
  result.set(trailerBuffer, writeOffset);

  return result;
}

/**
 * Create empty data section for V2
 */
function createEmptyDataSection(namespace: Namespace): Uint8Array {
  const encoder = new TextEncoder();
  const namespaceBytes = encoder.encode(namespace);

  // Minimal data section: just namespace info
  const size = 2 + namespaceBytes.length + 4; // namespace len + namespace + checksum
  const buffer = new Uint8Array(size);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Namespace length
  view.setUint16(offset, namespaceBytes.length, true);
  offset += 2;

  // Namespace
  buffer.set(namespaceBytes, offset);
  offset += namespaceBytes.length;

  // Checksum
  const checksum = calculateChecksum(buffer.subarray(0, offset));
  view.setUint32(offset, checksum, true);

  return buffer;
}

/**
 * Encode triple data without V1 header (for V2 format)
 * This encodes the columnar data that V1's encodeGraphCol creates,
 * but without the V1 header structure.
 */
function encodeGraphColDataOnly(triples: Triple[], namespace: Namespace): Uint8Array {
  // For simplicity, we'll use the full V1 encoding and just mark it as V2 data
  // The decoder will know to look at the footer to determine the version
  return encodeGraphCol(triples, namespace);
}

/**
 * Build entity index from sorted triples.
 *
 * The index stores triple indices rather than exact byte offsets, since the
 * V1 columnar format stores data by column type (not by entity). Exact byte
 * offsets would require columnar format changes or a more complex mapping.
 *
 * The offset/length fields in EntityIndexEntry represent:
 * - offset: Starting triple index in the sorted array
 * - length: Number of triples for this entity
 *
 * This allows O(log n) lookup to verify entity existence, and the caller
 * can decode the full data section and filter by entity ID.
 *
 * @param sortedTriples - Triples sorted by subject (entity ID)
 * @param _dataSection - Encoded data section (unused, kept for API compatibility)
 * @returns EntityIndex with sorted entries
 */
function buildEntityIndex(sortedTriples: Triple[], _dataSection: Uint8Array): EntityIndex {
  if (sortedTriples.length === 0) {
    return { entries: [], version: 1 };
  }

  const entries: EntityIndexEntry[] = [];
  let currentEntity: string | null = null;
  let entityStartIdx = 0;

  for (let i = 0; i <= sortedTriples.length; i++) {
    const entityId = i < sortedTriples.length ? sortedTriples[i]!.subject : null;

    if (entityId !== currentEntity) {
      // Finish previous entity
      if (currentEntity !== null) {
        const tripleCount = i - entityStartIdx;
        entries.push({
          entityId: currentEntity,
          // Store triple indices for now; byte-level seeking would require
          // changes to the columnar format to support entity-based partitioning
          offset: entityStartIdx,
          length: tripleCount,
        });
      }

      // Start new entity
      if (entityId !== null) {
        currentEntity = entityId;
        entityStartIdx = i;
      }
    }
  }

  return { entries, version: 1 };
}

/**
 * Read just the footer from a V2 GraphCol file.
 * Used for Range request planning and metadata extraction.
 *
 * This function validates the file structure and checksums before returning
 * the footer metadata. It throws detailed errors for corrupted or truncated files.
 *
 * @param data - The encoded GraphCol V2 data (or at least the trailer portion)
 * @returns The parsed footer metadata
 * @throws Error if the file is too small, corrupted, or has invalid magic/checksum
 */
export function readFooter(data: Uint8Array): GraphColFooter {
  // Minimum size check: footer (48) + trailer (8) = 56 bytes
  const minSize = GCOL_FOOTER_SIZE + GCOL_TRAILER_SIZE;
  if (data.length < minSize) {
    throw new Error(
      `Invalid GraphCol V2: file too small (got ${data.length} bytes, need at least ${minSize})`
    );
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  // Read and validate trailer (last 8 bytes)
  const trailerOffset = data.length - GCOL_TRAILER_SIZE;
  const magic = view.getUint32(trailerOffset + 4, true);

  if (magic !== GCOL_MAGIC) {
    throw new Error(
      `Invalid GraphCol magic: expected 0x${GCOL_MAGIC.toString(16).toUpperCase()}, ` +
      `got 0x${magic.toString(16).toUpperCase()}`
    );
  }

  // Calculate footer position from trailer
  const footerOffsetFromEnd = view.getUint32(trailerOffset, true);
  const footerStart = data.length - footerOffsetFromEnd;

  // Validate footer position bounds
  if (footerStart < 0) {
    throw new Error(
      `Invalid footer offset: footer_offset_from_end=${footerOffsetFromEnd} exceeds file size ${data.length}`
    );
  }

  // Ensure we can read the complete footer (36 bytes for checksummed data + 4 for checksum + 8 reserved)
  const footerEnd = footerStart + GCOL_FOOTER_SIZE;
  if (footerEnd > trailerOffset) {
    throw new Error(
      `Footer overlaps with trailer: footerEnd=${footerEnd}, trailerStart=${trailerOffset}`
    );
  }

  // Read footer fields
  let offset = footerStart;

  const version = view.getUint32(offset, true);
  offset += 4;

  const dataLength = view.getUint32(offset, true);
  offset += 4;

  const indexOffset = view.getUint32(offset, true);
  offset += 4;

  const indexLength = view.getUint32(offset, true);
  offset += 4;

  const entityCount = view.getUint32(offset, true);
  offset += 4;

  const minTimestamp = view.getBigInt64(offset, true);
  offset += 8;

  const maxTimestamp = view.getBigInt64(offset, true);
  offset += 8;

  const storedChecksum = view.getUint32(offset, true);
  offset += 4;

  // Validate checksum (covers first 36 bytes of footer: version through max_timestamp)
  const footerDataForChecksum = data.subarray(footerStart, footerStart + 36);
  const calculatedChecksum = calculateChecksum(footerDataForChecksum);

  if (storedChecksum !== calculatedChecksum) {
    throw new Error(
      `Footer checksum mismatch: stored=0x${storedChecksum.toString(16).toUpperCase()}, ` +
      `calculated=0x${calculatedChecksum.toString(16).toUpperCase()}`
    );
  }

  // Validate internal consistency
  if (dataLength > footerStart) {
    throw new Error(
      `Invalid data length: ${dataLength} exceeds footer start position ${footerStart}`
    );
  }

  if (indexOffset < dataLength) {
    throw new Error(
      `Invalid index offset: ${indexOffset} is less than data length ${dataLength}`
    );
  }

  if (indexOffset + indexLength > footerStart) {
    throw new Error(
      `Index extends past footer: indexEnd=${indexOffset + indexLength}, footerStart=${footerStart}`
    );
  }

  return {
    version,
    dataLength,
    indexOffset,
    indexLength,
    entityCount,
    minTimestamp,
    maxTimestamp,
    checksum: storedChecksum,
  };
}

/**
 * Read entity index from a V2 GraphCol file.
 *
 * Extracts the entity index section based on offsets from the footer.
 * Returns an empty index if no entities are present.
 *
 * @param data - The encoded GraphCol V2 data
 * @returns The decoded entity index
 * @throws Error if the index data is corrupted or out of bounds
 */
export function readEntityIndex(data: Uint8Array): EntityIndex {
  const footer = readFooter(data);

  if (footer.indexLength === 0 || footer.entityCount === 0) {
    return { entries: [], version: 1 };
  }

  // Validate index bounds
  const indexEnd = footer.indexOffset + footer.indexLength;
  if (indexEnd > data.length) {
    throw new Error(
      `Index data out of bounds: indexEnd=${indexEnd} exceeds file size ${data.length}`
    );
  }

  const indexData = data.subarray(footer.indexOffset, indexEnd);
  return decodeEntityIndex(indexData);
}

/**
 * Decode V2 GraphCol file to triples.
 *
 * This is the public V2 decoder. It validates the V2 format and extracts
 * the embedded V1 data section for decoding. If passed a V1 file, it
 * falls back to the V1 decoder.
 *
 * @param data - The encoded GraphCol data (V1 or V2)
 * @param options - Optional decoding options for column pruning
 * @returns Array of decoded triples
 */
export function decodeGraphColV2(data: Uint8Array, options?: DecodeGraphColOptions): Triple[] {
  // Check if this is actually a V2 file
  if (!isV2Format(data)) {
    // Fall back to V1 decoder for backward compatibility
    return decodeGraphColV1(data, options);
  }

  const footer = readFooter(data);

  if (footer.entityCount === 0) {
    return [];
  }

  // Extract and decode the V1 data section directly (avoid recursion)
  const dataSection = data.subarray(0, footer.dataLength);
  return decodeGraphColV1(dataSection, options);
}

/**
 * Partial decode: read only a specific entity's triples.
 * Returns null if entity not found.
 *
 * For V2 files, this uses the entity index for O(log n) lookup to determine
 * if the entity exists. Currently decodes the full data section and filters,
 * but future optimization could decode only the entity's byte range.
 *
 * For V1 files, falls back to full decode + filter.
 *
 * @param data - The encoded GraphCol data (V1 or V2)
 * @param entityId - The entity ID to look up
 * @returns Array of triples for the entity, or null if not found
 */
export function decodeEntity(data: Uint8Array, entityId: string): Triple[] | null {
  // Check if V2 format
  if (!isV2Format(data)) {
    // For V1, fall back to full decode + filter
    const allTriples = decodeGraphColV1(data);
    const filtered = allTriples.filter(t => t.subject === entityId);
    return filtered.length > 0 ? filtered : null;
  }

  const footer = readFooter(data);

  if (footer.entityCount === 0) {
    return null;
  }

  // Read entity index and do binary search for O(log n) existence check
  const index = readEntityIndex(data);
  const entry = lookupEntity(index, entityId);

  if (!entry) {
    return null;
  }

  // Decode the data section and filter for the specific entity
  // TODO: Optimize to decode only the entity's byte range using entry.offset/length
  const dataSection = data.subarray(0, footer.dataLength);
  const allTriples = decodeGraphColV1(dataSection);

  return allTriples.filter(t => t.subject === entityId);
}
