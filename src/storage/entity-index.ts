/**
 * Entity Index with Binary Search for O(log n) Lookup
 *
 * Index format for GraphCol v2 with Lance/Vortex-style footer.
 * Provides efficient entity lookup and prefix-based range queries.
 *
 * Binary Format:
 * [4 bytes: entry count (little-endian)]
 * [entries: sorted by entity_id]
 *   - [varint: entity_id length]
 *   - [N bytes: entity_id (UTF-8)]
 *   - [varint: byte offset in data file]
 *   - [varint: byte length]
 * [4 bytes: CRC32 checksum]
 *
 * @packageDocumentation
 */

import {
  encodeVarint,
  decodeVarint,
  varintSize,
  crc32,
} from '../core/encoding';

// ============================================================================
// TYPES
// ============================================================================

/**
 * Single entry in the entity index
 */
export interface EntityIndexEntry {
  /** Entity ID string */
  entityId: string;
  /** Byte offset in data file */
  offset: number;
  /** Byte length of entity data */
  length: number;
}

/**
 * Entity index structure
 */
export interface EntityIndex {
  /** Sorted entries (by entityId) */
  entries: EntityIndexEntry[];
  /** Index format version */
  version: number;
}

// ============================================================================
// ENCODING
// ============================================================================

/** Maximum entry count (prevents memory exhaustion) */
const MAX_ENTRY_COUNT = 10_000_000;

/**
 * Encode entity index to binary format
 *
 * Format:
 * [4 bytes: entry count (little-endian)]
 * [entries: sorted by entity_id]
 *   - [varint: entity_id length]
 *   - [N bytes: entity_id (UTF-8)]
 *   - [varint: byte offset in data file]
 *   - [varint: byte length]
 * [4 bytes: CRC32 checksum]
 *
 * @param index - The entity index to encode
 * @returns Encoded binary data
 * @throws Error if entry values are invalid
 */
export function encodeEntityIndex(index: EntityIndex): Uint8Array {
  // Validate entry count
  if (index.entries.length > MAX_ENTRY_COUNT) {
    throw new Error(`Entry count ${index.entries.length} exceeds maximum ${MAX_ENTRY_COUNT}`);
  }

  const encoder = new TextEncoder();

  // Pre-encode all entity IDs to calculate total size
  const encodedIds: Uint8Array[] = new Array(index.entries.length);
  let totalSize = 4; // entry count

  for (let i = 0; i < index.entries.length; i++) {
    const entry = index.entries[i]!;

    // Validate entry values
    if (entry.offset < 0) {
      throw new Error(`Entry "${entry.entityId}" has negative offset: ${entry.offset}`);
    }
    if (entry.length < 0) {
      throw new Error(`Entry "${entry.entityId}" has negative length: ${entry.length}`);
    }

    const encodedId = encoder.encode(entry.entityId);
    encodedIds[i] = encodedId;

    totalSize += varintSize(encodedId.length);
    totalSize += encodedId.length;
    totalSize += varintSize(entry.offset);
    totalSize += varintSize(entry.length);
  }

  totalSize += 4; // CRC32 checksum

  // Allocate buffer
  const buffer = new Uint8Array(totalSize);
  const view = new DataView(buffer.buffer);
  let offset = 0;

  // Write entry count
  view.setUint32(offset, index.entries.length, true);
  offset += 4;

  // Write entries
  for (let i = 0; i < index.entries.length; i++) {
    const entry = index.entries[i]!;
    const encodedId = encodedIds[i]!;

    // Write entity_id length
    offset = encodeVarint(encodedId.length, buffer, offset);

    // Write entity_id bytes
    buffer.set(encodedId, offset);
    offset += encodedId.length;

    // Write offset
    offset = encodeVarint(entry.offset, buffer, offset);

    // Write length
    offset = encodeVarint(entry.length, buffer, offset);
  }

  // Calculate and write CRC32 checksum
  const dataToChecksum = buffer.subarray(0, offset);
  const checksum = crc32(dataToChecksum);
  view.setUint32(offset, checksum, true);
  offset += 4;

  return buffer.subarray(0, offset);
}

// ============================================================================
// DECODING
// ============================================================================

/**
 * Decode entity index from binary format
 *
 * Validates CRC32 checksum and throws if corrupted.
 * Includes bounds checking for all reads.
 *
 * @param data - Binary data to decode
 * @returns Decoded entity index
 * @throws Error if data is corrupted, truncated, or invalid
 */
export function decodeEntityIndex(data: Uint8Array): EntityIndex {
  if (data.length < 8) {
    throw new Error(`Invalid entity index: too small (${data.length} bytes, minimum 8)`);
  }

  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const decoder = new TextDecoder();

  // Validate CRC32 checksum (stored in last 4 bytes)
  const storedChecksum = view.getUint32(data.length - 4, true);
  const calculatedChecksum = crc32(data.subarray(0, data.length - 4));

  if (storedChecksum !== calculatedChecksum) {
    throw new Error(
      `Entity index checksum mismatch: stored=0x${storedChecksum.toString(16)}, calculated=0x${calculatedChecksum.toString(16)}`
    );
  }

  let offset = 0;
  const dataEnd = data.length - 4; // Exclude checksum from data bounds

  // Read entry count
  const entryCount = view.getUint32(offset, true);
  offset += 4;

  // Validate entry count
  if (entryCount > MAX_ENTRY_COUNT) {
    throw new Error(`Entry count ${entryCount} exceeds maximum ${MAX_ENTRY_COUNT}`);
  }

  // Pre-allocate entries array for efficiency
  const entries: EntityIndexEntry[] = new Array(entryCount);

  for (let i = 0; i < entryCount; i++) {
    // Bounds check before reading varint
    if (offset >= dataEnd) {
      throw new Error(`Truncated index: expected ${entryCount} entries, only decoded ${i}`);
    }

    // Read entity_id length
    const idLenResult = decodeVarint(data, offset);
    offset = idLenResult.newOffset;
    const idLen = idLenResult.value;

    // Bounds check for entity_id bytes
    if (offset + idLen > dataEnd) {
      throw new Error(`Truncated entity ID at entry ${i}: need ${idLen} bytes, only ${dataEnd - offset} available`);
    }

    // Read entity_id bytes
    const entityId = decoder.decode(data.subarray(offset, offset + idLen));
    offset += idLen;

    // Bounds check before reading offset varint
    if (offset >= dataEnd) {
      throw new Error(`Truncated offset at entry ${i}`);
    }

    // Read offset
    const offsetResult = decodeVarint(data, offset);
    offset = offsetResult.newOffset;

    // Bounds check before reading length varint
    if (offset >= dataEnd) {
      throw new Error(`Truncated length at entry ${i}`);
    }

    // Read length
    const lengthResult = decodeVarint(data, offset);
    offset = lengthResult.newOffset;

    entries[i] = {
      entityId,
      offset: offsetResult.value,
      length: lengthResult.value,
    };
  }

  return {
    entries,
    version: 1,
  };
}

// ============================================================================
// BINARY SEARCH LOOKUP
// ============================================================================

/**
 * Binary search for exact entity match
 *
 * Uses standard binary search algorithm with string comparison.
 * Handles edge cases: empty index, single element, boundaries.
 *
 * @param index - Entity index to search
 * @param entityId - Exact entity ID to find
 * @returns Entry if found, null otherwise
 *
 * Time complexity: O(log n) comparisons
 * Space complexity: O(1)
 */
export function lookupEntity(index: EntityIndex, entityId: string): EntityIndexEntry | null {
  const entries = index.entries;
  const len = entries.length;

  if (len === 0) {
    return null;
  }

  let left = 0;
  let right = len - 1;

  while (left <= right) {
    // Use unsigned right shift to avoid overflow for very large arrays
    const mid = (left + right) >>> 1;
    const entry = entries[mid]!;
    const cmp = entry.entityId.localeCompare(entityId);

    if (cmp === 0) {
      return entry;
    } else if (cmp < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }

  return null;
}

/**
 * Binary search for first entry >= target (lower bound)
 *
 * Standard lower_bound algorithm: finds the insertion point for target
 * such that all elements before it are strictly less than target.
 *
 * @param entries - Sorted array of entries
 * @param target - Target string to search for
 * @returns Index of first entry >= target, or entries.length if none found
 *
 * Time complexity: O(log n)
 */
function lowerBound(entries: readonly EntityIndexEntry[], target: string): number {
  let left = 0;
  let right = entries.length;

  while (left < right) {
    const mid = (left + right) >>> 1;
    if (entries[mid]!.entityId.localeCompare(target) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

/**
 * Binary search for first entry > target (upper bound)
 *
 * Finds the first entry that is strictly greater than any string
 * starting with the given prefix. Used to find the end of prefix range.
 *
 * @param entries - Sorted array of entries
 * @param prefix - Prefix to search beyond
 * @returns Index of first entry > any prefix match, or entries.length
 */
function upperBoundPrefix(entries: readonly EntityIndexEntry[], prefix: string): number {
  let left = 0;
  let right = entries.length;

  while (left < right) {
    const mid = (left + right) >>> 1;
    const entry = entries[mid]!;
    // Check if entry could start with prefix or comes before any prefix match
    if (entry.entityId.startsWith(prefix) ||
        entry.entityId.localeCompare(prefix) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }

  return left;
}

/**
 * Binary search for prefix range
 *
 * Returns all entries where entityId starts with the given prefix.
 * Uses binary search to find range bounds for efficiency.
 *
 * @param index - Entity index to search
 * @param prefix - Prefix to match (empty string returns all entries)
 * @returns Array of matching entries (new array, does not share references)
 *
 * Time complexity: O(log n + k) where k is the number of matches
 * Space complexity: O(k) for the result array
 */
export function lookupPrefix(index: EntityIndex, prefix: string): EntityIndexEntry[] {
  const entries = index.entries;

  if (entries.length === 0) {
    return [];
  }

  // Empty prefix returns a copy of all entries
  if (prefix === '') {
    return entries.slice();
  }

  // Find first entry >= prefix (start of potential matches)
  const startIdx = lowerBound(entries, prefix);

  // Early exit if no entries could match
  if (startIdx >= entries.length) {
    return [];
  }

  // Find end of matching range using upper bound
  const endIdx = upperBoundPrefix(entries, prefix);

  // Return slice of matching entries
  if (startIdx >= endIdx) {
    return [];
  }

  return entries.slice(startIdx, endIdx);
}

// ============================================================================
// SIZE ESTIMATION
// ============================================================================

/**
 * Estimate the encoded size of the index in bytes
 *
 * Useful for budget checking before encoding. Uses TextEncoder
 * to accurately measure UTF-8 byte lengths for entity IDs.
 *
 * @param index - Entity index to measure
 * @returns Estimated size in bytes (exact for valid indexes)
 */
export function getIndexSize(index: EntityIndex): number {
  const encoder = new TextEncoder();

  let size = 4; // entry count header

  for (const entry of index.entries) {
    // Calculate UTF-8 byte length of entity ID
    const idByteLength = encoder.encode(entry.entityId).length;

    size += varintSize(idByteLength);  // entity_id length varint
    size += idByteLength;               // entity_id bytes
    size += varintSize(entry.offset);   // offset varint
    size += varintSize(entry.length);   // length varint
  }

  size += 4; // CRC32 checksum trailer

  return size;
}

/**
 * Estimate average bytes per entry for capacity planning
 *
 * @param index - Entity index to analyze
 * @returns Average bytes per entry, or 0 for empty index
 */
export function getAverageBytesPerEntry(index: EntityIndex): number {
  if (index.entries.length === 0) {
    return 0;
  }

  const totalSize = getIndexSize(index);
  const overhead = 8; // 4 bytes header + 4 bytes checksum
  const dataSize = totalSize - overhead;

  return dataSize / index.entries.length;
}
