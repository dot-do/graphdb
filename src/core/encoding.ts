/**
 * Binary Encoding Utilities for GraphDB
 *
 * Provides shared binary encoding/decoding utilities used across:
 * - GraphCol columnar storage format
 * - Entity index files
 * - Combined index files
 *
 * Implements LEB128 (Little Endian Base 128) variable-length encoding.
 *
 * @packageDocumentation
 */

// ============================================================================
// CONSTANTS
// ============================================================================

/** Maximum safe value for varint encoding (2^53 - 1, JavaScript's safe integer limit) */
export const MAX_SAFE_VARINT = Number.MAX_SAFE_INTEGER;

/** Maximum bytes needed for a 53-bit varint */
export const MAX_VARINT_BYTES = 8;

// ============================================================================
// UNSIGNED VARINT (LEB128)
// ============================================================================

/**
 * Encode unsigned varint (LEB128)
 *
 * Supports values up to Number.MAX_SAFE_INTEGER (2^53 - 1).
 * Uses arithmetic division instead of bit shifts for large number safety.
 *
 * @param value - Non-negative integer to encode
 * @param buffer - Target buffer
 * @param offset - Write position in buffer
 * @returns New offset after writing
 * @throws Error if value is negative or exceeds safe integer range
 *
 * @example
 * ```typescript
 * const buffer = new Uint8Array(10);
 * let offset = encodeVarint(300, buffer, 0);
 * // buffer now contains [0xAC, 0x02] and offset is 2
 * ```
 */
export function encodeVarint(value: number, buffer: Uint8Array, offset: number): number {
  if (value < 0) {
    throw new Error(`Varint value must be non-negative: ${value}`);
  }
  if (value > MAX_SAFE_VARINT) {
    throw new Error(`Varint value exceeds safe integer range: ${value}`);
  }

  // Fast path for small values (most common case)
  if (value < 0x80) {
    buffer[offset++] = value;
    return offset;
  }

  // Use arithmetic for large number safety
  while (value >= 0x80) {
    buffer[offset++] = (value & 0x7f) | 0x80;
    value = Math.floor(value / 128);
  }
  buffer[offset++] = value;
  return offset;
}

/**
 * Decode unsigned varint (LEB128)
 *
 * Includes bounds checking to prevent buffer overrun and overflow detection.
 *
 * @param buffer - Source buffer
 * @param offset - Read position in buffer
 * @returns Decoded value and new offset
 * @throws Error if buffer is truncated or value overflows
 *
 * @example
 * ```typescript
 * const buffer = new Uint8Array([0xAC, 0x02]);
 * const { value, newOffset } = decodeVarint(buffer, 0);
 * // value is 300, newOffset is 2
 * ```
 */
export function decodeVarint(buffer: Uint8Array, offset: number): { value: number; newOffset: number } {
  let value = 0;
  let shift = 0;
  let byte: number;
  const startOffset = offset;

  do {
    // Bounds check
    if (offset >= buffer.length) {
      throw new Error(`Truncated varint at offset ${startOffset}`);
    }

    // Overflow check (more than 8 bytes means value > 2^56)
    if (offset - startOffset >= MAX_VARINT_BYTES) {
      throw new Error(`Varint overflow at offset ${startOffset}`);
    }

    byte = buffer[offset++]!;

    // Use multiplication for large number safety (when shift >= 28)
    if (shift < 28) {
      value |= (byte & 0x7f) << shift;
    } else {
      value += (byte & 0x7f) * Math.pow(2, shift);
    }
    shift += 7;
  } while (byte >= 0x80);

  return { value, newOffset: offset };
}

/**
 * Calculate bytes needed for varint encoding
 *
 * @param value - Non-negative integer
 * @returns Number of bytes required
 * @throws Error if value is negative
 *
 * @example
 * ```typescript
 * varintSize(127);  // returns 1
 * varintSize(128);  // returns 2
 * varintSize(300);  // returns 2
 * varintSize(16383); // returns 2
 * varintSize(16384); // returns 3
 * ```
 */
export function varintSize(value: number): number {
  if (value < 0) {
    throw new Error(`Varint value must be non-negative: ${value}`);
  }

  // Fast path for common small values
  if (value < 0x80) return 1;
  if (value < 0x4000) return 2;
  if (value < 0x200000) return 3;
  if (value < 0x10000000) return 4;

  // Fallback for larger values
  let size = 5;
  let remaining = Math.floor(value / 0x10000000);
  while (remaining >= 0x80) {
    size++;
    remaining = Math.floor(remaining / 128);
  }
  return size;
}

// ============================================================================
// SIGNED VARINT (ZigZag + LEB128)
// ============================================================================

/**
 * Encode signed varint using ZigZag encoding
 *
 * ZigZag encoding maps signed integers to unsigned integers so that
 * small negative numbers have small encodings.
 * Maps: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4, etc.
 *
 * @param value - Signed bigint to encode
 * @param buffer - Target buffer
 * @param offset - Write position in buffer
 * @returns New offset after writing
 *
 * @example
 * ```typescript
 * const buffer = new Uint8Array(10);
 * let offset = encodeSignedVarint(-1n, buffer, 0);
 * // buffer now contains [0x01] and offset is 1
 * ```
 */
export function encodeSignedVarint(value: bigint, buffer: Uint8Array, offset: number): number {
  // ZigZag encoding: (n << 1) ^ (n >> 63)
  const zigzag = value >= 0n ? value << 1n : ((-value) << 1n) - 1n;
  let v = zigzag;
  do {
    let byte = Number(v & 0x7fn);
    v >>= 7n;
    if (v !== 0n) byte |= 0x80;
    buffer[offset++] = byte;
  } while (v !== 0n);
  return offset;
}

/**
 * Decode signed varint using ZigZag decoding
 *
 * @param buffer - Source buffer
 * @param offset - Read position in buffer
 * @returns Decoded value and new offset
 *
 * @example
 * ```typescript
 * const buffer = new Uint8Array([0x01]);
 * const { value, newOffset } = decodeSignedVarint(buffer, 0);
 * // value is -1n, newOffset is 1
 * ```
 */
export function decodeSignedVarint(buffer: Uint8Array, offset: number): { value: bigint; newOffset: number } {
  let value = 0n;
  let shift = 0n;
  let byte: number;
  do {
    byte = buffer[offset++]!;
    value |= BigInt(byte & 0x7f) << shift;
    shift += 7n;
  } while (byte & 0x80);
  // ZigZag decoding: (n >> 1) ^ -(n & 1)
  const result = (value >> 1n) ^ (-(value & 1n));
  return { value: result, newOffset: offset };
}

// ============================================================================
// CRC32 CHECKSUM
// ============================================================================

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
 *
 * @param data - Data to checksum
 * @returns 32-bit unsigned checksum value
 *
 * @example
 * ```typescript
 * const data = new TextEncoder().encode("hello");
 * const checksum = crc32(data);
 * ```
 */
export function crc32(data: Uint8Array): number {
  let crc = 0xFFFFFFFF;
  for (let i = 0; i < data.length; i++) {
    crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ data[i]!) & 0xFF]!;
  }
  return (crc ^ 0xFFFFFFFF) >>> 0;
}
