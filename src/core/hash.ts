/**
 * FNV-1a Hash Functions for GraphDB
 *
 * Provides consistent hashing utilities used across:
 * - Bloom filter membership testing (double hashing)
 * - Shard routing (single hash)
 * - Cache key generation
 *
 * FNV-1a is chosen for:
 * - Good distribution for string keys
 * - Fast computation (no crypto overhead)
 * - Deterministic results across JS environments
 */

// ============================================================================
// Constants
// ============================================================================

/** FNV-1a 32-bit prime */
const FNV_PRIME = 0x01000193;

/** FNV-1a 32-bit offset basis */
const FNV_OFFSET = 0x811c9dc5;

// ============================================================================
// Single Hash
// ============================================================================

/**
 * FNV-1a hash function for consistent hashing.
 *
 * Produces a 32-bit unsigned integer hash from a string input.
 * Used for shard routing and cache key generation.
 *
 * @param str - The string to hash
 * @returns Unsigned 32-bit integer hash value
 *
 * @example
 * ```typescript
 * const hash = fnv1aHash("https://example.com/users/123");
 * const shardIndex = hash % 256;
 * ```
 */
export function fnv1aHash(str: string): number {
  let hash = FNV_OFFSET;

  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, FNV_PRIME);
  }

  return hash >>> 0; // Ensure unsigned 32-bit
}

// ============================================================================
// Double Hash
// ============================================================================

/**
 * FNV-1a double hash function for bloom filters.
 *
 * Produces two independent 32-bit hashes that can be combined
 * to generate k hash values using the formula: h(i) = h1 + i * h2
 *
 * The second hash includes an additional mixing step (XOR with right shift)
 * to ensure independence from the first hash.
 *
 * @param key - The string to hash
 * @returns Tuple of two unsigned 32-bit integer hashes [h1, h2]
 *
 * @example
 * ```typescript
 * const [h1, h2] = fnv1aDoubleHash("entity:123");
 * for (let i = 0; i < k; i++) {
 *   const pos = ((h1 + Math.imul(i, h2)) >>> 0) % m;
 *   // Use pos for bloom filter bit position
 * }
 * ```
 */
export function fnv1aDoubleHash(key: string): [number, number] {
  let h1 = FNV_OFFSET;
  let h2 = FNV_OFFSET;

  for (let i = 0; i < key.length; i++) {
    const c = key.charCodeAt(i);
    h1 ^= c;
    h1 = Math.imul(h1, FNV_PRIME);
    h2 ^= c;
    h2 = Math.imul(h2, FNV_PRIME);
    h2 ^= h2 >>> 16; // Additional mixing for independence
  }

  return [h1 >>> 0, h2 >>> 0];
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Convert a hash value to a hex string, zero-padded to 8 characters.
 *
 * Useful for generating human-readable shard IDs or cache keys.
 *
 * @param hash - The hash value to convert
 * @returns 8-character hexadecimal string
 *
 * @example
 * ```typescript
 * const hash = fnv1aHash("namespace");
 * const hex = hashToHex(hash); // "a1b2c3d4"
 * const shardId = `shard-${hex}`;
 * ```
 */
export function hashToHex(hash: number): string {
  return hash.toString(16).padStart(8, '0');
}
