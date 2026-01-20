/**
 * Constants for Snippet Router Layer
 *
 * Centralizes magic numbers used across the snippet routing layer for:
 * - Query routing and shard selection
 * - Query cost estimation and rate limiting
 * - Bloom filter configuration
 * - Cache TTL and configuration
 *
 * These values are tuned for Cloudflare Workers constraints:
 * - 32KB max snippet size
 * - 5ms max compute time
 * - Optimal edge caching behavior
 */

// ============================================================================
// Router Configuration
// ============================================================================

/**
 * Default number of shards for consistent hashing.
 * 256 provides good distribution while keeping shard IDs manageable.
 * Each namespace hashes to one of 256 possible shards.
 */
export const DEFAULT_SHARD_COUNT = 256;

/**
 * Default cache TTL for read queries in seconds (5 minutes).
 * Balances freshness with cache efficiency for typical graph traversals.
 */
export const DEFAULT_CACHE_TTL_SECONDS = 300;

/**
 * Maximum query cost for rate limiting.
 * Queries exceeding this cost should be rejected or throttled.
 */
export const MAX_QUERY_COST = 100;

/**
 * Base cost for cross-namespace queries.
 * Cross-namespace queries require coordination between shards,
 * so they're penalized to encourage namespace-local queries.
 */
export const CROSS_NAMESPACE_COST = 5;

/**
 * Base cost for any query (entity lookup).
 * This is the minimum cost charged for any query operation.
 */
export const BASE_QUERY_COST = 1;

/**
 * Cost multiplier for filter operations in queries.
 * Filters like [?age > 30] require scanning and comparison,
 * so they cost more than simple traversals.
 */
export const FILTER_COST_MULTIPLIER = 2;

/**
 * Characters to look back when detecting URL context for traversals.
 * Used in countTraversalHops to determine if a dot is a TLD or property access.
 */
export const URL_CONTEXT_LOOKBACK_CHARS = 50;

/**
 * Hex string padding length for cache key generation.
 * Ensures consistent 8-character hex representation of hash values.
 */
export const CACHE_KEY_HEX_PAD_LENGTH = 8;

// ============================================================================
// Bloom Filter Configuration
// ============================================================================

/**
 * Default target false positive rate for bloom filters (1%).
 * This provides a good balance between filter size and accuracy.
 * Lower rates require more memory but reduce unnecessary shard queries.
 */
export const DEFAULT_BLOOM_FPR = 0.01;

/**
 * Default maximum bloom filter size in bytes (16KB).
 * Chosen to stay well under the 32KB Cloudflare Snippet limit,
 * leaving room for code and other data structures.
 */
export const DEFAULT_BLOOM_MAX_SIZE_BYTES = 16 * 1024;

/**
 * Base for encoding timestamps as short strings.
 * Base 36 uses digits 0-9 and letters a-z for compact representation.
 */
export const TIMESTAMP_ENCODING_BASE = 36;

/**
 * Bytes per kilobyte for size calculations.
 */
export const BYTES_PER_KB = 1024;

/**
 * Percentage multiplier for utilization calculations.
 */
export const PERCENTAGE_MULTIPLIER = 100;

/**
 * Default number of random tests for estimating false positive rate.
 * Higher values give more accurate estimates but take longer.
 */
export const DEFAULT_FPR_TEST_COUNT = 10000;

// ============================================================================
// Cache Configuration
// ============================================================================

/**
 * Default TTL for bloom filters at the edge in seconds (5 minutes).
 * Bloom filters are updated less frequently, but we use a conservative TTL
 * to ensure timely propagation of new entities.
 */
export const DEFAULT_BLOOM_TTL_SECONDS = 300;

/**
 * Default TTL for index segments at the edge in seconds (1 hour).
 * Index segments are immutable once created, so longer TTL is safe.
 */
export const DEFAULT_SEGMENT_TTL_SECONDS = 3600;

/**
 * Max-age for immutable bloom filters in seconds (1 year).
 * Content-addressed bloom filters can be cached indefinitely.
 */
export const IMMUTABLE_MAX_AGE_SECONDS = 31536000;

/**
 * Radix for parsing timestamp strings from cache headers.
 * Standard decimal (base 10) parsing.
 */
export const TIMESTAMP_PARSE_RADIX = 10;
