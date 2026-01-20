/**
 * Snippet Layer Exports
 *
 * Bloom filter, query lexer, routing utilities, and edge caching for Cloudflare Snippets.
 */

export {
  // Types
  type BloomFilter,
  type BloomFilterConfig,
  type CreateBloomFilterOptions,
  type SerializedFilter,
  // Core functions
  createBloomFilter,
  addToFilter,
  addManyToFilter,
  mightExist,
  // Serialization
  serializeFilter,
  deserializeFilter,
  // Filter operations
  mergeFilters,
  createIncrementalFilter,
  // Utilities
  extractEntityId,
  getFilterStats,
  estimateFpr,
  // Math utilities
  calculateOptimalBits,
  calculateOptimalK,
  calculateExpectedFpr,
} from "./bloom.js";

export {
  // Types
  TokenType,
  type Token,
  type Lexer,
  // Core functions
  tokenize,
  createLexer,
} from "./lexer.js";

export {
  // Types
  type ShardInfo,
  type RouteResult,
  // Core functions
  extractNamespace,
  routeEntity,
  routeQuery,
  getShardId,
  // Caching utilities
  canServeFromCache,
  generateCacheKey,
  estimateQueryCost,
} from "./router.js";

export {
  // Types
  type EdgeCacheConfig,
  type CachedBloomFilter,
  type CachedIndexSegment,
  type IndexSegmentEntry,
  type CachePutOptions,
  type CacheGetOptions,
  type ParsedCacheKey,
  // Constants
  DEFAULT_BLOOM_TTL,
  DEFAULT_SEGMENT_TTL,
  // Core class
  EdgeCache,
  // Utility functions
  createEdgeCacheKey,
  parseEdgeCacheKey,
} from "./edge-cache.js";

export {
  // Types
  type ImmutableBloomCacheConfig,
  type BloomCacheEntry,
  type BloomCacheHeaderOptions,
  // Constants
  DEFAULT_IMMUTABLE_MAX_AGE,
  // Core class
  ImmutableBloomCache,
  // Utility functions
  createBloomCacheKey,
  parseBloomCacheKey,
  generateBloomCacheHeaders,
} from "./bloom-cache.js";

export {
  // Types
  type BloomRouterConfig,
  type BloomRouteResult,
  // Core class
  BloomRouter,
} from "./bloom-router.js";

// Re-export all constants from the centralized constants module
export {
  // Router configuration
  DEFAULT_SHARD_COUNT,
  DEFAULT_CACHE_TTL_SECONDS,
  MAX_QUERY_COST,
  CROSS_NAMESPACE_COST,
  BASE_QUERY_COST,
  FILTER_COST_MULTIPLIER,
  URL_CONTEXT_LOOKBACK_CHARS,
  CACHE_KEY_HEX_PAD_LENGTH,
  // Bloom filter configuration
  DEFAULT_BLOOM_FPR,
  DEFAULT_BLOOM_MAX_SIZE_BYTES,
  TIMESTAMP_ENCODING_BASE,
  BYTES_PER_KB,
  PERCENTAGE_MULTIPLIER,
  DEFAULT_FPR_TEST_COUNT,
  // Cache configuration
  DEFAULT_BLOOM_TTL_SECONDS,
  DEFAULT_SEGMENT_TTL_SECONDS,
  IMMUTABLE_MAX_AGE_SECONDS,
  TIMESTAMP_PARSE_RADIX,
} from "./constants.js";
