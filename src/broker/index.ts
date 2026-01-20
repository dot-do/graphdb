/**
 * Broker module exports
 *
 * The Broker DO handles WebSocket connections with hibernation support,
 * enabling 95% cost savings while maintaining fresh subrequest quotas.
 */

export {
  BrokerDO,
  type WebSocketAttachment,
  type SubrequestBatchResult,
  type BrokerMetrics,
} from './broker-do.js';

export {
  planQuery,
  executeStep,
  orchestrateQuery,
  batchLookups,
  type QueryPlan,
  type QueryStep,
  type QueryResult,
  type FilterExpr,
} from './orchestrator.js';

export {
  validateShardResponse,
  isShardError,
  type ShardResponse,
  type ShardError,
  type ShardSuccess,
  type ShardErrorResponse,
} from './response-validator.js';

export {
  // Types
  type BrokerCacheConfig,
  type CacheableRequest,
  type CacheableResponse,
  type CachedResponse,
  type InvalidationResult,
  type RequestType,
  type ShouldCacheOptions,
  // Core class
  BrokerEdgeCache,
  // Utility functions
  createCacheTagsForNamespace,
  createCacheTagsForQuery,
  shouldCacheResponse,
  extractCacheableRequest,
} from './edge-cache.js';
