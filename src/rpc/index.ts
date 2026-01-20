/**
 * RPC module - capnweb RPC types and utilities for GraphDB
 *
 * Exports:
 * - TraversalApi interface for cross-colo RPC
 * - RpcEntity, Edge, TraversalResult types
 * - Type-safe RPC parameter types and validation
 * - Retry utilities with exponential backoff for transient failures
 * - Helper types and utilities
 */

// Re-export all types
export type {
  RpcEntity,
  Edge,
  TraversalResult,
  TraversalStats,
  TraversalApi,
  TraversalApiOptions,
  TraversalContext,
  // Type-safe RPC parameter types
  GetEntityParams,
  CreateEntityParams,
  UpdateEntityParams,
  DeleteEntityParams,
  TraverseParams,
  ReverseTraverseParams,
  PathTraverseParams,
  QueryParams,
  BatchGetParams,
  BatchCreateParams,
  BatchExecuteParams,
  BatchOperation,
  RpcCallParams,
  RpcMethodName,
  RpcCallMessage,
  RpcBatchMessage,
  RpcValidationResult,
  TypedRpcCall,
} from './types.js';

// Re-export validation functions
export { validateRpcCall, isValidRpcMethod } from './types.js';

// Re-export retry types and utilities
export type { RetryConfig, RetryResult } from './retry.js';

export {
  DEFAULT_RETRY_CONFIG,
  isIdempotentMethod,
  isTransientError,
  shouldRetry,
  calculateRetryDelay,
  withRetry,
  withRpcRetry,
  createRetryConfig,
  createLowLatencyRetryConfig,
  createHighReliabilityRetryConfig,
} from './retry.js';
