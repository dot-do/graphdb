/**
 * Protocol module - capnweb RPC integration for GraphDB
 *
 * Exports:
 * - GraphAPI interface and types
 * - GraphAPITarget - RpcTarget implementation
 * - Client factory functions (deprecated - use @dotdo/graphdb/client)
 * - Helper utilities
 *
 * ## Client SDK Migration
 *
 * The client functions in this module are deprecated. Use `@dotdo/graphdb/client` instead:
 *
 * ```typescript
 * // Old (deprecated):
 * import { createGraphClient } from '@dotdo/graphdb/protocol';
 *
 * // New (recommended):
 * import { createGraphClient } from '@dotdo/graphdb/client';
 * ```
 *
 * The new client SDK provides:
 * - Auto-reconnection with exponential backoff
 * - Connection state lifecycle callbacks
 * - Request timeouts
 * - Cleaner API (insert, query, update, delete)
 */

// Re-export types and interfaces
export type {
  ProtocolTriple,
  TraversalOptions,
  BatchResult,
  GraphAPI,
  GraphAPIMode,
  GraphAPITargetOptions,
} from './graph-api.js';

export { GraphAPITarget } from './graph-api.js';

// Re-export query executor functions
export {
  executeQuery,
  executeQueryWithParser,
  isFullUrlQuery,
  isSimpleLookup,
  parseQueryString,
} from './graph-api-executor.js';

export type {
  EntityStore,
  TraverseFunction,
  ExecutorContext,
} from './graph-api-executor.js';

// ============================================================================
// Legacy Client Exports (deprecated - use @dotdo/graphdb/client)
// ============================================================================

/**
 * @deprecated These client exports are deprecated.
 * Use `@dotdo/graphdb/client` instead for the recommended client SDK.
 */
export {
  createGraphClient,
  createGraphClientFromWebSocket,
  pipelineExample,
  batchExample,
  mapExample,
  ManualWebSocketClient,
} from './client.js';

/**
 * @deprecated These client types are deprecated.
 * Use the types from `@dotdo/graphdb/client` instead.
 */
export type {
  GraphClient,
  GraphClientOptions,
  ConnectionStats,
} from './client.js';

// ============================================================================
// New Client SDK Re-exports (for convenience/migration)
// ============================================================================

/**
 * New client SDK exports - available here for migration convenience.
 * Prefer importing directly from `@dotdo/graphdb/client`.
 */
export {
  createGraphClientV2,
  createGraphClientFromWebSocketV2,
} from './client.js';

export type {
  GraphClientV2,
  ClientOptions,
  ConnectionState,
  ConnectionStatsV2,
  TraversalOptionsV2,
  QueryResultV2,
  BatchResultV2,
  EntityInput,
  BatchOperation,
} from './client.js';
