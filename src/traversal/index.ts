/**
 * Traversal module exports
 *
 * This module provides region-optimized graph traversal using colo.do + capnweb.
 *
 * Exports:
 * - Edge Worker: Routes requests to R2-local DOs
 * - TraversalDO: Handles graph traversals at R2-local colos
 * - Routing utilities: pickBestColo, isR2LocalColo, makeRoutingDecision
 */

// Edge Worker exports (primary entry point)
export {
  // Constants
  R2_LOCAL_COLOS,
  DEFAULT_COLO,
  // Types
  type R2LocalColo,
  type EdgeWorkerEnv,
  type RoutingDecision,
  type DiagnosticInfo,
  type TraversalEntity,
  type TraversalEdge,
  type TraversalResult,
  type TraversalApi,
  // Routing functions
  isR2LocalColo,
  pickBestColo,
  getRequestColo,
  makeRoutingDecision,
  // Fetch handler
  edgeWorkerFetch,
  // Durable Object
  TraversalDO,
} from './edge-worker.js';

// Re-export types and constants from traversal-do.ts for backwards compatibility
export type {
  TraversalEnv,
  DOEntity,
  Edge,
  BootstrapState,
} from './traversal-do.js';

export {
  MAX_PATH_DEPTH,
  DEFAULT_PATH_DEPTH,
} from './traversal-do.js';

// Graph Lookup exports (bloom filter routing)
export {
  GraphLookup,
  namespaceToR2Path,
  extractNamespaceFromEntityId,
  type LookupEntity,
  type LookupEdge,
  type TraversalChunkInfo,
  type ChunkManifest,
  type LookupStats,
  type GraphLookupConfig,
} from './graph-lookup.js';

// Manifest Store exports (DO-local storage for zero-RTT lookups)
export {
  ManifestStore,
  type ManifestFile,
  type EntityIndexEntry,
  type R2Manifest,
} from './manifest-store.js';

// Default export for worker entry point
export { default } from './edge-worker.js';
