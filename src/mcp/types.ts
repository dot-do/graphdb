/**
 * MCP Server Types for GraphDB
 *
 * Defines the types for the graph binding API exposed to MCP tools.
 */

import type { Entity } from '../core/entity.js'

/**
 * Options for graph traversal operations
 */
export interface TraverseOptions {
  /** Maximum traversal depth */
  maxDepth?: number
  /** Maximum number of results */
  limit?: number
  /** Predicates to follow (default: all) */
  predicates?: string[]
  /** Direction of traversal */
  direction?: 'outgoing' | 'incoming' | 'both'
}

/**
 * Options for text search operations
 */
export interface SearchOptions {
  /** Maximum number of results */
  limit?: number
  /** Offset for pagination */
  offset?: number
  /** Namespaces to search in */
  namespaces?: string[]
  /** Entity types to filter by */
  types?: string[]
}

/**
 * Options for vector search operations
 */
export interface VectorSearchOptions {
  /** Maximum number of results */
  limit?: number
  /** Minimum similarity threshold (0-1) */
  threshold?: number
  /** Namespaces to search in */
  namespaces?: string[]
}

/**
 * Result from a text search operation
 */
export interface TextSearchResult {
  /** Entity ID */
  id: string
  /** Relevance score */
  score: number
  /** Matched text snippet */
  snippet?: string
  /** Entity data */
  entity?: Entity
}

/**
 * Result from a vector search operation
 */
export interface VectorSearchResult {
  /** Entity ID */
  id: string
  /** Cosine similarity score */
  similarity: number
  /** Entity data */
  entity?: Entity
}

/**
 * SPARQL query result
 */
export interface SPARQLResult {
  /** Result bindings */
  bindings: Array<Record<string, unknown>>
  /** Query execution time in ms */
  executionTime: number
  /** Whether results were truncated */
  truncated: boolean
}

/**
 * Graph binding interface exposed to the 'do' tool
 *
 * Provides methods for graph operations including:
 * - SPARQL queries
 * - Triple management
 * - Node retrieval
 * - Graph traversal
 * - Full-text search
 * - Vector search
 */
export interface GraphBinding {
  /**
   * Execute a SPARQL query
   * @param sparql - SPARQL query string
   * @returns Query results
   */
  query(sparql: string): Promise<SPARQLResult>

  /**
   * Add a triple to the graph
   * @param subject - Subject entity ID
   * @param predicate - Predicate name
   * @param object - Object value (entity ID for refs, or literal value)
   */
  addTriple(subject: string, predicate: string, object: unknown): Promise<void>

  /**
   * Remove a triple from the graph
   * @param subject - Subject entity ID
   * @param predicate - Predicate name
   * @param object - Object value
   */
  removeTriple(subject: string, predicate: string, object: unknown): Promise<void>

  /**
   * Get a node (entity) by ID
   * @param id - Entity ID
   * @returns Entity or null if not found
   */
  getNode(id: string): Promise<Entity | null>

  /**
   * Traverse the graph starting from an entity
   * @param startId - Starting entity ID
   * @param options - Traversal options
   * @returns Array of entities found during traversal
   */
  traverse(startId: string, options?: TraverseOptions): Promise<Entity[]>

  /**
   * Full-text search across entities
   * @param text - Search query text
   * @param options - Search options
   * @returns Search results with scores
   */
  search(text: string, options?: SearchOptions): Promise<TextSearchResult[]>

  /**
   * Vector similarity search
   * @param embedding - Query embedding vector
   * @param options - Search options
   * @returns Similar entities with similarity scores
   */
  vectorSearch(embedding: number[], options?: VectorSearchOptions): Promise<VectorSearchResult[]>
}

/**
 * MCP Auth context for GraphDB
 * Maps MCP auth to GraphDB's multi-layer auth
 */
export interface MCPAuthContext {
  /** MCP auth type */
  type: 'anon' | 'oauth' | 'apikey'
  /** User/client identifier */
  id: string
  /** Whether this is read-only access */
  readonly: boolean
  /** Admin privileges */
  isAdmin?: boolean
  /** Raw token */
  token?: string
  /** Additional metadata */
  metadata?: Record<string, unknown>
}

/**
 * Configuration for the GraphDB MCP server
 */
export interface GraphDBMCPConfig {
  /** GraphDB environment bindings */
  env: GraphDBEnv
  /** Optional auth configuration */
  auth?: {
    /** Authentication mode */
    mode: 'anon' | 'anon+auth' | 'auth-required'
    /** OAuth introspection URL */
    oauthIntrospectionUrl?: string
    /** API key verification URL */
    apiKeyVerifyUrl?: string
  }
  /** Code execution timeout in ms */
  timeout?: number
}

/**
 * GraphDB environment bindings for Cloudflare Workers
 */
export interface GraphDBEnv {
  /** Broker Durable Object namespace */
  BROKER: DurableObjectNamespace
  /** Shard Durable Object namespace */
  SHARD: DurableObjectNamespace
  /** Traversal Durable Object namespace (optional) */
  TRAVERSAL_DO?: DurableObjectNamespace
  /** R2 bucket for graph storage (optional) */
  GRAPH_R2?: R2Bucket
  /** Optional auth configuration */
  AUTH_SECRET?: string
  /** Optional JWT issuer */
  JWT_ISSUER?: string
  /** Optional JWT audience */
  JWT_AUDIENCE?: string
}

/**
 * MCP tool response format
 */
export interface ToolResponse {
  content: Array<{ type: string; text: string }>
  isError?: boolean
}

/**
 * Search tool input
 */
export interface SearchInput {
  /** Search query - can be text search, SPARQL, or graph traversal */
  query: string
  /** Query type hint */
  type?: 'text' | 'sparql' | 'traverse'
  /** Maximum results */
  limit?: number
  /** Offset for pagination */
  offset?: number
}

/**
 * Fetch tool input
 */
export interface FetchInput {
  /** Resource identifier (entity ID or node path) */
  resource: string
  /** Include related entities */
  includeRelations?: boolean
  /** Depth for relation expansion */
  depth?: number
}

/**
 * Do tool input
 */
export interface DoInput {
  /** Code to execute */
  code: string
}
