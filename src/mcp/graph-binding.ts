/**
 * Graph Binding Implementation
 *
 * Creates the `graph` binding exposed to the MCP 'do' tool.
 * Provides a sandboxed API for graph operations.
 */

import type { Entity } from '../core/entity.js'
import type { AuthContext } from '../security/auth.js'
import { checkPermission, checkNamespaceAccess } from '../security/permissions.js'
import type {
  GraphBinding,
  GraphDBEnv,
  MCPAuthContext,
  TraverseOptions,
  SearchOptions,
  VectorSearchOptions,
  TextSearchResult,
  VectorSearchResult,
  SPARQLResult,
} from './types.js'

/**
 * Map MCP auth context to GraphDB auth context
 */
function mapAuthContext(mcpAuth: MCPAuthContext): AuthContext {
  const context: AuthContext = {
    callerId: mcpAuth.id,
    authMethod: mcpAuth.type === 'apikey' ? 'api_key' : mcpAuth.type === 'oauth' ? 'jwt' : 'api_key',
    permissions: mcpAuth.readonly ? ['read'] : ['read', 'write'],
    namespaces: ['*'], // Will be filtered by permission checks
    timestamp: Date.now(),
  }
  if (mcpAuth.metadata) {
    context.metadata = mcpAuth.metadata
  }
  return context
}

/**
 * Extract namespace from entity ID
 */
function extractNamespace(entityId: string): string {
  try {
    const url = new URL(entityId)
    return url.hostname
  } catch {
    // Not a URL, use default namespace
    return 'default'
  }
}

/**
 * Create a graph binding for the MCP do tool
 *
 * @param env - GraphDB environment bindings
 * @param mcpAuth - MCP authentication context
 * @returns Graph binding with permission-checked operations
 */
export function createGraphBinding(
  env: GraphDBEnv,
  mcpAuth: MCPAuthContext
): GraphBinding {
  const authContext = mapAuthContext(mcpAuth)

  /**
   * Get shard stub for an entity ID
   */
  async function getShardStub(entityId: string): Promise<DurableObjectStub> {
    // Simple shard routing based on entity ID hash
    const namespace = extractNamespace(entityId)
    const shardId = env.SHARD.idFromName(`shard-${namespace}`)
    return env.SHARD.get(shardId)
  }

  /**
   * Check read permission for namespace
   */
  function checkReadPermission(entityId: string): void {
    const namespace = extractNamespace(entityId)
    const access = checkNamespaceAccess(authContext, namespace)
    if (!access.allowed) {
      throw new Error(`Permission denied: cannot read from namespace '${namespace}'`)
    }
  }

  /**
   * Check write permission for namespace
   */
  function checkWritePermission(entityId: string): void {
    if (mcpAuth.readonly) {
      throw new Error('Permission denied: read-only access')
    }
    const namespace = extractNamespace(entityId)
    const access = checkNamespaceAccess(authContext, namespace)
    if (!access.allowed) {
      throw new Error(`Permission denied: cannot write to namespace '${namespace}'`)
    }
  }

  return {
    async query(sparql: string): Promise<SPARQLResult> {
      // SPARQL queries require read permission
      const permissionCheck = checkPermission(authContext, 'read')
      if (!permissionCheck.allowed) {
        throw new Error('Permission denied: cannot execute SPARQL query')
      }

      const startTime = Date.now()

      // Get default broker for query routing
      const brokerId = env.BROKER.idFromName('default')
      const brokerStub = env.BROKER.get(brokerId)

      // Execute query via broker
      const response = await brokerStub.fetch(
        new Request('https://internal/query', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({ sparql }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`SPARQL query failed: ${error}`)
      }

      const result = (await response.json()) as { bindings: Array<Record<string, unknown>>; truncated?: boolean }
      const executionTime = Date.now() - startTime

      return {
        bindings: result.bindings,
        executionTime,
        truncated: result.truncated ?? false,
      }
    },

    async addTriple(subject: string, predicate: string, object: unknown): Promise<void> {
      checkWritePermission(subject)

      const shardStub = await getShardStub(subject)

      const response = await shardStub.fetch(
        new Request('https://internal/triples', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({
            subject,
            predicate,
            object,
          }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Failed to add triple: ${error}`)
      }
    },

    async removeTriple(subject: string, predicate: string, object: unknown): Promise<void> {
      checkWritePermission(subject)

      const shardStub = await getShardStub(subject)

      const response = await shardStub.fetch(
        new Request('https://internal/triples', {
          method: 'DELETE',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({
            subject,
            predicate,
            object,
          }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Failed to remove triple: ${error}`)
      }
    },

    async getNode(id: string): Promise<Entity | null> {
      checkReadPermission(id)

      const shardStub = await getShardStub(id)

      const response = await shardStub.fetch(
        new Request(`https://internal/entity/${encodeURIComponent(id)}`, {
          method: 'GET',
          headers: {
            'X-Auth-Context': JSON.stringify(authContext),
          },
        })
      )

      if (response.status === 404) {
        return null
      }

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Failed to get node: ${error}`)
      }

      return (await response.json()) as Entity
    },

    async traverse(startId: string, options?: TraverseOptions): Promise<Entity[]> {
      checkReadPermission(startId)

      const brokerId = env.BROKER.idFromName('default')
      const brokerStub = env.BROKER.get(brokerId)

      const response = await brokerStub.fetch(
        new Request('https://internal/traverse', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({
            startId,
            maxDepth: options?.maxDepth ?? 3,
            limit: options?.limit ?? 100,
            predicates: options?.predicates,
            direction: options?.direction ?? 'outgoing',
          }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Traversal failed: ${error}`)
      }

      const result = (await response.json()) as { entities: Entity[] }
      return result.entities
    },

    async search(text: string, options?: SearchOptions): Promise<TextSearchResult[]> {
      // Check read permission for requested namespaces
      const namespaces = options?.namespaces ?? ['*']
      for (const ns of namespaces) {
        if (ns !== '*') {
          const access = checkNamespaceAccess(authContext, ns)
          if (!access.allowed) {
            throw new Error(`Permission denied: cannot search namespace '${ns}'`)
          }
        }
      }

      const brokerId = env.BROKER.idFromName('default')
      const brokerStub = env.BROKER.get(brokerId)

      const response = await brokerStub.fetch(
        new Request('https://internal/search', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({
            query: text,
            limit: options?.limit ?? 20,
            offset: options?.offset ?? 0,
            namespaces: options?.namespaces,
            types: options?.types,
          }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Search failed: ${error}`)
      }

      const result = (await response.json()) as { results: TextSearchResult[] }
      return result.results
    },

    async vectorSearch(embedding: number[], options?: VectorSearchOptions): Promise<VectorSearchResult[]> {
      // Check read permission for requested namespaces
      const namespaces = options?.namespaces ?? ['*']
      for (const ns of namespaces) {
        if (ns !== '*') {
          const access = checkNamespaceAccess(authContext, ns)
          if (!access.allowed) {
            throw new Error(`Permission denied: cannot search namespace '${ns}'`)
          }
        }
      }

      const brokerId = env.BROKER.idFromName('default')
      const brokerStub = env.BROKER.get(brokerId)

      const response = await brokerStub.fetch(
        new Request('https://internal/vector-search', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Auth-Context': JSON.stringify(authContext),
          },
          body: JSON.stringify({
            embedding,
            limit: options?.limit ?? 10,
            threshold: options?.threshold ?? 0.7,
            namespaces: options?.namespaces,
          }),
        })
      )

      if (!response.ok) {
        const error = await response.text()
        throw new Error(`Vector search failed: ${error}`)
      }

      const result = (await response.json()) as { results: VectorSearchResult[] }
      return result.results
    },
  }
}

/**
 * Generate TypeScript type definitions for the graph binding
 */
export const GRAPH_BINDING_TYPES = `
interface TraverseOptions {
  maxDepth?: number;
  limit?: number;
  predicates?: string[];
  direction?: 'outgoing' | 'incoming' | 'both';
}

interface SearchOptions {
  limit?: number;
  offset?: number;
  namespaces?: string[];
  types?: string[];
}

interface VectorSearchOptions {
  limit?: number;
  threshold?: number;
  namespaces?: string[];
}

interface Entity {
  $id: string;
  $type: string;
  $context?: string;
  [key: string]: unknown;
}

interface TextSearchResult {
  id: string;
  score: number;
  snippet?: string;
  entity?: Entity;
}

interface VectorSearchResult {
  id: string;
  similarity: number;
  entity?: Entity;
}

interface SPARQLResult {
  bindings: Array<Record<string, unknown>>;
  executionTime: number;
  truncated: boolean;
}

/**
 * Graph binding for interacting with the GraphDB knowledge graph.
 *
 * Available methods:
 * - query(sparql): Execute SPARQL queries
 * - addTriple(s, p, o): Add a triple
 * - removeTriple(s, p, o): Remove a triple
 * - getNode(id): Get entity by ID
 * - traverse(id, options): Traverse the graph
 * - search(text, options): Full-text search
 * - vectorSearch(embedding, options): Vector similarity search
 */
declare const graph: {
  query(sparql: string): Promise<SPARQLResult>;
  addTriple(subject: string, predicate: string, object: unknown): Promise<void>;
  removeTriple(subject: string, predicate: string, object: unknown): Promise<void>;
  getNode(id: string): Promise<Entity | null>;
  traverse(startId: string, options?: TraverseOptions): Promise<Entity[]>;
  search(text: string, options?: SearchOptions): Promise<TextSearchResult[]>;
  vectorSearch(embedding: number[], options?: VectorSearchOptions): Promise<VectorSearchResult[]>;
};
`
