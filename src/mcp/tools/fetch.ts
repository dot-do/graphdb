/**
 * Fetch Tool
 *
 * MCP tool for fetching entities/nodes from the GraphDB knowledge graph.
 * Retrieves triples and entities by ID with optional relation expansion.
 */

import type { Entity } from '../../core/entity.js'
import type { AuthContext } from '../../security/auth.js'
import { checkNamespaceAccess } from '../../security/permissions.js'
import type { GraphDBEnv, FetchInput, ToolResponse, MCPAuthContext } from '../types.js'

/**
 * Tool definition for the fetch tool
 */
export const fetchTool = {
  name: 'fetch',
  description: `Fetch an entity/node from the knowledge graph by its identifier.
Returns the entity with all its properties and optionally related entities.

Examples:
- fetch({ resource: "https://example.com/entity/123" })
- fetch({ resource: "https://example.com/user/alice", includeRelations: true })
- fetch({ resource: "https://example.com/article/456", depth: 2 })`,
  inputSchema: {
    type: 'object',
    properties: {
      resource: {
        type: 'string',
        description: 'Entity ID (URL format) or path to fetch',
      },
      includeRelations: {
        type: 'boolean',
        description: 'Include related entities (default: false)',
      },
      depth: {
        type: 'number',
        description: 'Depth for relation expansion (default: 1, max: 3)',
      },
    },
    required: ['resource'],
  },
} as const

/**
 * Extract namespace from entity ID
 */
function extractNamespace(entityId: string): string {
  try {
    const url = new URL(entityId)
    return url.hostname
  } catch {
    return 'default'
  }
}

/**
 * Map MCP auth context to GraphDB auth context
 */
function mapAuthContext(mcpAuth: MCPAuthContext): AuthContext {
  const context: AuthContext = {
    callerId: mcpAuth.id,
    authMethod: mcpAuth.type === 'apikey' ? 'api_key' : mcpAuth.type === 'oauth' ? 'jwt' : 'api_key',
    permissions: mcpAuth.readonly ? ['read'] : ['read', 'write'],
    namespaces: ['*'],
    timestamp: Date.now(),
  }
  if (mcpAuth.metadata) {
    context.metadata = mcpAuth.metadata
  }
  return context
}

/**
 * Fetch a single entity from the shard
 */
async function fetchEntity(
  env: GraphDBEnv,
  authContext: AuthContext,
  entityId: string
): Promise<Entity | null> {
  const namespace = extractNamespace(entityId)
  const shardId = env.SHARD.idFromName(`shard-${namespace}`)
  const shardStub = env.SHARD.get(shardId)

  const response = await shardStub.fetch(
    new Request(`https://internal/entity/${encodeURIComponent(entityId)}`, {
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
    throw new Error(`Failed to fetch entity: ${error}`)
  }

  return (await response.json()) as Entity
}

/**
 * Fetch entity with expanded relations
 */
async function fetchEntityWithRelations(
  env: GraphDBEnv,
  authContext: AuthContext,
  entityId: string,
  depth: number
): Promise<{ entity: Entity | null; relations: Record<string, Entity[]> }> {
  const entity = await fetchEntity(env, authContext, entityId)

  if (!entity || depth === 0) {
    return { entity, relations: {} }
  }

  // Fetch related entities via broker
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
        startId: entityId,
        maxDepth: Math.min(depth, 3), // Cap at 3 to prevent excessive traversal
        limit: 100,
        direction: 'outgoing',
      }),
    })
  )

  if (!response.ok) {
    // Relations fetch failed, return entity without relations
    return { entity, relations: {} }
  }

  const result = (await response.json()) as { entities: Entity[] }

  // Group relations by type
  const relations: Record<string, Entity[]> = {}
  for (const relatedEntity of result.entities) {
    // Skip the root entity itself
    if (relatedEntity.$id === entityId) continue

    // Get first type (or 'unknown' if not set)
    const typeValue = relatedEntity.$type
    const entityType = Array.isArray(typeValue) ? (typeValue[0] ?? 'unknown') : (typeValue ?? 'unknown')
    if (!Object.prototype.hasOwnProperty.call(relations, entityType)) {
      relations[entityType] = []
    }
    relations[entityType]!.push(relatedEntity)
  }

  return { entity, relations }
}

/**
 * Format entity for JSON output
 */
function formatEntity(entity: Entity | null): string {
  if (!entity) {
    return JSON.stringify({ found: false, entity: null }, null, 2)
  }

  return JSON.stringify({ found: true, entity }, null, 2)
}

/**
 * Format entity with relations for JSON output
 */
function formatEntityWithRelations(
  entity: Entity | null,
  relations: Record<string, Entity[]>
): string {
  if (!entity) {
    return JSON.stringify({ found: false, entity: null, relations: {} }, null, 2)
  }

  return JSON.stringify(
    {
      found: true,
      entity,
      relations,
      relationCounts: Object.fromEntries(
        Object.entries(relations).map(([k, v]) => [k, v.length])
      ),
    },
    null,
    2
  )
}

/**
 * Create a fetch handler for the MCP server
 *
 * @param env - GraphDB environment bindings
 * @param mcpAuth - MCP authentication context
 * @returns Handler function for the fetch tool
 */
export function createFetchHandler(
  env: GraphDBEnv,
  mcpAuth: MCPAuthContext
): (input: FetchInput) => Promise<ToolResponse> {
  const authContext = mapAuthContext(mcpAuth)

  return async (input: FetchInput): Promise<ToolResponse> => {
    try {
      // Check namespace access
      const namespace = extractNamespace(input.resource)
      const accessCheck = checkNamespaceAccess(authContext, namespace)
      if (!accessCheck.allowed) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                error: `Permission denied: cannot access namespace '${namespace}'`,
              }),
            },
          ],
          isError: true,
        }
      }

      const includeRelations = input.includeRelations ?? false
      const depth = Math.min(input.depth ?? 1, 3)

      let content: string

      if (includeRelations) {
        const { entity, relations } = await fetchEntityWithRelations(
          env,
          authContext,
          input.resource,
          depth
        )
        content = formatEntityWithRelations(entity, relations)
      } else {
        const entity = await fetchEntity(env, authContext, input.resource)
        content = formatEntity(entity)
      }

      return {
        content: [
          {
            type: 'text',
            text: content,
          },
        ],
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred'

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({ error: errorMessage }),
          },
        ],
        isError: true,
      }
    }
  }
}
