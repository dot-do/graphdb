/**
 * Search Tool
 *
 * MCP tool for searching the GraphDB knowledge graph.
 * Supports SPARQL queries, graph traversal, and full-text search.
 */

import type { ToolResponse } from '@dotdo/mcp'
import type { AuthContext } from '../../security/auth.js'
import { checkPermission } from '../../security/permissions.js'
import type { GraphDBEnv, SearchInput, MCPAuthContext } from '../types.js'

/**
 * Tool definition for the search tool
 */
export const searchTool = {
  name: 'search',
  description: `Search the knowledge graph using various query methods:
- Text search: Natural language queries for full-text search
- SPARQL: Execute SPARQL queries for precise graph queries
- Traverse: Graph traversal from a starting entity

Examples:
- search({ query: "artificial intelligence papers" })
- search({ query: "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10", type: "sparql" })
- search({ query: "https://example.com/entity/123", type: "traverse" })`,
  inputSchema: {
    type: 'object',
    properties: {
      query: { type: 'string', description: 'Search query (text, SPARQL, or entity ID for traversal)' },
      type: {
        type: 'string',
        enum: ['text', 'sparql', 'traverse'],
        description: 'Query type (default: auto-detect)',
      },
      limit: { type: 'number', description: 'Maximum results (default: 20)' },
      offset: { type: 'number', description: 'Offset for pagination (default: 0)' },
    },
    required: ['query'],
  },
} as const

/**
 * Detect query type from query string
 */
function detectQueryType(query: string): 'text' | 'sparql' | 'traverse' {
  const trimmed = query.trim().toUpperCase()

  // Check for SPARQL keywords
  if (
    trimmed.startsWith('SELECT') ||
    trimmed.startsWith('ASK') ||
    trimmed.startsWith('CONSTRUCT') ||
    trimmed.startsWith('DESCRIBE') ||
    trimmed.startsWith('PREFIX')
  ) {
    return 'sparql'
  }

  // Check for entity ID (URL format)
  if (query.startsWith('http://') || query.startsWith('https://')) {
    return 'traverse'
  }

  // Default to text search
  return 'text'
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
 * Execute a SPARQL query
 */
async function executeSparqlQuery(
  env: GraphDBEnv,
  authContext: AuthContext,
  query: string,
  limit: number
): Promise<unknown> {
  const brokerId = env.BROKER.idFromName('default')
  const brokerStub = env.BROKER.get(brokerId)

  const response = await brokerStub.fetch(
    new Request('https://internal/query', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Auth-Context': JSON.stringify(authContext),
      },
      body: JSON.stringify({
        sparql: query,
        limit,
      }),
    })
  )

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`SPARQL query failed: ${error}`)
  }

  return response.json()
}

/**
 * Execute a text search
 */
async function executeTextSearch(
  env: GraphDBEnv,
  authContext: AuthContext,
  query: string,
  limit: number,
  offset: number
): Promise<unknown> {
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
        query,
        limit,
        offset,
      }),
    })
  )

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`Text search failed: ${error}`)
  }

  return response.json()
}

/**
 * Execute a graph traversal
 */
async function executeTraversal(
  env: GraphDBEnv,
  authContext: AuthContext,
  startId: string,
  limit: number
): Promise<unknown> {
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
        maxDepth: 2,
        limit,
        direction: 'outgoing',
      }),
    })
  )

  if (!response.ok) {
    const error = await response.text()
    throw new Error(`Traversal failed: ${error}`)
  }

  return response.json()
}

/**
 * Create a search handler for the MCP server
 *
 * @param env - GraphDB environment bindings
 * @param mcpAuth - MCP authentication context
 * @returns Handler function for the search tool
 */
export function createSearchHandler(
  env: GraphDBEnv,
  mcpAuth: MCPAuthContext
): (input: SearchInput) => Promise<ToolResponse> {
  const authContext = mapAuthContext(mcpAuth)

  return async (input: SearchInput): Promise<ToolResponse> => {
    try {
      // Check read permission
      const permissionCheck = checkPermission(authContext, 'read')
      if (!permissionCheck.allowed) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({ error: 'Permission denied: read access required' }),
            },
          ],
          isError: true,
        }
      }

      const queryType = input.type ?? detectQueryType(input.query)
      const limit = input.limit ?? 20
      const offset = input.offset ?? 0

      let result: unknown

      switch (queryType) {
        case 'sparql':
          result = await executeSparqlQuery(env, authContext, input.query, limit)
          break
        case 'traverse':
          result = await executeTraversal(env, authContext, input.query, limit)
          break
        case 'text':
        default:
          result = await executeTextSearch(env, authContext, input.query, limit, offset)
          break
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2),
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
