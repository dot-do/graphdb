/**
 * GraphDB MCP Server
 *
 * Creates an MCP server for GraphDB with search, fetch, and do tools.
 * Integrates with GraphDB's multi-layer authentication system.
 */

import type { ToolResponse } from '@dotdo/mcp'
import type { DoInput } from '@dotdo/mcp/tools'
import { GRAPH_BINDING_TYPES } from './graph-binding.js'
import {
  searchTool,
  createSearchHandler,
  fetchTool,
  createFetchHandler,
  doTool,
  createDoHandler,
} from './tools/index.js'
import type {
  GraphDBEnv,
  GraphDBMCPConfig,
  MCPAuthContext,
  SearchInput,
  FetchInput,
} from './types.js'

/**
 * MCP Server instance for GraphDB
 */
export interface GraphDBMCPServer {
  /** Available tool definitions */
  tools: {
    search: typeof searchTool
    fetch: typeof fetchTool
    do: typeof doTool
  }

  /** Call a tool by name */
  callTool(
    name: 'search' | 'fetch' | 'do',
    args: SearchInput | FetchInput | DoInput,
    authContext: MCPAuthContext
  ): Promise<ToolResponse>

  /** Get tool definitions for MCP protocol */
  getToolDefinitions(): Array<{
    name: string
    description: string
    inputSchema: Record<string, unknown>
  }>

  /** Get type definitions for the do tool */
  getDoTypes(): string

  /** Handle an MCP request */
  handleRequest(request: MCPRequest): Promise<MCPResponse>
}

/**
 * MCP Request format
 */
export interface MCPRequest {
  /** Request ID */
  id: string
  /** Method name */
  method: string
  /** Method parameters */
  params?: Record<string, unknown>
}

/**
 * MCP Response format
 */
export interface MCPResponse {
  /** Request ID (matching the request) */
  id: string
  /** Result if successful */
  result?: unknown
  /** Error if failed */
  error?: {
    code: number
    message: string
    data?: unknown
  }
}

/**
 * Default anonymous auth context
 */
const ANONYMOUS_AUTH: MCPAuthContext = {
  type: 'anon',
  id: 'anonymous',
  readonly: true,
}

/**
 * Create a GraphDB MCP server
 *
 * @param config - Server configuration
 * @returns MCP server instance
 */
export function createGraphDBMCPServer(config: GraphDBMCPConfig): GraphDBMCPServer {
  const { env, timeout = 5000 } = config

  /**
   * Create tool handlers for a given auth context
   */
  function createHandlers(authContext: MCPAuthContext) {
    return {
      search: createSearchHandler(env, authContext),
      fetch: createFetchHandler(env, authContext),
      do: createDoHandler(env, authContext, timeout),
    }
  }

  /**
   * Get tool definitions for MCP protocol
   */
  function getToolDefinitions() {
    return [
      {
        name: searchTool.name,
        description: searchTool.description,
        inputSchema: searchTool.inputSchema,
      },
      {
        name: fetchTool.name,
        description: fetchTool.description,
        inputSchema: fetchTool.inputSchema,
      },
      {
        name: doTool.name,
        description: doTool.description + '\n\nType Definitions:\n' + GRAPH_BINDING_TYPES,
        inputSchema: doTool.inputSchema,
      },
    ]
  }

  /**
   * Call a tool by name
   */
  async function callTool(
    name: 'search' | 'fetch' | 'do',
    args: SearchInput | FetchInput | DoInput,
    authContext: MCPAuthContext
  ): Promise<ToolResponse> {
    const handlers = createHandlers(authContext)

    switch (name) {
      case 'search':
        return handlers.search(args as SearchInput)
      case 'fetch':
        return handlers.fetch(args as FetchInput)
      case 'do':
        return handlers.do(args as DoInput)
      default:
        return {
          content: [{ type: 'text', text: JSON.stringify({ error: `Unknown tool: ${name}` }) }],
          isError: true,
        }
    }
  }

  /**
   * Handle an MCP protocol request
   */
  async function handleRequest(request: MCPRequest): Promise<MCPResponse> {
    const { id, method, params } = request

    try {
      switch (method) {
        case 'tools/list': {
          return {
            id,
            result: {
              tools: getToolDefinitions(),
            },
          }
        }

        case 'tools/call': {
          const toolName = params?.['name'] as string
          const toolArgs = params?.['arguments'] as Record<string, unknown>
          const authContext = (params?.['authContext'] as MCPAuthContext) ?? ANONYMOUS_AUTH

          if (!toolName) {
            return {
              id,
              error: {
                code: -32602,
                message: 'Invalid params: tool name required',
              },
            }
          }

          if (toolName !== 'search' && toolName !== 'fetch' && toolName !== 'do') {
            return {
              id,
              error: {
                code: -32601,
                message: `Unknown tool: ${toolName}`,
              },
            }
          }

          const result = await callTool(
            toolName,
            (toolArgs ?? {}) as unknown as SearchInput | FetchInput | DoInput,
            authContext
          )

          return {
            id,
            result,
          }
        }

        case 'initialize': {
          return {
            id,
            result: {
              protocolVersion: '2024-11-05',
              serverInfo: {
                name: 'graphdb-mcp',
                version: '0.1.0',
              },
              capabilities: {
                tools: {},
              },
            },
          }
        }

        case 'ping': {
          return {
            id,
            result: {},
          }
        }

        default: {
          return {
            id,
            error: {
              code: -32601,
              message: `Unknown method: ${method}`,
            },
          }
        }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error'

      return {
        id,
        error: {
          code: -32000,
          message: errorMessage,
        },
      }
    }
  }

  return {
    tools: {
      search: searchTool,
      fetch: fetchTool,
      do: doTool,
    },
    callTool,
    getToolDefinitions,
    getDoTypes: () => GRAPH_BINDING_TYPES,
    handleRequest,
  }
}

/**
 * Create MCP server config from GraphDB environment
 */
export function createMCPConfig(env: GraphDBEnv): GraphDBMCPConfig {
  return {
    env,
    auth: {
      mode: env.AUTH_SECRET ? 'anon+auth' : 'anon',
    },
    timeout: 5000,
  }
}
