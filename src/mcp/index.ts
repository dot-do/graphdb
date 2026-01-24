/**
 * GraphDB MCP Server Module
 *
 * Provides an MCP (Model Context Protocol) server for GraphDB with three tools:
 * - search: Query the knowledge graph (SPARQL, text, traversal)
 * - fetch: Retrieve entities by ID
 * - do: Execute code with `graph` binding
 *
 * @example
 * ```typescript
 * import { createGraphDBMCPServer } from '@dotdo/graphdb/mcp';
 *
 * // Create MCP server from environment
 * const server = createGraphDBMCPServer({
 *   env,
 *   auth: { mode: 'anon+auth' },
 *   timeout: 5000,
 * });
 *
 * // Get tool definitions
 * const tools = server.getToolDefinitions();
 *
 * // Call a tool
 * const result = await server.callTool('search', {
 *   query: 'machine learning',
 * }, authContext);
 *
 * // Handle MCP protocol request
 * const response = await server.handleRequest({
 *   id: '1',
 *   method: 'tools/call',
 *   params: {
 *     name: 'fetch',
 *     arguments: { resource: 'https://example.com/entity/123' },
 *   },
 * });
 * ```
 *
 * @packageDocumentation
 */

// Server exports
export {
  createGraphDBMCPServer,
  createMCPConfig,
  type GraphDBMCPServer,
  type MCPRequest,
  type MCPResponse,
} from './server.js'

// Types
export type {
  GraphBinding,
  GraphDBMCPConfig,
  GraphDBEnv,
  MCPAuthContext,
  ToolResponse,
  SearchInput,
  FetchInput,
  DoInput,
  TraverseOptions,
  SearchOptions,
  VectorSearchOptions,
  TextSearchResult,
  VectorSearchResult,
  SPARQLResult,
} from './types.js'

// Graph binding
export {
  createGraphBinding,
  GRAPH_BINDING_TYPES,
} from './graph-binding.js'

// Tools
export {
  searchTool,
  createSearchHandler,
  fetchTool,
  createFetchHandler,
  doTool,
  createDoHandler,
  getDoToolTypes,
  type DoResult,
} from './tools/index.js'
