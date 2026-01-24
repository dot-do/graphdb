/**
 * MCP Tools Module
 *
 * Exports the three core MCP tools for GraphDB:
 * - search: Query the knowledge graph (SPARQL, text, traversal)
 * - fetch: Retrieve entities by ID
 * - do: Execute code with graph binding
 */

// Search tool
export {
  searchTool,
  createSearchHandler,
} from './search.js'

// Fetch tool
export {
  fetchTool,
  createFetchHandler,
} from './fetch.js'

// Do tool
export {
  doTool,
  createDoHandler,
  getDoToolTypes,
  type DoResult,
} from './do.js'
