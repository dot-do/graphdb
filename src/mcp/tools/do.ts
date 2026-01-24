/**
 * Do Tool
 *
 * MCP tool for executing code in a sandboxed environment with access
 * to the `graph` binding for GraphDB operations.
 */

import type { ToolResponse, DoResult } from '@dotdo/mcp'
import type { DoInput } from '@dotdo/mcp/tools'
import { createGraphBinding, GRAPH_BINDING_TYPES } from '../graph-binding.js'
import type { GraphDBEnv, MCPAuthContext } from '../types.js'

/**
 * Tool definition for the do tool
 */
export const doTool = {
  name: 'do',
  description: `Execute TypeScript/JavaScript code in a sandboxed environment with access to the GraphDB knowledge graph.

Available binding:
- graph: GraphDB API for graph operations

Available graph methods:
- graph.query(sparql): Execute SPARQL queries
- graph.addTriple(subject, predicate, object): Add a triple
- graph.removeTriple(subject, predicate, object): Remove a triple
- graph.getNode(id): Get entity by ID
- graph.traverse(startId, options?): Traverse the graph
- graph.search(text, options?): Full-text search
- graph.vectorSearch(embedding, options?): Vector similarity search

Examples:
- const results = await graph.search("machine learning")
- const entity = await graph.getNode("https://example.com/user/123")
- await graph.addTriple("https://example.com/a", "knows", "https://example.com/b")
- const related = await graph.traverse("https://example.com/a", { maxDepth: 2 })`,
  inputSchema: {
    type: 'object',
    properties: {
      code: {
        type: 'string',
        description: 'TypeScript/JavaScript code to execute',
      },
    },
    required: ['code'],
  },
} as const

/**
 * Result from code execution - uses shared DoResult from @dotdo/mcp
 * { success, value?, logs, error?, duration }
 */
export type { DoResult } from '@dotdo/mcp'

/**
 * Simple sandboxed code execution
 *
 * NOTE: In production, this should use a proper V8 isolate via ai-evaluate.
 * This implementation provides basic sandboxing for development.
 */
async function executeInSandbox(
  code: string,
  bindings: Record<string, unknown>,
  timeout: number
): Promise<DoResult> {
  const startTime = Date.now()
  const logs: Array<{ level: string; message: string; timestamp: number }> = []

  // Create a mock console that captures output
  const mockConsole = {
    log: (...args: unknown[]) => {
      logs.push({ level: 'log', message: args.map(String).join(' '), timestamp: Date.now() })
    },
    error: (...args: unknown[]) => {
      logs.push({ level: 'error', message: args.map(String).join(' '), timestamp: Date.now() })
    },
    warn: (...args: unknown[]) => {
      logs.push({ level: 'warn', message: args.map(String).join(' '), timestamp: Date.now() })
    },
    info: (...args: unknown[]) => {
      logs.push({ level: 'info', message: args.map(String).join(' '), timestamp: Date.now() })
    },
  }

  // Create sandbox context
  const sandbox = {
    ...bindings,
    console: mockConsole,
  }

  try {
    // Wrap code in async function to support top-level await
    const wrappedCode = `
      (async () => {
        ${code}
      })()
    `

    // Create function with sandbox bindings
    const fn = new Function(...Object.keys(sandbox), `return ${wrappedCode}`)

    // Execute with timeout
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error(`Execution timeout after ${timeout}ms`)), timeout)
    })

    const resultPromise = fn(...Object.values(sandbox))

    const result = await Promise.race([resultPromise, timeoutPromise])
    const duration = Date.now() - startTime

    return {
      success: true,
      value: result,
      logs,
      duration,
    }
  } catch (error) {
    const duration = Date.now() - startTime
    const errorMessage = error instanceof Error ? error.message : String(error)

    return {
      success: false,
      error: errorMessage,
      logs,
      duration,
    }
  }
}

/**
 * Create a do handler for the MCP server
 *
 * @param env - GraphDB environment bindings
 * @param mcpAuth - MCP authentication context
 * @param timeout - Execution timeout in milliseconds (default: 5000)
 * @returns Handler function for the do tool
 */
export function createDoHandler(
  env: GraphDBEnv,
  mcpAuth: MCPAuthContext,
  timeout: number = 5000
): (input: DoInput) => Promise<ToolResponse> {
  // Create the graph binding with auth context
  const graph = createGraphBinding(env, mcpAuth)

  return async (input: DoInput): Promise<ToolResponse> => {
    try {
      const result = await executeInSandbox(
        input.code,
        { graph },
        timeout
      )

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2),
          },
        ],
        isError: !result.success,
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

/**
 * Get the type definitions for the do tool
 */
export function getDoToolTypes(): string {
  return GRAPH_BINDING_TYPES
}
