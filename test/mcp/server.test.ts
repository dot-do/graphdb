/**
 * MCP Server Tests
 *
 * Tests for the GraphDB MCP server implementation.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  createGraphDBMCPServer,
  createMCPConfig,
  searchTool,
  fetchTool,
  doTool,
  GRAPH_BINDING_TYPES,
} from '../../src/mcp/index.js'
import type { GraphDBEnv, MCPAuthContext } from '../../src/mcp/types.js'

// Mock environment
function createMockEnv(): GraphDBEnv {
  const mockStub = {
    fetch: vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ entities: [], bindings: [], results: [] }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      })
    ),
  }

  return {
    BROKER: {
      idFromName: vi.fn().mockReturnValue({ toString: () => 'broker-id' }),
      get: vi.fn().mockReturnValue(mockStub),
    } as unknown as DurableObjectNamespace,
    SHARD: {
      idFromName: vi.fn().mockReturnValue({ toString: () => 'shard-id' }),
      get: vi.fn().mockReturnValue(mockStub),
    } as unknown as DurableObjectNamespace,
  }
}

// Mock auth context
const mockAuthContext: MCPAuthContext = {
  type: 'apikey',
  id: 'test-user',
  readonly: false,
}

const readonlyAuthContext: MCPAuthContext = {
  type: 'anon',
  id: 'anonymous',
  readonly: true,
}

describe('GraphDB MCP Server', () => {
  describe('createGraphDBMCPServer', () => {
    it('should create a server with three tools', () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      expect(server.tools).toBeDefined()
      expect(server.tools.search).toBe(searchTool)
      expect(server.tools.fetch).toBe(fetchTool)
      expect(server.tools.do).toBe(doTool)
    })

    it('should return tool definitions', () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const definitions = server.getToolDefinitions()

      expect(definitions).toHaveLength(3)
      expect(definitions.map((d) => d.name)).toEqual(['search', 'fetch', 'do'])
    })

    it('should return do tool types', () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const types = server.getDoTypes()

      expect(types).toBe(GRAPH_BINDING_TYPES)
      expect(types).toContain('graph')
      expect(types).toContain('query')
      expect(types).toContain('traverse')
    })
  })

  describe('handleRequest', () => {
    it('should handle initialize request', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '1',
        method: 'initialize',
      })

      expect(response.id).toBe('1')
      expect(response.result).toBeDefined()
      expect((response.result as Record<string, unknown>).protocolVersion).toBe('2024-11-05')
    })

    it('should handle tools/list request', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '2',
        method: 'tools/list',
      })

      expect(response.id).toBe('2')
      expect(response.result).toBeDefined()
      const result = response.result as { tools: Array<{ name: string }> }
      expect(result.tools).toHaveLength(3)
    })

    it('should handle ping request', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '3',
        method: 'ping',
      })

      expect(response.id).toBe('3')
      expect(response.result).toEqual({})
    })

    it('should return error for unknown method', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '4',
        method: 'unknown/method',
      })

      expect(response.id).toBe('4')
      expect(response.error).toBeDefined()
      expect(response.error?.code).toBe(-32601)
    })

    it('should handle tools/call request', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '5',
        method: 'tools/call',
        params: {
          name: 'search',
          arguments: { query: 'test query' },
          authContext: mockAuthContext,
        },
      })

      expect(response.id).toBe('5')
      expect(response.result).toBeDefined()
    })

    it('should return error for missing tool name', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '6',
        method: 'tools/call',
        params: {
          arguments: { query: 'test' },
        },
      })

      expect(response.id).toBe('6')
      expect(response.error).toBeDefined()
      expect(response.error?.code).toBe(-32602)
    })

    it('should return error for unknown tool', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const response = await server.handleRequest({
        id: '7',
        method: 'tools/call',
        params: {
          name: 'unknown',
          arguments: {},
        },
      })

      expect(response.id).toBe('7')
      expect(response.error).toBeDefined()
      expect(response.error?.code).toBe(-32601)
    })
  })

  describe('callTool', () => {
    it('should call search tool', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const result = await server.callTool(
        'search',
        { query: 'test query' },
        mockAuthContext
      )

      expect(result.content).toBeDefined()
      expect(result.content[0]?.type).toBe('text')
    })

    it('should call fetch tool', async () => {
      const env = createMockEnv()
      const mockStub = {
        fetch: vi.fn().mockResolvedValue(
          new Response(JSON.stringify({ $id: 'test', $type: 'Entity' }), {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          })
        ),
      }
      env.SHARD.get = vi.fn().mockReturnValue(mockStub)

      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const result = await server.callTool(
        'fetch',
        { resource: 'https://example.com/entity/123' },
        mockAuthContext
      )

      expect(result.content).toBeDefined()
      expect(result.content[0]?.type).toBe('text')
    })

    it('should call do tool', async () => {
      const env = createMockEnv()
      const server = createGraphDBMCPServer({ env, timeout: 5000 })

      const result = await server.callTool(
        'do',
        { code: 'return 1 + 1' },
        mockAuthContext
      )

      expect(result.content).toBeDefined()
      expect(result.content[0]?.type).toBe('text')
    })
  })

  describe('createMCPConfig', () => {
    it('should create config with anonymous mode when no auth secret', () => {
      const env = createMockEnv()
      const config = createMCPConfig(env)

      expect(config.env).toBe(env)
      expect(config.auth?.mode).toBe('anon')
      expect(config.timeout).toBe(5000)
    })

    it('should create config with anon+auth mode when auth secret exists', () => {
      const env = { ...createMockEnv(), AUTH_SECRET: 'secret' }
      const config = createMCPConfig(env)

      expect(config.auth?.mode).toBe('anon+auth')
    })
  })
})

describe('Tool Definitions', () => {
  describe('searchTool', () => {
    it('should have correct schema', () => {
      expect(searchTool.name).toBe('search')
      expect(searchTool.inputSchema.required).toContain('query')
      expect(searchTool.inputSchema.properties.query.type).toBe('string')
    })
  })

  describe('fetchTool', () => {
    it('should have correct schema', () => {
      expect(fetchTool.name).toBe('fetch')
      expect(fetchTool.inputSchema.required).toContain('resource')
      expect(fetchTool.inputSchema.properties.resource.type).toBe('string')
    })
  })

  describe('doTool', () => {
    it('should have correct schema', () => {
      expect(doTool.name).toBe('do')
      expect(doTool.inputSchema.required).toContain('code')
      expect(doTool.inputSchema.properties.code.type).toBe('string')
    })
  })
})

describe('Graph Binding Types', () => {
  it('should include all graph methods', () => {
    expect(GRAPH_BINDING_TYPES).toContain('query(sparql: string)')
    expect(GRAPH_BINDING_TYPES).toContain('addTriple')
    expect(GRAPH_BINDING_TYPES).toContain('removeTriple')
    expect(GRAPH_BINDING_TYPES).toContain('getNode')
    expect(GRAPH_BINDING_TYPES).toContain('traverse')
    expect(GRAPH_BINDING_TYPES).toContain('search')
    expect(GRAPH_BINDING_TYPES).toContain('vectorSearch')
  })

  it('should include option types', () => {
    expect(GRAPH_BINDING_TYPES).toContain('TraverseOptions')
    expect(GRAPH_BINDING_TYPES).toContain('SearchOptions')
    expect(GRAPH_BINDING_TYPES).toContain('VectorSearchOptions')
  })

  it('should include result types', () => {
    expect(GRAPH_BINDING_TYPES).toContain('SPARQLResult')
    expect(GRAPH_BINDING_TYPES).toContain('TextSearchResult')
    expect(GRAPH_BINDING_TYPES).toContain('VectorSearchResult')
    expect(GRAPH_BINDING_TYPES).toContain('Entity')
  })
})
