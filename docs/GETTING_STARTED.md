# Getting Started with @dotdo/graphdb

A cost-optimized graph database for Cloudflare Workers.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Basic Operations](#basic-operations)
- [Query Language](#query-language)
- [WebSocket Connection](#websocket-connection)
- [Advanced Usage](#advanced-usage)

## Installation

```bash
npm install @dotdo/graphdb
```

### Peer Dependencies

```bash
npm install @cloudflare/workers-types
```

## Quick Start

### 1. Configure wrangler.jsonc

Create a `wrangler.jsonc` file in your project root:

```jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-graphdb-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-15",
  "compatibility_flags": ["nodejs_compat_v2"],

  "durable_objects": {
    "bindings": [
      { "name": "BROKER", "class_name": "BrokerDO" },
      { "name": "SHARD", "class_name": "ShardDO" },
      { "name": "COORDINATOR", "class_name": "CoordinatorDO" }
    ]
  },

  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["ShardDO", "CoordinatorDO"],
      "new_classes": ["BrokerDO"]
    }
  ],

  "r2_buckets": [
    { "binding": "LAKEHOUSE", "bucket_name": "my-graphdb-lakehouse" }
  ],

  "kv_namespaces": [
    { "binding": "CACHE_META", "id": "your-kv-namespace-id" }
  ]
}
```

### 2. Create Your Worker

```typescript
// src/index.ts
import { BrokerDO, ShardDO, CoordinatorDO } from '@dotdo/graphdb';

// Re-export Durable Object classes for Wrangler
export { BrokerDO, ShardDO, CoordinatorDO };

export interface Env {
  BROKER: DurableObjectNamespace;
  SHARD: DurableObjectNamespace;
  COORDINATOR: DurableObjectNamespace;
  LAKEHOUSE: R2Bucket;
  CACHE_META: KVNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket connection for real-time queries
    if (url.pathname === '/connect') {
      const id = env.BROKER.idFromName('default');
      return env.BROKER.get(id).fetch(request);
    }

    // Direct shard access for CRUD operations
    if (url.pathname.startsWith('/api/')) {
      const id = env.SHARD.idFromName('shard-1');
      return env.SHARD.get(id).fetch(request);
    }

    return new Response('GraphDB API', { status: 200 });
  },
};
```

### 3. Run Locally

```bash
npm run dev  # or: npx wrangler dev
```

### 4. Deploy

```bash
npm run deploy  # or: npx wrangler deploy
```

## Configuration

### Environment Bindings

| Binding | Type | Description |
|---------|------|-------------|
| `BROKER` | DurableObjectNamespace | Hibernating WebSocket broker (95% cost discount) |
| `SHARD` | DurableObjectNamespace | SQLite-backed triple storage |
| `COORDINATOR` | DurableObjectNamespace | Query planning and coordination |
| `LAKEHOUSE` | R2Bucket | Cold storage for CDC and compaction |
| `CACHE_META` | KVNamespace | Edge cache metadata |

### Durable Object Classes

- **BrokerDO**: Handles WebSocket connections with hibernation support for cost-effective long-lived connections.
- **ShardDO**: Stores graph triples in SQLite with typed object columns and SPO/POS/OSP indexes.
- **CoordinatorDO**: Orchestrates multi-shard queries and CDC streaming to R2.

## Basic Operations

### Entity Model

Entities use URL-based identifiers and JS-native field names (no colons):

```typescript
interface Entity {
  $id: string;      // "https://example.com/user/123"
  $type: string;    // "User"
  $context: string; // "https://example.com/user/"

  // Properties - use JS field names, NOT RDF prefixes
  name: string;           // Correct
  email: string;          // Correct
  // schema:name: string; // WRONG - no colons allowed
}
```

### Create Entity

```typescript
// POST /api/triples
const response = await fetch('https://your-worker.dev/api/triples', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    subject: 'https://example.com/user/123',
    predicate: 'name',
    object: { type: 5, value: 'Alice' },  // STRING type = 5
    timestamp: Date.now(),
    txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV'  // ULID format
  })
});
```

### Batch Insert

```typescript
// POST /api/triples (array)
const response = await fetch('https://your-worker.dev/api/triples', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify([
    {
      subject: 'https://example.com/user/123',
      predicate: 'name',
      object: { type: 5, value: 'Alice' },
      timestamp: Date.now(),
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV'
    },
    {
      subject: 'https://example.com/user/123',
      predicate: 'email',
      object: { type: 5, value: 'alice@example.com' },
      timestamp: Date.now(),
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV'
    }
  ])
});
```

### Read Entity

```typescript
// GET /api/triples/:subject
const response = await fetch(
  'https://your-worker.dev/api/triples/' +
  encodeURIComponent('https://example.com/user/123')
);
const { triples } = await response.json();
```

### Read Specific Property

```typescript
// GET /api/triples/:subject/:predicate
const response = await fetch(
  'https://your-worker.dev/api/triples/' +
  encodeURIComponent('https://example.com/user/123') +
  '/name'
);
const { triple } = await response.json();
```

### Update Entity

```typescript
// PUT /api/triples/:subject/:predicate
const response = await fetch(
  'https://your-worker.dev/api/triples/' +
  encodeURIComponent('https://example.com/user/123') +
  '/name',
  {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      object: { type: 5, value: 'Alice Smith' },
      txId: '01ARZ3NDEKTSV4RRFFQ69G5FAV'
    })
  }
);
```

### Delete Triple

```typescript
// DELETE /api/triples/:subject/:predicate?txId=...
const response = await fetch(
  'https://your-worker.dev/api/triples/' +
  encodeURIComponent('https://example.com/user/123') +
  '/name?txId=01ARZ3NDEKTSV4RRFFQ69G5FAV',
  { method: 'DELETE' }
);
```

### Delete Entity

```typescript
// DELETE /api/entities/:subject?txId=...
const response = await fetch(
  'https://your-worker.dev/api/entities/' +
  encodeURIComponent('https://example.com/user/123') +
  '?txId=01ARZ3NDEKTSV4RRFFQ69G5FAV',
  { method: 'DELETE' }
);
```

### Object Types

Common types: `NULL=0`, `BOOL=1`, `INT64=3`, `FLOAT64=4`, `STRING=5`, `TIMESTAMP=7`, `REF=10`, `JSON=12`, `GEO_POINT=13`. See `src/core/types.ts` for the full list.

## Query Language

GraphDB uses a path-based query language for graph traversals.

### Basic Queries

```
user:123.friends                     # Single hop
user:123.friends.posts               # Multi-hop
user:123.friends[?age > 30]          # Filter
post:456 <- likes                    # Reverse traversal
user:123.friends*[depth <= 3]        # Bounded recursion
```

### JSON Expansion

```
user:123 { friends { name, posts { title } } }
```

### Filter Operators

- Comparison: `=`, `!=`, `>`, `<`, `>=`, `<=`
- Logical: `and`, `or`
- Depth constraint: `depth <= N`

### Query Examples

```typescript
// Using the query endpoint
const response = await fetch('https://your-worker.dev/api/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    query: 'user:123.friends[?age > 25].posts'
  })
});
```

## WebSocket Connection

For real-time queries and efficient batch operations, use WebSocket connections with hibernation support.

### Connect via WebSocket

```typescript
const ws = new WebSocket('wss://your-worker.dev/connect');

ws.onopen = () => {
  console.log('Connected to GraphDB');
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Received:', data);
};
```

### Ping/Pong

```typescript
// Send
ws.send(JSON.stringify({ type: 'ping', timestamp: Date.now() }));

// Receive
{ "type": "pong", "timestamp": 1234567890, "serverTime": 1234567891 }
```

### State Management

```typescript
// Set state
ws.send(JSON.stringify({ type: 'setState', value: 42 }));

// Get state
ws.send(JSON.stringify({ type: 'getState' }));
```

### RPC Calls (capnweb)

```typescript
// Single RPC call
ws.send(JSON.stringify({
  id: 'req-1',
  method: 'getEntity',
  args: ['https://example.com/user/123']
}));

// Batch RPC calls
ws.send(JSON.stringify({
  id: 'batch-1',
  calls: [
    { id: 'call-1', method: 'getEntity', args: ['https://example.com/user/1'] },
    { id: 'call-2', method: 'getEntity', args: ['https://example.com/user/2'] }
  ]
}));
```

### Using the Client SDK

```typescript
import { createGraphClient } from '@dotdo/graphdb/protocol';

const client = createGraphClient('wss://your-worker.dev/connect');

// Create entity
await client.createEntity({
  $id: 'https://example.com/user/123',
  $type: 'User',
  $context: 'https://example.com/user/',
  name: 'Alice',
  email: 'alice@example.com'
});

// Get entity
const user = await client.getEntity('https://example.com/user/123');

// Traverse relationships
const friends = await client.traverse(
  'https://example.com/user/123',
  'friends'
);

// Multi-hop traversal
const posts = await client.pathTraverse(
  'https://example.com/user/123',
  ['friends', 'posts']
);

// Run query
const result = await client.query('user:123.friends[?age > 25].posts');

// Batch operations
const results = await client.batchGet([
  'https://example.com/user/1',
  'https://example.com/user/2',
  'https://example.com/user/3'
]);

// Cleanup
client.close();
```

## Advanced Usage

### Bloom Filter Routing

For high-performance entity existence checks, use bloom filters in snippets:

```typescript
import {
  createBloomFilter,
  addToFilter,
  mightExist,
  serializeFilter
} from '@dotdo/graphdb/snippet';

// Create filter for 10,000 entities with 1% false positive rate
const filter = createBloomFilter({
  capacity: 10000,
  targetFpr: 0.01
});

// Add entities
addToFilter(filter, 'https://example.com/user/123');
addToFilter(filter, 'https://example.com/user/456');

// Check existence (fast, may have false positives)
if (mightExist(filter, 'https://example.com/user/123')) {
  // Entity might exist - check database
}

// Serialize for edge caching
const serialized = serializeFilter(filter);
```

### Chunk-Based Storage

For cost-optimized storage (1KB row costs same as 2MB BLOB on DO):

```typescript
// Chunk compaction is automatic, but can be triggered manually
const response = await fetch('https://your-worker.dev/api/chunks/compact', {
  method: 'POST'
});

// Force compaction
const response = await fetch('https://your-worker.dev/api/chunks/compact?force=true', {
  method: 'POST'
});

// Get chunk stats
const { stats } = await fetch('https://your-worker.dev/api/chunks/stats').then(r => r.json());
```

### Health Monitoring

```typescript
const health = await fetch('https://your-worker.dev/health').then(r => r.json());
const stats = await fetch('https://your-worker.dev/api/stats').then(r => r.json());
```

### Running Benchmarks

```bash
curl -X POST "https://your-worker.dev/benchmark/seed?dataset=small"
curl -X POST "https://your-worker.dev/benchmark/run/point-lookup?iterations=100"
curl "https://your-worker.dev/benchmark/results"
```

Available scenarios: `point-lookup`, `traversal-1hop`, `traversal-3hop`, `write-throughput`, `bloom-filter-hit-rate`, `edge-cache-hit-rate`.

## Cost Optimization

GraphDB is designed for cost efficiency on Cloudflare Workers:

| Layer | Cost | Role |
|-------|------|------|
| **Snippet** | FREE | Bloom filter routing, query parsing |
| **Edge Cache** | FREE | Index segments, geohash cells |
| **Broker DO** | 95% discount | Hibernating WebSocket connections |
| **Shard DO** | Standard | SQLite triples with typed columns |
| **R2 Lakehouse** | $0.015/GB | CDC streaming, cold storage |

### Key Cost-Saving Patterns

1. **WebSocket Hibernation**: Use `/connect` for long-lived connections - 95% cheaper than active connections
2. **Bloom Filters**: Reject 98% of non-existent entity lookups at the edge (FREE)
3. **Edge Caching**: Cache index segments and frequently accessed data
4. **Chunk Storage**: Batch triples into 2MB blobs for storage efficiency

## Next Steps

- Review [README.md](../README.md) for architecture details
- Explore `src/` for implementation details
- Run `npm test` to verify your setup
