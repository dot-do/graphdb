# @dotdo/graphdb

Cost-optimized graph database for Cloudflare Workers. Leverages Cloudflare's global edge network with hibernating WebSocket connections (95% cost reduction), SQLite-backed Durable Objects, and R2 lakehouse storage.

## Installation

```bash
npm install @dotdo/graphdb
```

Peer dependencies:

```bash
npm install @cloudflare/workers-types
```

## Quick Start

### 1. Configure Wrangler

```jsonc
// wrangler.jsonc
{
  "name": "my-graph-app",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-15",
  "durable_objects": {
    "bindings": [
      { "name": "BROKER", "class_name": "BrokerDO" },
      { "name": "SHARD", "class_name": "ShardDO" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_sqlite_classes": ["ShardDO"], "new_classes": ["BrokerDO"] }
  ],
  "r2_buckets": [
    { "binding": "LAKEHOUSE", "bucket_name": "my-lakehouse" }
  ]
}
```

### 2. Export Durable Objects

```typescript
// src/index.ts
export { BrokerDO, ShardDO } from '@dotdo/graphdb';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket connection for real-time queries
    if (url.pathname === '/connect') {
      const id = env.BROKER.idFromName('default');
      return env.BROKER.get(id).fetch(request);
    }

    // HTTP API for shard operations
    if (url.pathname.startsWith('/shard/')) {
      const shardId = url.pathname.split('/')[2];
      const id = env.SHARD.idFromName(shardId);
      return env.SHARD.get(id).fetch(request);
    }

    return new Response('Not Found', { status: 404 });
  }
};
```

### 3. Connect via WebSocket

```typescript
import { createGraphClient } from '@dotdo/graphdb/protocol';

const client = await createGraphClient('wss://my-graph-app.workers.dev/connect');

// Insert triples
await client.insert([
  { subject: 'user:1', predicate: 'name', object: { type: 'STRING', value: 'Alice' } },
  { subject: 'user:1', predicate: 'follows', object: { type: 'REF', value: 'user:2' } }
]);

// Query entities
const user = await client.get('user:1');

// Traverse relationships
const followers = await client.traverse('user:1', { direction: 'in', predicate: 'follows' });
```

## Architecture

```
Client (capnweb) --> Snippet (FREE) --> Edge Cache (FREE) --> DO (paid if needed)
```

### Request Flow

```
+-------------------------------------------------------------------+
| SNIPPET (FREE) - 5ms budget, 32KB limit                           |
| - Bloom check: entity exists? (98% reject non-existent)           |
| - Parse query: user:123.friends.posts                             |
| - Route to shard: namespace -> DO stub                            |
+-------------------------------------------------------------------+
                              |
              +---------------+---------------+
              v                               v
+----------------------+      +-----------------------------------+
| EDGE CACHE (FREE)    |      | BROKER DO (Hibernating)           |
| - Index segments     |      | - WS termination + 95% discount   |
| - Geohash cells      |      | - 1000 subrequest quota per msg   |
| - Bloom filters      |      | - capnweb promise pipelining      |
+----------------------+      | - Multi-hop traversal batching    |
                              +-----------------------------------+
                                              |
                                              v
+-------------------------------------------------------------------+
| SHARD DO (per namespace)                                          |
| - SQLite: subject | predicate | object | type | timestamp         |
| - Typed columns: REF, STRING, INT64, FLOAT64, GEO_POINT...        |
| - Indexes: SPO (forward), POS (predicate), OSP (reverse)          |
+-------------------------------------------------------------------+
                              |
                              | Streaming WAL (WS, FREE)
                              v
+-------------------------------------------------------------------+
| REGION COORDINATOR DO --> R2 LAKEHOUSE                            |
| - Buffer CDC, flush GraphCol format                               |
| - URL hierarchy: .com/.example/crm/acme/                          |
| - Tiered compaction: L1 (8MB) -> L2 (128MB)                       |
+-------------------------------------------------------------------+
```

### Components

| Component | Cost | Role |
|-----------|------|------|
| **Snippets** | FREE | Bloom filter routing, query parsing, shard selection (32KB/5ms limit) |
| **Edge Cache** | FREE | Index segments, geohash cells, posting lists (Cache API) |
| **Broker DO** | 95% discount | Hibernating WS, query orchestration, capnweb RPC |
| **Shard DO** | Standard DO | SQLite triples, typed columns, SPO/POS/OSP indexes |
| **R2 Lakehouse** | $0.015/GB | CDC streaming, GraphCol format, tiered compaction |

## Data Model

GraphDB uses URL-based entity identifiers with typed object columns:

```typescript
interface Entity {
  $id: string;      // "https://example.com/crm/acme/customer/123"
  $type: string;    // "Person"
  $context: string; // "https://example.com/crm/acme/"

  // JS/TS native field names (no colons)
  name: string;
  email: string;
  accountManager: string;  // REF -> another $id
  geo: { lat: number; lng: number };  // GEO_POINT
  visits: string[];  // REF_ARRAY
}
```

### Object Types

| Type | Column | Encoding | Index |
|------|--------|----------|-------|
| REF | `obj_ref` | Dictionary | Hash (OSP) |
| STRING | `obj_string` | Dict + ZSTD | FTS + Hash |
| INT64 | `obj_int64` | Delta | B-tree |
| FLOAT64 | `obj_float64` | Byte-split | B-tree |
| GEO_POINT | `obj_lat, obj_lng` | Plain | Geohash |
| TIMESTAMP | `obj_timestamp` | Delta | B-tree |

### Query Language

```
user:123.friends                     # Single hop
user:123.friends.posts               # Multi-hop
user:123.friends[?age > 30]          # Filter
post:456 <- likes                    # Reverse traversal
user:123.friends*[depth <= 3]        # Bounded recursion
```

## API Reference

### Core Types

```typescript
import {
  // Types
  Triple, TypedObject, EntityId, Predicate,
  ObjectType, GeoPoint, GeoPolygon,

  // Type guards
  isEntityId, isPredicate, isValidTypedObject,

  // Factories
  createEntityId, createTriple, createEntity,

  // Converters
  typedObjectToJson, jsonToTypedObject,
} from '@dotdo/graphdb/core';
```

### Broker DO (WebSocket)

```typescript
import { BrokerDO, planQuery, orchestrateQuery } from '@dotdo/graphdb/broker';

// Query planning
const plan = planQuery('user:1.follows[*].name');
const results = await orchestrateQuery(plan, shardStubs);
```

### Shard DO (SQLite Storage)

```typescript
import {
  ShardDO, createTripleStore, createChunkStore,
  initializeSchema, TRIPLES_SCHEMA,
} from '@dotdo/graphdb/shard';

// Initialize schema in DO
initializeSchema(sql);

// Triple operations
const store = createTripleStore(sql);
await store.insert(triple);
const results = await store.query({ subject: 'user:1' });
```

### Query Module

```typescript
import {
  // Parser
  parse, stringify, ParseError,

  // Planner
  planQuery, optimizePlan, estimateCost,

  // Executor
  executePlan, traverseFrom, traverseTo,

  // Materializer
  materializeTriples, groupBySubject, expandRefs,
} from '@dotdo/graphdb/query';

// Parse a path query
const ast = parse('user:1.follows[*].posts[?published=true]');

// Plan and execute
const plan = planQuery(ast);
const results = await executePlan(plan, context);
```

### Index Module

```typescript
import {
  // Triple indexes
  querySPO, queryPOS, queryOSP,

  // Full-text search
  initializeFTS, searchFTS, FTS_SCHEMA,

  // Geospatial
  initializeGeoIndex, queryGeoRadius, queryGeoBBox,

  // Combined index file format
  encodeCombinedIndex, decodeCombinedIndex,
} from '@dotdo/graphdb/index';

// FTS query
const results = await searchFTS(sql, { query: 'machine learning', limit: 10 });

// Geo query (radius search)
const nearby = await queryGeoRadius(sql, { lat: 37.7749, lng: -122.4194, radiusKm: 5 });
```

### Snippet Layer (Edge Routing)

```typescript
import {
  // Bloom filter
  createBloomFilter, mightExist, serializeFilter,

  // Query lexer
  tokenize, TokenType,

  // Routing
  routeEntity, routeQuery, getShardId,

  // Edge cache
  EdgeCache, createEdgeCacheKey,
} from '@dotdo/graphdb/snippet';

// Check bloom filter before shard lookup
const filter = deserializeFilter(cachedFilter);
if (mightExist(filter, entityId)) {
  // Route to shard
}
```

### Storage Module (R2/GraphCol)

```typescript
import {
  // GraphCol format
  encodeGraphCol, decodeGraphCol, createEncoder,

  // R2 CDC writer
  createR2Writer, getCDCPath, listCDCFiles,

  // Compaction
  compactChunks, selectChunksForCompaction,
} from '@dotdo/graphdb/storage';

// Write CDC file
const writer = createR2Writer(r2Bucket, { namespace: 'users' });
await writer.write(triples);

// Compact chunks
await compactChunks(r2Bucket, { level: 'L1', namespace: 'users' });
```

### Protocol (capnweb RPC)

```typescript
import {
  // Client
  createGraphClient, GraphClient,

  // Server target
  GraphAPITarget,

  // Query execution
  executeQuery, parseQueryString,
} from '@dotdo/graphdb/protocol';

// Create RPC client
const client = await createGraphClient('wss://api.example.com/connect');
await client.batch([
  { op: 'insert', triple },
  { op: 'delete', subject: 'user:old' },
]);
```

## Configuration

### Environment Bindings

```typescript
interface Env {
  BROKER: DurableObjectNamespace;      // Broker DO
  SHARD: DurableObjectNamespace;       // Shard DO
  COORDINATOR: DurableObjectNamespace; // Optional: query coordinator
  TRAVERSAL_DO: DurableObjectNamespace;// Optional: traversal optimizer
  LAKEHOUSE: R2Bucket;                 // R2 storage
  CACHE_META: KVNamespace;             // Optional: cache metadata
}
```

### Shard Configuration

```typescript
const shardConfig = {
  // Triple storage
  targetBufferSize: 64 * 1024,        // 64KB chunks
  minChunkSize: 16 * 1024,            // 16KB minimum for compaction

  // Indexes
  enableFTS: true,                     // Full-text search
  enableGeo: true,                     // Geospatial indexes

  // Compaction
  compactionThreshold: 10,             // Chunks before compaction
  hotRowAgeMs: 5 * 60 * 1000,         // 5 minutes hot window
};
```

### Rate Limiting

```typescript
import { createRateLimiter } from '@dotdo/graphdb/security';

const limiter = createRateLimiter({
  windowMs: 60_000,         // 1 minute window
  maxRequests: 100,         // 100 requests per window
  keyPrefix: 'graphdb:',
});
```

## Performance Characteristics

Live benchmarks from https://graphdb.workers.do (workers.do enterprise zone).

### Latency Metrics

| Operation | p50 | p95 | p99 |
|-----------|-----|-----|-----|
| Point Lookup (cached) | < 5ms | 10ms | 15ms |
| Point Lookup (shard) | 21ms | 30ms | 36ms |
| 1-Hop Traversal | 46ms | 57ms | 69ms |
| 3-Hop Traversal | 101ms | 128ms | 154ms |
| FTS Query | 50-200ms | - | - |
| Geo Radius (5km) | 30-100ms | - | - |

### Throughput Metrics

| Operation | Rate |
|-----------|------|
| Write Throughput | 4,274 ops/sec |
| Seed Rate | 11,191 triples/sec |
| Bulk Insert | 10K triples/sec |

### Cache Efficiency

| Metric | Value |
|--------|-------|
| Edge Cache Hit Rate | 81.4% |
| Bloom Filter Efficiency | 46.2% |

### Cost Optimization Tips

1. **Use Hibernating WebSockets**: 95% cost reduction vs active connections
2. **Cache Bloom Filters**: Route only to shards that might contain data
3. **Batch Operations**: Combine multiple operations per wake cycle
4. **Leverage Edge Cache**: Cache index segments and query results
5. **Compact Regularly**: Reduce R2 storage and improve read performance

## Security

### Authentication

```typescript
import {
  validateApiKey, validateJwt, createAuthContext,
  AuthError, AuthErrorCode,
} from '@dotdo/graphdb/security';

// API key validation
const auth = await validateApiKey(request, {
  keys: [{ key: 'sk_...', permissions: ['read', 'write'] }],
});

// JWT validation
const auth = await validateJwt(request, {
  issuer: 'https://auth.example.com',
  audience: 'graphdb',
});
```

### Permissions

```typescript
import {
  checkPermission, checkNamespaceAccess, checkEntityAccess,
  hasReadPermission, hasWritePermission,
} from '@dotdo/graphdb/security';

// Check namespace access
const allowed = checkNamespaceAccess(authContext, 'users', 'write');

// Check entity-level ACL
const result = checkEntityAccess(authContext, entityId, 'read');
if (isDenied(result)) {
  return forbiddenResponse(result.reason);
}
```

### Input Validation

```typescript
import {
  sanitizeFtsQuery, validateEntityId,
  safeJsonParse, JsonParseError,
} from '@dotdo/graphdb/security';

// Sanitize FTS queries (prevent injection)
const safeQuery = sanitizeFtsQuery(userInput);

// Validate entity IDs
const result = validateEntityId(input);
if (!result.valid) {
  throw new EntityIdValidationError(result.error);
}

// Safe JSON parsing with limits
const data = safeJsonParse(body, {
  maxSize: 1024 * 1024,  // 1MB
  maxDepth: 10,
  maxKeys: 1000,
});
```

## HTTP Endpoints

When deployed, the worker exposes these endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/connect` | GET (WS) | WebSocket upgrade for real-time queries |
| `/connect/:brokerId` | GET (WS) | Connect to specific broker instance |
| `/broker/:id/metrics` | GET | Broker metrics and stats |
| `/broker/:id/state` | GET | Broker state value |
| `/broker/:id/reset` | POST | Reset broker state and metrics |
| `/shard/:id/stats` | GET | Shard statistics |
| `/shard/:id/count` | GET | Triple count |
| `/health` | GET | Health check |
| `/bench` | GET | Benchmark endpoint (jsonbench-compatible) |
| `/benchmark/scenarios` | GET | List available benchmark scenarios |
| `/benchmark/results` | GET | Get benchmark results |
| `/benchmark/seed` | POST | Seed test data |
| `/benchmark/run/:scenario` | POST | Run specific benchmark |
| `/benchmark/run-all` | POST | Run all benchmarks |
| `/benchmark/reset` | DELETE | Reset benchmark data |

### WebSocket Protocol

```typescript
// Client messages
{ "type": "ping", "timestamp": number }
{ "type": "setState", "value": number }
{ "type": "getState" }
{ "subrequests": number, "messageId": number }

// Server messages
{ "type": "connected", "clientId": string }
{ "type": "pong", "timestamp": number, "serverTime": number }
{ "type": "stateSet", "value": number }
{ "type": "state", "value": number }
{ "type": "subrequestResult", "result": {...}, "metrics": {...} }
```

## Package Structure

```
packages/graphdb/
├── src/
│   ├── core/           # Types, utilities, branded IDs
│   ├── snippet/        # Bloom filter, query parser, router
│   ├── broker/         # Hibernating WS DO, capnweb handler
│   ├── shard/          # Graph shard DO, SQLite triples
│   ├── coordinator/    # Query coordinator DO
│   ├── traversal/      # Region-optimized traversal DO
│   ├── storage/        # GraphCol format, R2 writer, compaction
│   ├── index/          # SPO, POS, OSP, FTS, Geo indexes
│   ├── query/          # Query parser, planner, executor
│   ├── protocol/       # capnweb RPC definitions
│   ├── cache/          # Edge cache integration
│   ├── import/         # Streaming data ingestion
│   ├── security/       # Auth, permissions, validation
│   └── benchmark/      # Performance benchmarking
├── test/
├── package.json
└── wrangler.jsonc
```

## Benchmarks

### Run Benchmarks

```bash
# Deploy to production
npm run deploy

# Run benchmarks via curl
curl -X POST "https://graphdb.workers.do/benchmark/seed?dataset=small"
curl -X POST "https://graphdb.workers.do/benchmark/run/point-lookup?iterations=100"
curl https://graphdb.workers.do/benchmark/results
```

### Available Scenarios

- `point-lookup` - Single entity fetch by ID
- `traversal-1hop` - Entity + direct relationships
- `traversal-3hop` - Deep graph traversal (3 hops)
- `write-throughput` - Batch insert performance
- `bloom-filter-hit-rate` - Routing efficiency
- `edge-cache-hit-rate` - Cache hit distribution

### Dataset Sizes

| Dataset | Entities | Est. Triples | Size |
|---------|----------|--------------|------|
| tiny | 100 | 550 | 50KB |
| small | 1,000 | 9,200 | 1MB |
| medium | 10,000 | 120,000 | 10MB |
| onet | 100,000 | 1,250,000 | 100MB |
| imdb | 1,000,000 | 13,000,000 | 1GB |

## Development

```bash
# Install dependencies
npm install

# Run tests
npm test

# Type check
npm run typecheck

# Local development
npm run dev

# Deploy
npm run deploy
```

## Validated Assumptions (P0 Spikes)

- [x] WS hibernation: 95% cost savings, quota resets per message
- [x] Bloom snippets: 98% negative lookup rejection, <1us latency

## Roadmap

### Current Version (v0.1.x)

The following features are fully implemented and production-ready:

| Component | Status | Description |
|-----------|--------|-------------|
| **ShardDO** | Implemented | SQLite-backed triple storage with typed columns |
| **BrokerDO** | Implemented | Hibernating WebSocket with capnweb RPC |
| **Snippet Layer** | Implemented | Bloom filter routing, query parsing |
| **Index Module** | Implemented | SPO, POS, OSP, FTS, and Geo indexes |
| **R2 Storage** | Implemented | GraphCol format, CDC streaming, compaction |

### Future Features (v0.2.0+)

#### Cross-Shard Query Coordination

The `CoordinatorDO` is currently a stub that returns `NOT_IMPLEMENTED`. When implemented, it will provide:

- **Query Planning & Optimization**: Parse queries to determine which shards contain relevant data, generate optimal execution plans
- **Shard Selection & Routing**: Use bloom filter metadata to route query fragments to appropriate shards in parallel
- **Result Aggregation**: Collect, merge, and deduplicate results from multiple shards
- **Distributed Transactions**: Two-phase commit (2PC) for multi-shard write operations

**Current Workarounds:**
- For single-shard queries: Use `ShardDO` directly via `env.SHARD.get(id).fetch()`
- For multi-hop traversals: Use `BrokerDO` with WebSocket connections for query orchestration

#### Region-Optimized Traversal

The `TraversalDO` will optimize graph traversals by:
- Caching frequently-accessed subgraphs at the edge
- Pre-computing common traversal patterns
- Optimizing for geo-distributed access patterns

### Contributing

If cross-shard coordination is a priority for your use case, please open an issue describing your requirements. This helps us prioritize the roadmap.

## License

MIT
