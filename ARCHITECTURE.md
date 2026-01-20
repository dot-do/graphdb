# GraphDB Architecture

Cost-optimized graph database built on Cloudflare Workers with a layered architecture designed for minimal cost and maximum performance.

## Layer Overview

```
+------------------+
|     Client       |  WebSocket / HTTP
+--------+---------+
         |
+--------v---------+
|  Snippet Layer   |  FREE: Bloom routing, query parsing, shard routing
|  (Edge Script)   |  Constraints: 32KB, 5ms, 2-5 subrequests, vanilla JS
+--------+---------+
         |
+--------v---------+
|  Broker Layer    |  95% discount: Hibernating WebSocket DO
|  (Durable Object)|  Role: Query orchestration, RPC via capnweb
+--------+---------+
         |
+--------v---------+
|  Shard Layer     |  SQLite storage per namespace
|  (Durable Object)|  Role: Triple CRUD, indexing, chunk management
+--------+---------+
         |
+--------v---------+
|  Storage Layer   |  R2 object storage
|  (R2 Bucket)     |  Role: CDC streaming, GraphCol format, compaction
+------------------+
```

## Layer Responsibilities

### 1. Snippet Layer (`src/snippet/`)

**Cost:** FREE (Cloudflare Snippets)

**Constraints:**
- 32KB max script size
- 5ms max compute time
- 2-5 subrequests per invocation
- Vanilla JavaScript only (no Node.js APIs)

**Responsibilities:**
- Bloom filter routing (probabilistic entity existence check)
- Query lexing and parsing
- Namespace extraction from entity URLs
- Shard ID calculation via consistent hashing (FNV-1a)
- Cache key generation for cacheable queries
- Query cost estimation for rate limiting

**Key Files:**
| File | Purpose |
|------|---------|
| `bloom.ts` | Bloom filter creation, serialization, membership test |
| `router.ts` | Entity/query routing, namespace extraction, shard ID |
| `lexer.ts` | Query tokenization |
| `edge-cache.ts` | Edge cache key management for bloom filters |
| `bloom-router.ts` | Combined bloom filter + routing logic |

**When to use:** Initial request handling, routing decisions, cache lookups.

---

### 2. Broker Layer (`src/broker/`)

**Cost:** 95% discount via WebSocket hibernation ($0.0075 vs $0.15/million)

**Key Pattern:** Hibernating WebSocket
```typescript
// Hibernation enables fresh 1000 subrequest quota per wake
this.ctx.acceptWebSocket(server, ['broker-client']);
server.serializeAttachment(attachment);  // State survives hibernation
```

**Responsibilities:**
- WebSocket connection management with hibernation
- Query orchestration across multiple shards
- capnweb RPC endpoint for GraphAPI
- Response aggregation and validation
- Cursor management for pagination (survives hibernation)
- Metrics tracking (wakes, subrequests, failures)

**Key Files:**
| File | Purpose |
|------|---------|
| `broker-do.ts` | Main Durable Object: WebSocket handling, RPC routing |
| `orchestrator.ts` | Query planning, step execution, batch lookups |
| `edge-cache.ts` | Response caching with namespace-based invalidation |
| `response-validator.ts` | Shard response validation |

**Key Interface - WebSocketAttachment:**
```typescript
interface WebSocketAttachment {
  clientId: string;
  connectedAt: number;
  totalMessagesReceived: number;
  totalSubrequestsMade: number;
  cursors?: Record<string, string>;  // Pagination state
}
```

**When to use:** Query coordination, multi-shard operations, stateful client connections.

---

### 3. Shard Layer (`src/shard/`)

**Cost:** Standard Durable Object pricing

**Storage:** SQLite with typed object columns

**Responsibilities:**
- Triple CRUD operations (subject, predicate, object)
- Schema management and migrations
- Chunk-based BLOB storage (2MB target size)
- Secondary indexes (SPO, POS, OSP, FTS, Geo)
- Orchestrator query endpoints (/lookup, /traverse, /filter)
- WebSocket hibernation support
- Maintenance tasks via alarms

**Key Files:**
| File | Purpose |
|------|---------|
| `shard-do.ts` | Main Durable Object: HTTP endpoints, WebSocket, alarms |
| `schema.ts` | SQLite schema definition, migrations |
| `crud.ts` | Triple store operations (deprecated for new code) |
| `chunk-store.ts` | BLOB-optimized storage with compaction |
| `row-parsers.ts` | Type-safe SQL row parsing |

**Triple Schema:**
```sql
CREATE TABLE triples (
  subject TEXT NOT NULL,      -- Entity URL ($id)
  predicate TEXT NOT NULL,    -- Field name (no colons)
  obj_type INTEGER NOT NULL,  -- ObjectType enum
  obj_ref TEXT,               -- REF type
  obj_string TEXT,            -- STRING type
  obj_int64 INTEGER,          -- INT64 type
  obj_float64 REAL,           -- FLOAT64 type
  obj_lat REAL,               -- GEO_POINT latitude
  obj_lng REAL,               -- GEO_POINT longitude
  obj_timestamp INTEGER,      -- TIMESTAMP type
  timestamp INTEGER NOT NULL, -- When triple was created
  tx_id TEXT NOT NULL         -- Transaction ID
);
```

**When to use:** Data persistence, entity operations, index queries.

---

### 4. Storage Layer (`src/storage/`)

**Cost:** R2 storage pricing

**Format:** GraphCol (columnar graph format)

**Responsibilities:**
- CDC (Change Data Capture) streaming to R2
- GraphCol encoding/decoding for efficient storage
- URL hierarchy path generation for R2 keys
- Tiered compaction of CDC files
- Entity index management

**Key Files:**
| File | Purpose |
|------|---------|
| `r2-writer.ts` | CDC event batching, R2 writes with retry |
| `graphcol.ts` | Columnar encoding/decoding of triples |
| `compaction.ts` | Merge small chunks into larger ones |
| `entity-index.ts` | Entity lookup index |

**R2 Path Format:**
```
{tld}/{domain}/{path}/_wal/{date}/{sequence}.gcol

Example: .com/.example/crm/acme/_wal/2024-01-16/123456-789.gcol
```

**When to use:** Durable storage, CDC replay, analytics, backup.

---

## Data Flow Diagrams

### Query Flow

```
Client                 Snippet              Broker DO            Shard DO              R2
  |                       |                     |                    |                   |
  |---(1) Query---------->|                     |                    |                   |
  |                       |                     |                    |                   |
  |                       |--(2) Bloom check--->|                    |                   |
  |                       |    (cache or fetch) |                    |                   |
  |                       |                     |                    |                   |
  |                       |<-(3) Shard IDs------|                    |                   |
  |                       |                     |                    |                   |
  |                       |--(4) Route to------>|                    |                   |
  |                       |    Broker DO        |                    |                   |
  |                       |                     |                    |                   |
  |                       |                     |--(5) Query-------->|                   |
  |                       |                     |    Shard DO(s)     |                   |
  |                       |                     |                    |                   |
  |                       |                     |<-(6) Results-------|                   |
  |                       |                     |                    |                   |
  |<-(7) Response---------|-------------------- |                    |                   |
  |                       |                     |                    |                   |
```

### Write Flow

```
Client                 Broker DO            Shard DO              R2
  |                       |                    |                   |
  |---(1) Write---------->|                    |                   |
  |    (via WebSocket)    |                    |                   |
  |                       |                    |                   |
  |                       |--(2) Route-------->|                   |
  |                       |    to Shard        |                   |
  |                       |                    |                   |
  |                       |                    |--(3) SQLite------>|
  |                       |                    |    insert         |
  |                       |                    |                   |
  |                       |                    |--(4) CDC event--->|
  |                       |                    |    to R2 buffer   |
  |                       |                    |                   |
  |                       |<-(5) Ack-----------|                   |
  |                       |                    |                   |
  |<-(6) Ack--------------|                    |                   |
  |                       |                    |                   |
```

### Hibernation Wake Cycle

```
                    Hibernated                Active
                    (no cost)                 (fresh quota)
                        |                         |
Client--WebSocket msg-->|---(wake)--------------->|
                        |                         |
                        |                         |--deserializeAttachment()
                        |                         |--process message
                        |                         |--make up to 1000 subrequests
                        |                         |--serializeAttachment()
                        |                         |--send response
                        |                         |
                        |<---(hibernate)----------|
                        |                         |
```

---

## Key Interfaces Between Layers

### Snippet -> Broker

```typescript
// Router output used to select Broker DO
interface RouteResult {
  shards: ShardInfo[];
  cacheKey?: string;
  ttl?: number;
}

interface ShardInfo {
  namespace: Namespace;
  shardId: string;  // Used with env.SHARD.idFromName()
  region?: string;
}
```

### Broker -> Shard

```typescript
// HTTP endpoints exposed by ShardDO
GET /lookup?ids=id1,id2,id3      // Batch entity lookup
GET /traverse?from=id&predicate=p // Forward traversal
GET /filter?field=f&op==&value=v // Property filter

POST /triples                     // Insert triples
PUT /triples/:subject/:predicate  // Update triple
DELETE /triples/:subject/:predicate?txId=... // Delete triple
```

### Shard -> R2 (Storage)

```typescript
// R2Writer for CDC streaming
interface R2Writer {
  write(events: CDCEvent[]): Promise<void>;
  flush(): Promise<void>;
  getStats(): R2WriterStats;
  close(): void;
}

interface CDCEvent {
  type: 'insert' | 'update' | 'delete';
  triple: Triple;
  timestamp: bigint;
}
```

### GraphAPI (via capnweb RPC)

```typescript
interface GraphAPI {
  // Entity CRUD
  getEntity(id: string): Promise<Entity | null>;
  createEntity(entity: Entity): Promise<void>;
  updateEntity(id: string, props: Record<string, unknown>): Promise<void>;
  deleteEntity(id: string): Promise<void>;

  // Traversals
  traverse(startId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>;
  reverseTraverse(targetId: string, predicate: string, options?: TraversalOptions): Promise<Entity[]>;
  pathTraverse(startId: string, path: string[], options?: TraversalOptions): Promise<Entity[]>;

  // Query
  query(queryString: string, options?: QueryOptions): Promise<QueryResult>;

  // Batch
  batchGet(ids: string[]): Promise<BatchResult<Entity | null>>;
  batchCreate(entities: Entity[]): Promise<BatchResult<void>>;
  batchExecute(operations: Operation[]): Promise<BatchResult<unknown>>;
}
```

---

## When to Use Each Layer

| Use Case | Layer | Reason |
|----------|-------|--------|
| Initial request routing | Snippet | FREE, fast bloom check |
| Cache key generation | Snippet | Deterministic, no state needed |
| Query orchestration | Broker | Coordinates multi-shard queries |
| WebSocket connections | Broker | Hibernation saves 95% cost |
| RPC API endpoint | Broker | capnweb RPC handler |
| Entity CRUD | Shard | SQLite persistence |
| Triple indexing | Shard | Secondary indexes |
| Full-text search | Shard | FTS5 index |
| Geo queries | Shard | Geohash index |
| CDC streaming | Storage | R2 write-ahead log |
| Analytics/replay | Storage | Read CDC files from R2 |
| Backup/export | Storage | GraphCol files in R2 |

---

## Supporting Modules

### Core (`src/core/`)
Types, branded IDs, validation, hash functions, geo utilities.

### Index (`src/index/`)
Secondary indexes: SPO, POS, OSP, FTS, Geo, HNSW vector search.

### Query (`src/query/`)
Query parsing, planning, execution, materialization.

### Protocol (`src/protocol/`)
capnweb RPC definitions, GraphAPI implementation.

### Cache (`src/cache/`)
Edge cache utilities, invalidation, metrics.

### Security (`src/security/`)
Auth, rate limiting, input validation, FTS sanitization.

### Errors (`src/errors/`)
Standardized API error responses.
