# GraphDB Development Context

## Architecture Summary

Cost-optimized graph database for Cloudflare Workers using:
- **Snippets** (FREE): Bloom routing, query parsing, shard routing
- **Edge Cache** (FREE): Index segments, geohash cells
- **Broker DO** (95% discount): Hibernating WebSocket, capnweb RPC
- **Shard DO**: SQLite triples with typed object columns
- **R2**: CDC streaming, GraphCol format, tiered compaction

## Critical Constraints

### Snippet Layer (FREE but limited)
- 32KB max script size
- 5ms max compute time
- 2-5 subrequests allowed
- No Node.js APIs - vanilla JS only

### DO Hibernation
- Use `ctx.acceptWebSocket()` for hibernation
- Fresh 1000 subrequest quota per `webSocketMessage` wake
- State via `serializeAttachment()`/`deserializeAttachment()`
- 95% cost discount vs active connections

### Data Model
- URL-based identifiers: `$id`, `$type`, `$context`
- **JS/TS native field names - NO COLONS** (e.g., `name` not `schema:name`)
- Typed object columns: REF, STRING, INT64, FLOAT64, GEO_POINT, TIMESTAMP, etc.

### Protocol
- Use **capnweb** (https://github.com/cloudflare/capnweb) - NOT Cap'n Proto
- JS-native RPC, <10KB, zero deps, promise pipelining
- Works over WebSocket, HTTP, postMessage

## Code Patterns

### Triple Storage (SQLite)
```sql
CREATE TABLE triples (
  subject TEXT NOT NULL,      -- $id URL
  predicate TEXT NOT NULL,    -- field name (no colons)
  obj_type INTEGER NOT NULL,  -- ObjectType enum
  obj_ref TEXT,               -- for REF type
  obj_string TEXT,            -- for STRING type
  obj_int64 INTEGER,          -- for INT64 type
  obj_float64 REAL,           -- for FLOAT64 type
  obj_lat REAL,               -- for GEO_POINT
  obj_lng REAL,               -- for GEO_POINT
  obj_timestamp INTEGER,      -- for TIMESTAMP
  timestamp INTEGER NOT NULL,
  tx_id TEXT NOT NULL
);

-- Indexes for all access patterns
CREATE INDEX idx_spo ON triples(subject, predicate, obj_type);
CREATE INDEX idx_pos ON triples(predicate, obj_type, subject);
CREATE INDEX idx_osp ON triples(obj_ref, subject, predicate) WHERE obj_type = 10;
```

### Bloom Filter Check (Snippet)
```javascript
function mightExist(filter, id) {
  const h1 = fnv1a(id);
  const h2 = fnv1a(id + '\x00');
  for (let i = 0; i < filter.k; i++) {
    const bit = (h1 + i * h2) % filter.m;
    if (!getBit(filter.bits, bit)) return false;
  }
  return true;
}
```

### Hibernating WebSocket DO
```typescript
export class BrokerDO implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    const upgradeHeader = request.headers.get('Upgrade');
    if (upgradeHeader === 'websocket') {
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]); // Enables hibernation
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    // Fresh 1000 subrequest quota here
    const result = await this.handleQuery(message);
    ws.send(JSON.stringify(result));
  }
}
```

## File Locations

| Component | Path |
|-----------|------|
| Types/Core | `src/core/` |
| Snippet (bloom, parser, router) | `src/snippet/` |
| Broker DO | `src/broker/` |
| Shard DO | `src/shard/` |
| GraphCol format | `src/storage/` |
| Indexes (SPO, FTS, Geo) | `src/index/` |
| Query planner/executor | `src/query/` |
| capnweb protocol | `src/protocol/` |

## TDD Approach

1. **RED**: Write failing test first
2. **GREEN**: Minimal code to pass
3. **REFACTOR**: Clean up, optimize

Tests in `test/` mirror `src/` structure.

## Validated Spikes

Reference implementations in `packages/graphdb-spikes/`:
- `ws-subrequest-reset/` - Hibernation + quota reset (P0 validated)
- `snippet-bloom-router/` - Bloom filter routing (P0 validated)

## Dependencies

- `capnweb` - RPC protocol
- `vitest` - Testing
- `@cloudflare/workers-types` - CF types
- `wrangler` - Dev/deploy
