# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-20

### Added

#### Core Architecture
- Cost-optimized graph database designed for Cloudflare Workers
- Tiered architecture: Snippets (FREE) -> Edge Cache (FREE) -> Broker DO (95% discount) -> Shard DO
- URL-based entity identifiers with typed object columns ($id, $type, $context)
- Triple storage model (subject, predicate, object) with SQLite backing

#### Triple Storage & Indexes
- SQLite-backed Shard DO with typed columns (REF, STRING, INT64, FLOAT64, GEO_POINT, TIMESTAMP)
- SPO (Subject-Predicate-Object) index for forward lookups
- POS (Predicate-Object-Subject) index for predicate queries
- OSP (Object-Subject-Predicate) index for reverse traversals
- Combined index file format (.gidx) with header-based range requests

#### Vector Search (HNSW)
- Quantized vector file format (.qvec) for efficient storage
- Support for Float32 and Int8 quantization modes
- Cosine similarity and Hamming distance metrics
- VectorIndex type with configurable dimensions

#### Full-Text Search (FTS)
- FTS5-based full-text search integration
- FTS query sanitization to prevent injection attacks
- Configurable FTS index initialization and rebuilding

#### Geospatial Queries
- Geohash-based spatial indexing
- Radius queries with Haversine distance calculations
- Bounding box queries for rectangular regions
- GEO_POINT typed column support

#### WebSocket & RPC
- Hibernating WebSocket connections via Broker DO (95% cost reduction)
- capnweb RPC protocol for promise pipelining
- Fresh 1000 subrequest quota per webSocketMessage wake
- Auto-reconnection with exponential backoff

#### Client SDK
- TypeScript client with full type support
- Connection state management (connected, connecting, reconnecting, disconnected)
- CRUD operations: insert, query, update, delete
- Graph traversal: traverse, reverseTraverse, pathTraverse
- Batch operations: batchGet, batchInsert, batchExecute
- Connection stats and latency tracking

#### Query System
- Path query language: `user:123.friends.posts`, `user:123.friends[?age > 30]`
- Reverse traversal syntax: `post:456 <- likes`
- Bounded recursion: `user:123.friends*[depth <= 3]`
- Query tokenizer/lexer for the snippet layer

#### Snippet Layer (Edge Routing)
- Bloom filter routing with 98% negative lookup rejection
- Query parsing within 5ms/32KB snippet constraints
- Shard routing based on namespace extraction
- Edge cache integration for index segments and geohash cells
- Immutable bloom cache with configurable TTL

#### Storage & CDC
- R2 lakehouse integration for CDC streaming
- GraphCol columnar format for efficient storage
- Tiered compaction (L1 -> L2)
- URL hierarchy-based partitioning
- Streaming line reader for large file imports
- Batched triple writer with resumable state

#### Coordinator & Traversal
- CoordinatorDO for query orchestration
- CDCCoordinatorDO for change data capture coordination
- TraversalDO for region-optimized graph traversal
- Bootstrap mechanism for DO placement at R2-local colos (colo.do integration)

#### Caching
- ChunkEdgeCache for cached chunk responses
- CacheInvalidator for compaction-triggered invalidation
- CacheMetricsCollector for hit/miss rate tracking
- Configurable cache headers with stale-while-revalidate support

#### Benchmarking
- JSONBench-compatible `/bench` endpoint
- Configurable dataset sizes (tiny, small, medium, onet, imdb)
- Benchmark scenarios: point-lookup, traversal-1hop, traversal-3hop, write-throughput
- In-memory triple store for benchmark isolation
- Throughput and latency metrics collection

#### HTTP API
- `/connect` - WebSocket upgrade endpoint
- `/shard/:id/*` - Shard DO operations
- `/broker/:id/*` - Broker metrics and state
- `/bootstrap/*` - colo.do DO placement endpoints
- `/benchmark/*` - Benchmark suite endpoints
- `/health` - Health check endpoint

### Changed

- Adopted JS/TS native field names (no colons) instead of RDF-style prefixes
- Designed for Cloudflare Workers constraints (5ms snippet budget, 32KB limit)

### Security

- FTS query sanitization preventing SQL injection attacks
- JSON parsing with configurable limits (max size, depth, keys)
- Entity ID validation with maximum length constraints
- Rate limiter with configurable window and request limits
- API key validation for authentication
- JWT validation with issuer/audience verification
- Worker binding validation for service-to-service auth
- Namespace-level and entity-level permission checks
- Permission context with read/write/internal access controls

[0.1.0]: https://github.com/dot-do/pocs/releases/tag/graphdb-v0.1.0
