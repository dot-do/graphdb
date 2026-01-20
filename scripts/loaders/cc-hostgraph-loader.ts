/**
 * Common Crawl Host Graph Loader Worker
 *
 * Streams Common Crawl host-level web graph data to R2 as GraphCol chunks.
 * Uses streaming decompression and transformation - processes data incrementally.
 *
 * Source: Common Crawl Web Graph
 * https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/index.html
 *
 * File Format:
 * - Vertices: id TAB reversed_hostname (16 files)
 * - Edges: from_id TAB to_id (32 files)
 *
 * Entity Model:
 * - Host: $id=https://cc.org/host/{hostname}
 *   - hostname (STRING), vertexId (INT64)
 * - Link: subject=source_host, linksTo=target_host (REF)
 *
 * Limits (for initial load):
 * - 1M hosts max
 * - 10M edges max
 *
 * @packageDocumentation
 */

import type { Triple, TypedObject } from '../../src/core/triple';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { EntityId, Predicate, TransactionId, Namespace } from '../../src/core/types';
import { encodeGraphCol, decodeGraphCol } from '../../src/storage/graphcol';
import {
  createBloomFilter,
  addToFilter,
  addManyToFilter,
  serializeFilter,
  type BloomFilter,
  type SerializedFilter,
} from '../../src/snippet/bloom';
import { createExplorerRoutes, type Entity, type SearchResult } from './lib/explorer';
import {
  createStreamingLineReader,
  createBatchedTripleWriter,
  type StreamingLineReader,
  type BatchedTripleWriter,
} from './lib/import-utils';

// ============================================================================
// CONSTANTS
// ============================================================================

/**
 * Common Crawl Web Graph - August/September/October 2024 release
 * Using the most recent stable release
 */
const CC_BASE_URL = 'https://data.commoncrawl.org';
const CC_GRAPH_PATH = '/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host';

const PATHS_URLS = {
  vertices: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-vertices.paths.gz`,
  edges: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-edges.paths.gz`,
} as const;

const NAMESPACE = 'https://cc.org/' as Namespace;
const R2_PREFIX = 'datasets/cc-hostgraph';

// Limits for initial load (full graph is ~300M hosts, 2.6B edges)
// Note: idToHostname Map will use ~50-100MB for 1M hosts - within DO limits
const MAX_HOSTS = 1_000_000;
const MAX_EDGES = 10_000_000;
const CHUNK_TRIPLE_LIMIT = 10_000; // Reduced from 50K for safer memory usage

// ============================================================================
// TYPES
// ============================================================================

interface Env {
  LAKEHOUSE: R2Bucket;
}

interface LoaderProgress {
  phase: 'vertices' | 'edges' | 'finalizing';
  filesProcessed: number;
  totalFiles: number;
  linesProcessed: number;
  entitiesCreated: number;
  edgesProcessed: number;
  chunksUploaded: number;
  bytesUploaded: number;
}

interface LoaderIndex {
  version: string;
  source: string;
  release: string;
  loadedAt: string;
  namespace: string;
  limits: {
    maxHosts: number;
    maxEdges: number;
  };
  stats: {
    hosts: number;
    edges: number;
    totalTriples: number;
    totalChunks: number;
    totalSizeBytes: number;
  };
  chunks: {
    path: string;
    tripleCount: number;
    sizeBytes: number;
  }[];
  bloom: {
    path: string;
    entityCount: number;
  };
}

interface ChunkInfo {
  path: string;
  tripleCount: number;
  sizeBytes: number;
}

// ============================================================================
// ULID GENERATOR
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
let lastTime = 0;
let lastRandom = new Uint8Array(10);

function generateULID(): TransactionId {
  let now = Date.now();

  if (now === lastTime) {
    for (let i = lastRandom.length - 1; i >= 0; i--) {
      if (lastRandom[i] < 255) {
        lastRandom[i]++;
        break;
      }
      lastRandom[i] = 0;
    }
  } else {
    lastTime = now;
    crypto.getRandomValues(lastRandom);
  }

  let ulid = '';

  for (let i = 9; i >= 0; i--) {
    ulid = ENCODING[now % 32] + ulid;
    now = Math.floor(now / 32);
  }

  for (let i = 0; i < 10; i++) {
    const byte = lastRandom[i] ?? 0;
    ulid += ENCODING[byte >> 3];
    ulid += ENCODING[(byte & 7) << 2];
  }

  return ulid.slice(0, 26) as TransactionId;
}

// ============================================================================
// TRIPLE FACTORY
// ============================================================================

function createTripleWithType(
  subject: EntityId,
  predicate: Predicate,
  object: TypedObject,
  txId: TransactionId
): Triple {
  return {
    subject,
    predicate,
    object,
    timestamp: BigInt(Date.now()),
    txId,
  };
}

function stringObject(value: string): TypedObject {
  return { type: ObjectType.STRING, value };
}

function int64Object(value: bigint): TypedObject {
  return { type: ObjectType.INT64, value };
}

function refObject(entityId: EntityId): TypedObject {
  return { type: ObjectType.REF, value: entityId };
}

// ============================================================================
// HOSTNAME UTILITIES
// ============================================================================

/**
 * Convert reversed hostname to normal hostname
 * Common Crawl stores hostnames in reverse (e.g., "com.example.www" -> "www.example.com")
 */
function reverseHostname(reversed: string): string {
  return reversed.split('.').reverse().join('.');
}

/**
 * Create a safe URL-friendly host ID from hostname
 * Encodes special characters that might be in hostnames
 */
function createHostId(hostname: string): string {
  // URL-encode the hostname for safe use in URLs
  return `https://cc.org/host/${encodeURIComponent(hostname)}`;
}

// ============================================================================
// STREAMING UTILITIES
// ============================================================================

/**
 * Fetch and decompress the paths file to get list of data files
 */
async function fetchPathsList(pathsUrl: string): Promise<string[]> {
  console.log(`Fetching paths list from ${pathsUrl}...`);

  const response = await fetch(pathsUrl);
  if (!response.ok) {
    throw new Error(`Failed to fetch paths: ${response.status} ${response.statusText}`);
  }

  if (!response.body) {
    throw new Error('No response body for paths file');
  }

  // Decompress gzip
  const decompressed = response.body.pipeThrough(new DecompressionStream('gzip'));
  const text = await new Response(decompressed).text();

  // Each line is a path relative to commoncrawl S3/HTTP root
  const paths = text
    .split('\n')
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((path) => `${CC_BASE_URL}/${path}`);

  console.log(`Found ${paths.length} data files`);
  return paths;
}

/**
 * Create a line-by-line streaming reader from a gzipped URL
 */
async function* streamGzipLines(url: string): AsyncGenerator<string> {
  console.log(`Streaming from ${url}...`);

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }

  if (!response.body) {
    throw new Error(`No response body for ${url}`);
  }

  const decompressed = response.body.pipeThrough(new DecompressionStream('gzip'));
  const textStream = decompressed.pipeThrough(new TextDecoderStream());
  const reader = textStream.getReader();

  let buffer = '';

  try {
    while (true) {
      const { done, value } = await reader.read();

      if (done) {
        // Yield any remaining content
        if (buffer.trim()) {
          yield buffer.trim();
        }
        break;
      }

      buffer += value;
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        const trimmed = line.trim();
        if (trimmed) {
          yield trimmed;
        }
      }
    }
  } finally {
    reader.releaseLock();
  }
}

// ============================================================================
// CHUNK WRITER
// ============================================================================

class ChunkWriter {
  private triples: Triple[] = [];
  private chunkIndex = 0;
  private totalTriples = 0;
  private uploadedChunks: ChunkInfo[] = [];

  constructor(
    private bucket: R2Bucket,
    private basePath: string,
    private onProgress?: (info: { chunksUploaded: number; bytesUploaded: number }) => void
  ) {}

  async addTriple(triple: Triple): Promise<void> {
    this.triples.push(triple);

    if (this.triples.length >= CHUNK_TRIPLE_LIMIT) {
      await this.flushChunk();
    }
  }

  async addTriples(triples: Triple[]): Promise<void> {
    for (const triple of triples) {
      this.triples.push(triple);
      if (this.triples.length >= CHUNK_TRIPLE_LIMIT) {
        await this.flushChunk();
      }
    }
  }

  async flushChunk(): Promise<void> {
    if (this.triples.length === 0) return;

    const chunkId = `chunk_${this.chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `${this.basePath}/chunks/${chunkId}.graphcol`;

    // Encode triples to GraphCol format
    const encoded = encodeGraphCol(this.triples, NAMESPACE);

    // Upload chunk
    await this.bucket.put(chunkPath, encoded, {
      customMetadata: {
        tripleCount: this.triples.length.toString(),
      },
    });

    // Track chunk info
    this.uploadedChunks.push({
      path: chunkPath,
      tripleCount: this.triples.length,
      sizeBytes: encoded.length,
    });

    this.totalTriples += this.triples.length;
    this.chunkIndex++;

    if (this.onProgress) {
      this.onProgress({
        chunksUploaded: this.chunkIndex,
        bytesUploaded: this.uploadedChunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      });
    }

    console.log(
      `[ChunkWriter] Uploaded ${chunkId}: ${this.triples.length} triples, ${(encoded.length / 1024).toFixed(1)}KB`
    );

    this.triples = [];
  }

  async finalize(): Promise<{
    chunks: ChunkInfo[];
    totalTriples: number;
  }> {
    await this.flushChunk();
    return {
      chunks: this.uploadedChunks,
      totalTriples: this.totalTriples,
    };
  }
}

// ============================================================================
// HOST GRAPH LOADER
// ============================================================================

/**
 * Load vertices (hosts) from Common Crawl
 * Format: vertex_id TAB reversed_hostname
 *
 * Creates triples:
 * - $type = "Host"
 * - hostname = normal hostname
 * - vertexId = original CC vertex ID
 */
async function loadVertices(
  bucket: R2Bucket,
  writer: ChunkWriter,
  onProgress?: (progress: LoaderProgress) => void
): Promise<Map<bigint, string>> {
  const txId = generateULID();
  const idToHostname = new Map<bigint, string>();

  // Fetch list of vertex files
  const vertexPaths = await fetchPathsList(PATHS_URLS.vertices);

  let linesProcessed = 0;
  let hostsCreated = 0;
  let filesProcessed = 0;

  for (const fileUrl of vertexPaths) {
    filesProcessed++;
    console.log(`Processing vertex file ${filesProcessed}/${vertexPaths.length}: ${fileUrl}`);

    for await (const line of streamGzipLines(fileUrl)) {
      // Check if we've hit the host limit
      if (hostsCreated >= MAX_HOSTS) {
        console.log(`Reached host limit of ${MAX_HOSTS}`);
        break;
      }

      linesProcessed++;

      // Parse line: vertex_id TAB reversed_hostname
      const tabIndex = line.indexOf('\t');
      if (tabIndex === -1) continue;

      const vertexIdStr = line.substring(0, tabIndex);
      const reversedHostname = line.substring(tabIndex + 1);

      if (!vertexIdStr || !reversedHostname) continue;

      const vertexId = BigInt(vertexIdStr);
      const hostname = reverseHostname(reversedHostname);

      // Store mapping for edge processing
      idToHostname.set(vertexId, hostname);

      // Create entity URL
      const entityUrl = createHostId(hostname);
      const entityId = createEntityId(entityUrl);

      // Generate triples for this host
      const triples: Triple[] = [
        createTripleWithType(
          entityId,
          createPredicate('$type'),
          stringObject('Host'),
          txId
        ),
        createTripleWithType(
          entityId,
          createPredicate('hostname'),
          stringObject(hostname),
          txId
        ),
        createTripleWithType(
          entityId,
          createPredicate('vertexId'),
          int64Object(vertexId),
          txId
        ),
      ];

      await writer.addTriples(triples);
      hostsCreated++;

      // Progress logging
      if (linesProcessed % 100_000 === 0) {
        console.log(
          `[Vertices] ${linesProcessed.toLocaleString()} lines, ${hostsCreated.toLocaleString()} hosts`
        );
        if (onProgress) {
          onProgress({
            phase: 'vertices',
            filesProcessed,
            totalFiles: vertexPaths.length,
            linesProcessed,
            entitiesCreated: hostsCreated,
            edgesProcessed: 0,
            chunksUploaded: 0,
            bytesUploaded: 0,
          });
        }
      }
    }

    // Check limit again after file
    if (hostsCreated >= MAX_HOSTS) {
      break;
    }
  }

  console.log(
    `[Vertices] Completed: ${linesProcessed.toLocaleString()} lines, ${hostsCreated.toLocaleString()} hosts`
  );

  return idToHostname;
}

/**
 * Load edges (links) from Common Crawl
 * Format: from_vertex_id TAB to_vertex_id
 *
 * Creates triples:
 * - linksTo = REF to target host
 */
async function loadEdges(
  bucket: R2Bucket,
  writer: ChunkWriter,
  idToHostname: Map<bigint, string>,
  onProgress?: (progress: LoaderProgress) => void
): Promise<number> {
  const txId = generateULID();

  // Fetch list of edge files
  const edgePaths = await fetchPathsList(PATHS_URLS.edges);

  let linesProcessed = 0;
  let edgesCreated = 0;
  let skippedEdges = 0;
  let filesProcessed = 0;

  for (const fileUrl of edgePaths) {
    filesProcessed++;
    console.log(`Processing edge file ${filesProcessed}/${edgePaths.length}: ${fileUrl}`);

    for await (const line of streamGzipLines(fileUrl)) {
      // Check if we've hit the edge limit
      if (edgesCreated >= MAX_EDGES) {
        console.log(`Reached edge limit of ${MAX_EDGES}`);
        break;
      }

      linesProcessed++;

      // Parse line: from_vertex_id TAB to_vertex_id
      const tabIndex = line.indexOf('\t');
      if (tabIndex === -1) continue;

      const fromIdStr = line.substring(0, tabIndex);
      const toIdStr = line.substring(tabIndex + 1);

      if (!fromIdStr || !toIdStr) continue;

      const fromId = BigInt(fromIdStr);
      const toId = BigInt(toIdStr);

      // Look up hostnames from our vertex map
      const fromHostname = idToHostname.get(fromId);
      const toHostname = idToHostname.get(toId);

      // Skip edges where we don't have both vertices (due to limit)
      if (!fromHostname || !toHostname) {
        skippedEdges++;
        continue;
      }

      // Create entity URLs
      const fromEntityUrl = createHostId(fromHostname);
      const toEntityUrl = createHostId(toHostname);
      const fromEntityId = createEntityId(fromEntityUrl);
      const toEntityId = createEntityId(toEntityUrl);

      // Create link triple
      const triple = createTripleWithType(
        fromEntityId,
        createPredicate('linksTo'),
        refObject(toEntityId),
        txId
      );

      await writer.addTriple(triple);
      edgesCreated++;

      // Progress logging
      if (linesProcessed % 500_000 === 0) {
        console.log(
          `[Edges] ${linesProcessed.toLocaleString()} lines, ${edgesCreated.toLocaleString()} edges, ${skippedEdges.toLocaleString()} skipped`
        );
        if (onProgress) {
          onProgress({
            phase: 'edges',
            filesProcessed,
            totalFiles: edgePaths.length,
            linesProcessed,
            entitiesCreated: idToHostname.size,
            edgesProcessed: edgesCreated,
            chunksUploaded: 0,
            bytesUploaded: 0,
          });
        }
      }
    }

    // Check limit again after file
    if (edgesCreated >= MAX_EDGES) {
      break;
    }
  }

  console.log(
    `[Edges] Completed: ${linesProcessed.toLocaleString()} lines, ${edgesCreated.toLocaleString()} edges, ${skippedEdges.toLocaleString()} skipped`
  );

  return edgesCreated;
}

/**
 * Main loader function
 */
async function loadHostGraph(bucket: R2Bucket): Promise<LoaderIndex> {
  const startTime = Date.now();
  console.log('Starting Common Crawl Host Graph load...');
  console.log(`Limits: ${MAX_HOSTS.toLocaleString()} hosts, ${MAX_EDGES.toLocaleString()} edges`);

  // Create chunk writer
  const writer = new ChunkWriter(bucket, R2_PREFIX);

  // Phase 1: Load vertices (hosts)
  console.log('\n=== Phase 1: Loading Vertices ===');
  const idToHostname = await loadVertices(bucket, writer);
  const hostCount = idToHostname.size;

  // Phase 2: Load edges (links)
  console.log('\n=== Phase 2: Loading Edges ===');
  const edgeCount = await loadEdges(bucket, writer, idToHostname);

  // Phase 3: Finalize
  console.log('\n=== Phase 3: Finalizing ===');
  const finalResult = await writer.finalize();

  // Create bloom filter for all hosts
  console.log('Building bloom filter...');
  const bloomFilter = createBloomFilter({
    capacity: hostCount,
    targetFpr: 0.01,
  });

  const entityIds: string[] = [];
  for (const hostname of idToHostname.values()) {
    entityIds.push(createHostId(hostname));
  }
  addManyToFilter(bloomFilter, entityIds);

  // Upload bloom filter
  const bloomPath = `${R2_PREFIX}/bloom/filter.json`;
  const serializedBloom = serializeFilter(bloomFilter);
  await bucket.put(bloomPath, JSON.stringify(serializedBloom, null, 2));
  console.log(`Uploaded bloom filter to ${bloomPath}`);

  // Create and upload index
  const index: LoaderIndex = {
    version: '1.0.0',
    source: 'Common Crawl Web Graph',
    release: 'cc-main-2024-aug-sep-oct',
    loadedAt: new Date().toISOString(),
    namespace: NAMESPACE,
    limits: {
      maxHosts: MAX_HOSTS,
      maxEdges: MAX_EDGES,
    },
    stats: {
      hosts: hostCount,
      edges: edgeCount,
      totalTriples: finalResult.totalTriples,
      totalChunks: finalResult.chunks.length,
      totalSizeBytes: finalResult.chunks.reduce((sum, c) => sum + c.sizeBytes, 0),
    },
    chunks: finalResult.chunks,
    bloom: {
      path: bloomPath,
      entityCount: hostCount,
    },
  };

  const indexPath = `${R2_PREFIX}/index.json`;
  await bucket.put(indexPath, JSON.stringify(index, null, 2));
  console.log(`Uploaded index to ${indexPath}`);

  const elapsed = Date.now() - startTime;
  console.log(`\n=== Load Complete ===`);
  console.log(`Duration: ${(elapsed / 1000).toFixed(1)}s`);
  console.log(`Hosts: ${hostCount.toLocaleString()}`);
  console.log(`Edges: ${edgeCount.toLocaleString()}`);
  console.log(`Triples: ${finalResult.totalTriples.toLocaleString()}`);
  console.log(`Chunks: ${finalResult.chunks.length}`);
  console.log(`Size: ${(index.stats.totalSizeBytes / 1024 / 1024).toFixed(1)}MB`);

  return index;
}

// ============================================================================
// EXPLORER HELPERS
// ============================================================================

function triplesToEntity(triples: Triple[]): Entity | null {
  if (triples.length === 0) return null;

  const entity: Entity = { $id: triples[0].subject };

  for (const triple of triples) {
    const predicate = triple.predicate;

    if (predicate === '$type' && triple.object.type === ObjectType.STRING) {
      entity.$type = triple.object.value;
      continue;
    }

    let value: unknown;
    switch (triple.object.type) {
      case ObjectType.STRING:
        value = triple.object.value;
        break;
      case ObjectType.INT32:
      case ObjectType.INT64:
        value = Number(triple.object.value);
        break;
      case ObjectType.FLOAT64:
        value = triple.object.value;
        break;
      case ObjectType.REF:
        value = triple.object.value;
        break;
      default:
        value = String(triple.object.value);
    }

    if (entity[predicate] !== undefined) {
      if (Array.isArray(entity[predicate])) {
        (entity[predicate] as unknown[]).push(value);
      } else {
        entity[predicate] = [entity[predicate], value];
      }
    } else {
      entity[predicate] = value;
    }
  }

  return entity;
}

async function getEntityFromR2(bucket: R2Bucket, entityId: string): Promise<Entity | null> {
  const indexObj = await bucket.get(`${R2_PREFIX}/index.json`);
  if (!indexObj) return null;

  const index = await indexObj.json<LoaderIndex>();

  for (const chunk of index.chunks) {
    const chunkObj = await bucket.get(chunk.path);
    if (!chunkObj) continue;

    const data = new Uint8Array(await chunkObj.arrayBuffer());
    const triples = decodeGraphCol(data);

    const entityTriples = triples.filter((t) => t.subject === entityId);
    if (entityTriples.length > 0) {
      return triplesToEntity(entityTriples);
    }
  }

  return null;
}

async function searchEntitiesInR2(
  bucket: R2Bucket,
  query: string,
  limit: number = 50
): Promise<SearchResult[]> {
  const results: SearchResult[] = [];
  const lowerQuery = query.toLowerCase();

  const indexObj = await bucket.get(`${R2_PREFIX}/index.json`);
  if (!indexObj) return results;

  const index = await indexObj.json<LoaderIndex>();

  for (const chunk of index.chunks) {
    if (results.length >= limit) break;

    const chunkObj = await bucket.get(chunk.path);
    if (!chunkObj) continue;

    const data = new Uint8Array(await chunkObj.arrayBuffer());
    const triples = decodeGraphCol(data);

    const bySubject = new Map<string, Triple[]>();
    for (const triple of triples) {
      const existing = bySubject.get(triple.subject) || [];
      existing.push(triple);
      bySubject.set(triple.subject, existing);
    }

    for (const [subject, subjectTriples] of bySubject) {
      if (results.length >= limit) break;

      let hostname: string | undefined;
      let type: string | undefined;

      for (const triple of subjectTriples) {
        if (triple.predicate === 'hostname' && triple.object.type === ObjectType.STRING) {
          hostname = triple.object.value;
        }
        if (triple.predicate === '$type' && triple.object.type === ObjectType.STRING) {
          type = triple.object.value;
        }
      }

      if (hostname && hostname.toLowerCase().includes(lowerQuery)) {
        results.push({
          $id: subject,
          $type: type,
          label: hostname,
        });
      }
    }
  }

  return results;
}

async function getRandomEntityIdFromR2(bucket: R2Bucket): Promise<string | null> {
  const indexObj = await bucket.get(`${R2_PREFIX}/index.json`);
  if (!indexObj) return null;

  const index = await indexObj.json<LoaderIndex>();
  if (index.chunks.length === 0) return null;

  const randomChunk = index.chunks[Math.floor(Math.random() * index.chunks.length)];
  const chunkObj = await bucket.get(randomChunk.path);
  if (!chunkObj) return null;

  const data = new Uint8Array(await chunkObj.arrayBuffer());
  const triples = decodeGraphCol(data);
  if (triples.length === 0) return null;

  const subjects = [...new Set(triples.map((t) => t.subject))];
  return subjects[Math.floor(Math.random() * subjects.length)];
}

async function getEntityCountFromR2(bucket: R2Bucket): Promise<number> {
  const indexObj = await bucket.get(`${R2_PREFIX}/index.json`);
  if (!indexObj) return 0;

  const index = await indexObj.json<LoaderIndex>();
  return index.stats.hosts;
}

// ============================================================================
// WORKER HANDLER
// ============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const baseUrl = `${url.protocol}//${url.host}`;

    // Create explorer routes
    const explorer = createExplorerRoutes({
      namespace: 'cc-hostgraph',
      displayName: 'Common Crawl Host Graph Explorer',
      baseUrl,
      getEntity: (id) => getEntityFromR2(env.LAKEHOUSE, id),
      searchEntities: (q, limit) => searchEntitiesInR2(env.LAKEHOUSE, q, limit),
      getRandomEntityId: () => getRandomEntityIdFromR2(env.LAKEHOUSE),
      getEntityCount: () => getEntityCountFromR2(env.LAKEHOUSE),
    });

    // Try explorer routes first
    const explorerResult = await explorer(request, url);
    if (explorerResult.handled && explorerResult.response) {
      return explorerResult.response;
    }

    // Root endpoint
    if (url.pathname === '/' || url.pathname === '') {
      return new Response(
        JSON.stringify(
          {
            name: 'Common Crawl Host Graph Loader',
            description: 'Streams CC host-level web graph to R2 as GraphCol chunks',
            endpoints: {
              'POST /load': 'Trigger host graph load (1M hosts, 10M edges)',
              'GET /status': 'Check load status',
              'GET /sample': 'Fetch sample of loaded hosts',
              '/explore': 'Interactive graph explorer',
              '/entity/{id}': 'View entity by ID (URL-encoded)',
              '/search?q=term': 'Search hosts by hostname',
              '/random': 'Redirect to random host',
            },
            source: {
              release: 'cc-main-2024-aug-sep-oct',
              url: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/',
              fullStats: {
                hosts: '299.9 million',
                edges: '2.6 billion',
              },
            },
            limits: {
              maxHosts: MAX_HOSTS,
              maxEdges: MAX_EDGES,
            },
          },
          null,
          2
        ),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Status endpoint
    if (url.pathname === '/status' && request.method === 'GET') {
      const indexObj = await env.LAKEHOUSE.get(`${R2_PREFIX}/index.json`);
      if (!indexObj) {
        return new Response(JSON.stringify({ status: 'not_loaded' }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      const index = await indexObj.json<LoaderIndex>();
      return new Response(
        JSON.stringify(
          {
            status: 'loaded',
            ...index,
          },
          null,
          2
        ),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Sample endpoint - show first few hosts
    if (url.pathname === '/sample' && request.method === 'GET') {
      const indexObj = await env.LAKEHOUSE.get(`${R2_PREFIX}/index.json`);
      if (!indexObj) {
        return new Response(
          JSON.stringify({ error: 'No data loaded' }),
          {
            status: 404,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      const index = await indexObj.json<LoaderIndex>();

      // Just return index summary as sample
      return new Response(
        JSON.stringify(
          {
            status: 'loaded',
            stats: index.stats,
            sampleChunks: index.chunks.slice(0, 5),
          },
          null,
          2
        ),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Load endpoint
    if (url.pathname === '/load' && request.method === 'POST') {
      try {
        const index = await loadHostGraph(env.LAKEHOUSE);
        return new Response(
          JSON.stringify(
            {
              success: true,
              index,
            },
            null,
            2
          ),
          {
            headers: { 'Content-Type': 'application/json' },
          }
        );
      } catch (error) {
        console.error('Load failed:', error);
        return new Response(
          JSON.stringify({
            success: false,
            error: error instanceof Error ? error.message : String(error),
            stack: error instanceof Error ? error.stack : undefined,
          }),
          {
            status: 500,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }
    }

    return new Response('Not Found', { status: 404 });
  },
};

// Export types for testing
export type { LoaderIndex, LoaderProgress };
