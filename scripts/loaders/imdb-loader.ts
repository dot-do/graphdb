/**
 * IMDB Data Loader Worker
 *
 * Streams IMDB TSV datasets directly from source to R2 as GraphCol chunks.
 * Uses streaming decompression and transformation - never buffers entire dataset.
 *
 * Entity Model:
 * - Movie: $id=https://imdb.com/title/{tconst}
 *   - title (STRING), year (INT32), genres (STRING), rating (FLOAT64), runtime (INT32)
 * - Person: $id=https://imdb.com/name/{nconst}
 *   - name (STRING), birthYear (INT32), profession (STRING)
 * - Relations: starring (REF), directedBy (REF)
 *
 * Datasets:
 * - title.basics.tsv.gz - Movie metadata
 * - title.ratings.tsv.gz - Movie ratings
 * - name.basics.tsv.gz - Person metadata
 * - title.principals.tsv.gz - Movie-Person relations
 */

import type { Triple, TypedObject } from '../../src/core/triple';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import type { EntityId, Predicate, TransactionId, Namespace } from '../../src/core/types';
import { encodeGraphCol, decodeGraphCol } from '../../src/storage/graphcol';
import {
  createBloomFilter,
  addToFilter,
  serializeFilter,
  type BloomFilter,
  type SerializedFilter,
} from '../../src/snippet/bloom';
import { createExplorerRoutes, type Entity, type SearchResult } from './lib/explorer';
import { createStreamingLineReader, type StreamingLineReader } from './lib/import-utils';

// ============================================================================
// CONSTANTS
// ============================================================================

const IMDB_DATASETS = {
  titleBasics: 'https://datasets.imdbws.com/title.basics.tsv.gz',
  titleRatings: 'https://datasets.imdbws.com/title.ratings.tsv.gz',
  nameBasics: 'https://datasets.imdbws.com/name.basics.tsv.gz',
  titlePrincipals: 'https://datasets.imdbws.com/title.principals.tsv.gz',
} as const;

const NAMESPACE = 'https://imdb.com/' as Namespace;
// Increased from 50K to 250K to reduce R2 uploads and stay under subrequest limit
// With 1000 subrequest limit and 2 uploads per chunk (data + bloom), max ~450 chunks
const CHUNK_TRIPLE_LIMIT = 250_000;
const TARGET_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB chunks

// Bloom filter capacities per dataset (approximate entity counts)
const BLOOM_CAPACITIES: Record<string, number> = {
  titleBasics: 15_000_000,   // ~10M titles
  titleRatings: 2_000_000,   // ~1.3M ratings
  nameBasics: 20_000_000,    // ~13M people
  titlePrincipals: 70_000_000, // ~60M relations
};

// ============================================================================
// TYPES
// ============================================================================

interface Env {
  LAKEHOUSE: R2Bucket;
}

interface ChunkManifest {
  version: 1;
  namespace: string;
  datasets: {
    name: string;
    url: string;
    status: 'pending' | 'processing' | 'completed' | 'error';
    chunks: string[];
    tripleCount: number;
    entityCount: number;
    error?: string;
    startedAt?: string;
    completedAt?: string;
  }[];
  chunks: {
    id: string;
    path: string;
    bloomPath: string;
    tripleCount: number;
    sizeBytes: number;
    predicates: string[];
    createdAt: string;
  }[];
  stats: {
    totalTriples: number;
    totalChunks: number;
    totalEntities: number;
    totalSizeBytes: number;
  };
  createdAt: string;
  updatedAt: string;
}

interface LoaderProgress {
  dataset: string;
  linesProcessed: number;
  triplesGenerated: number;
  chunksUploaded: number;
  bytesUploaded: number;
  errors: number;
}

// ============================================================================
// ULID GENERATOR (Simple timestamp-based for loader)
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
let lastTime = 0;
let lastRandom = new Uint8Array(10);

function generateULID(): TransactionId {
  let now = Date.now();

  if (now === lastTime) {
    // Increment random part
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

  // Encode timestamp (first 10 chars)
  for (let i = 9; i >= 0; i--) {
    ulid = ENCODING[now % 32] + ulid;
    now = Math.floor(now / 32);
  }

  // Encode random (last 16 chars)
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

function int32Object(value: number): TypedObject {
  return { type: ObjectType.INT32, value: BigInt(value) };
}

function float64Object(value: number): TypedObject {
  return { type: ObjectType.FLOAT64, value };
}

function refObject(entityId: EntityId): TypedObject {
  return { type: ObjectType.REF, value: entityId };
}

// ============================================================================
// TSV STREAMING PARSER
// ============================================================================

class TSVStreamParser extends TransformStream<string, string[]> {
  private buffer = '';
  private headers: string[] | null = null;

  constructor() {
    let buffer = '';
    let headers: string[] | null = null;

    super({
      transform(chunk, controller) {
        buffer += chunk;
        const lines = buffer.split('\n');
        buffer = lines.pop() ?? '';

        for (const line of lines) {
          if (!line.trim()) continue;

          const fields = line.split('\t');

          // First line is headers
          if (!headers) {
            headers = fields;
            continue;
          }

          controller.enqueue(fields);
        }
      },

      flush(controller) {
        if (buffer.trim() && headers) {
          controller.enqueue(buffer.split('\t'));
        }
      },
    });
  }
}

// ============================================================================
// LINE COUNTER TRANSFORM
// ============================================================================

class LineCounter extends TransformStream<string[], string[]> {
  public count = 0;

  constructor(onProgress?: (count: number) => void) {
    let count = 0;

    super({
      transform(chunk, controller) {
        count++;
        if (onProgress && count % 100_000 === 0) {
          onProgress(count);
        }
        controller.enqueue(chunk);
      },
    });

    // Store reference to count
    Object.defineProperty(this, 'count', {
      get: () => count,
    });
  }
}

// ============================================================================
// TRIPLE GENERATORS
// ============================================================================

function* generateTitleBasicsTriples(
  fields: string[],
  txId: TransactionId
): Generator<Triple> {
  // Fields: tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres
  const [tconst, titleType, primaryTitle, , , startYear, , runtimeMinutes, genres] = fields;

  if (!tconst || tconst === '\\N') return;

  // Only process movies and TV shows
  if (titleType !== 'movie' && titleType !== 'tvMovie' && titleType !== 'tvSeries') return;

  const entityId = createEntityId(`https://imdb.com/title/${tconst}`);

  // $type predicate
  yield createTripleWithType(
    entityId,
    createPredicate('$type'),
    stringObject('Movie'),
    txId
  );

  // title
  if (primaryTitle && primaryTitle !== '\\N') {
    yield createTripleWithType(
      entityId,
      createPredicate('title'),
      stringObject(primaryTitle),
      txId
    );
  }

  // year
  if (startYear && startYear !== '\\N') {
    const year = parseInt(startYear, 10);
    if (!isNaN(year)) {
      yield createTripleWithType(
        entityId,
        createPredicate('year'),
        int32Object(year),
        txId
      );
    }
  }

  // runtime
  if (runtimeMinutes && runtimeMinutes !== '\\N') {
    const runtime = parseInt(runtimeMinutes, 10);
    if (!isNaN(runtime)) {
      yield createTripleWithType(
        entityId,
        createPredicate('runtime'),
        int32Object(runtime),
        txId
      );
    }
  }

  // genres (comma-separated in source)
  if (genres && genres !== '\\N') {
    yield createTripleWithType(
      entityId,
      createPredicate('genres'),
      stringObject(genres),
      txId
    );
  }
}

function* generateTitleRatingsTriples(
  fields: string[],
  txId: TransactionId
): Generator<Triple> {
  // Fields: tconst, averageRating, numVotes
  const [tconst, averageRating] = fields;

  if (!tconst || tconst === '\\N') return;

  const entityId = createEntityId(`https://imdb.com/title/${tconst}`);

  // rating
  if (averageRating && averageRating !== '\\N') {
    const rating = parseFloat(averageRating);
    if (!isNaN(rating)) {
      yield createTripleWithType(
        entityId,
        createPredicate('rating'),
        float64Object(rating),
        txId
      );
    }
  }
}

function* generateNameBasicsTriples(
  fields: string[],
  txId: TransactionId
): Generator<Triple> {
  // Fields: nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles
  const [nconst, primaryName, birthYear, , primaryProfession] = fields;

  if (!nconst || nconst === '\\N') return;

  const entityId = createEntityId(`https://imdb.com/name/${nconst}`);

  // $type predicate
  yield createTripleWithType(
    entityId,
    createPredicate('$type'),
    stringObject('Person'),
    txId
  );

  // name
  if (primaryName && primaryName !== '\\N') {
    yield createTripleWithType(
      entityId,
      createPredicate('name'),
      stringObject(primaryName),
      txId
    );
  }

  // birthYear
  if (birthYear && birthYear !== '\\N') {
    const year = parseInt(birthYear, 10);
    if (!isNaN(year)) {
      yield createTripleWithType(
        entityId,
        createPredicate('birthYear'),
        int32Object(year),
        txId
      );
    }
  }

  // profession
  if (primaryProfession && primaryProfession !== '\\N') {
    yield createTripleWithType(
      entityId,
      createPredicate('profession'),
      stringObject(primaryProfession),
      txId
    );
  }
}

function* generateTitlePrincipalsTriples(
  fields: string[],
  txId: TransactionId
): Generator<Triple> {
  // Fields: tconst, ordering, nconst, category, job, characters
  const [tconst, , nconst, category] = fields;

  if (!tconst || tconst === '\\N' || !nconst || nconst === '\\N') return;

  const movieId = createEntityId(`https://imdb.com/title/${tconst}`);
  const personId = createEntityId(`https://imdb.com/name/${nconst}`);

  // Create relation based on category
  if (category === 'director') {
    yield createTripleWithType(
      movieId,
      createPredicate('directedBy'),
      refObject(personId),
      txId
    );
  } else if (category === 'actor' || category === 'actress' || category === 'self') {
    yield createTripleWithType(
      movieId,
      createPredicate('starring'),
      refObject(personId),
      txId
    );
  }
}

// ============================================================================
// CHUNK WRITER
// ============================================================================

class ChunkWriter {
  private triples: Triple[] = [];
  private chunkIndex = 0;
  private totalTriples = 0;
  private entityCount = 0; // Approximate count instead of Set
  private bloomFilter: BloomFilter;
  private uploadedChunks: ChunkManifest['chunks'] = [];
  private totalBytesUploaded = 0;

  constructor(
    private bucket: R2Bucket,
    private basePath: string,
    private datasetName: string,
    private onProgress?: (info: { chunksUploaded: number; bytesUploaded: number }) => void
  ) {
    // Initialize bloom filter with dataset-specific capacity
    const capacity = BLOOM_CAPACITIES[datasetName] || 10_000_000;
    this.bloomFilter = createBloomFilter({
      capacity,
      targetFpr: 0.01,
    });
    console.log(`[ChunkWriter] Initialized with bloom capacity ${capacity.toLocaleString()} for ${datasetName}`);
  }

  async addTriple(triple: Triple): Promise<void> {
    this.triples.push(triple);
    // Use bloom filter for approximate unique counting (no memory growth)
    addToFilter(this.bloomFilter, triple.subject);

    if (this.triples.length >= CHUNK_TRIPLE_LIMIT) {
      await this.flushChunk();
    }
  }

  async addTriples(triples: Triple[]): Promise<void> {
    for (const triple of triples) {
      this.triples.push(triple);
      addToFilter(this.bloomFilter, triple.subject);
    }

    if (this.triples.length >= CHUNK_TRIPLE_LIMIT) {
      await this.flushChunk();
    }
  }

  async flushChunk(): Promise<void> {
    if (this.triples.length === 0) return;

    const chunkId = `chunk_${this.chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `${this.basePath}/chunks/${chunkId}.graphcol`;
    const bloomPath = `${this.basePath}/bloom/${chunkId}.bloom`;

    // Encode triples to GraphCol format
    const encoded = encodeGraphCol(this.triples, NAMESPACE);

    // Create chunk-specific bloom filter
    const chunkBloom = createBloomFilter({
      capacity: this.triples.length,
      targetFpr: 0.01,
    });
    const chunkEntities = new Set<string>();
    for (const triple of this.triples) {
      chunkEntities.add(triple.subject);
      addToFilter(chunkBloom, triple.subject);
    }

    // Extract unique predicates
    const predicates = [...new Set(this.triples.map((t) => t.predicate))];

    // Upload chunk and bloom in parallel
    await Promise.all([
      this.bucket.put(chunkPath, encoded, {
        customMetadata: {
          tripleCount: this.triples.length.toString(),
          predicates: predicates.join(','),
          entityCount: chunkEntities.size.toString(),
        },
      }),
      this.bucket.put(bloomPath, JSON.stringify(serializeFilter(chunkBloom)), {
        customMetadata: {
          entityCount: chunkEntities.size.toString(),
        },
      }),
    ]);

    // Track chunk info (keep minimal metadata to reduce memory)
    this.uploadedChunks.push({
      id: chunkId,
      path: chunkPath,
      bloomPath,
      tripleCount: this.triples.length,
      sizeBytes: encoded.length,
      predicates,
      createdAt: new Date().toISOString(),
    });

    this.totalTriples += this.triples.length;
    this.entityCount += chunkEntities.size;
    this.totalBytesUploaded += encoded.length;
    this.chunkIndex++;

    // Report progress (use cached total instead of reducing array)
    if (this.onProgress) {
      this.onProgress({
        chunksUploaded: this.chunkIndex,
        bytesUploaded: this.totalBytesUploaded,
      });
    }

    console.log(
      `[ChunkWriter] Uploaded ${chunkId}: ${this.triples.length} triples, ${(encoded.length / 1024).toFixed(1)}KB, total: ${this.totalTriples.toLocaleString()}`
    );

    // Clear buffer immediately to free memory
    this.triples = [];
  }

  async finalize(): Promise<{
    chunks: ChunkManifest['chunks'];
    totalTriples: number;
    totalEntities: number;
    bloomFilter: SerializedFilter;
  }> {
    // Flush any remaining triples
    await this.flushChunk();

    return {
      chunks: this.uploadedChunks,
      totalTriples: this.totalTriples,
      totalEntities: this.entityCount, // Use counter instead of Set.size
      bloomFilter: serializeFilter(this.bloomFilter),
    };
  }
}

// ============================================================================
// DATASET LOADER
// ============================================================================

async function loadDataset(
  datasetName: string,
  url: string,
  tripleGenerator: (fields: string[], txId: TransactionId) => Generator<Triple>,
  writer: ChunkWriter,
  onProgress?: (progress: LoaderProgress) => void
): Promise<{ tripleCount: number; entityCount: number }> {
  console.log(`[Loader] Starting ${datasetName} from ${url}`);

  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }

  if (!response.body) {
    throw new Error(`No response body for ${url}`);
  }

  const txId = generateULID();
  let linesProcessed = 0;
  let triplesGenerated = 0;
  // Removed entities Set - was causing memory blowup for large datasets
  // Entity count is now tracked approximately via bloom filter in ChunkWriter

  // Build streaming pipeline:
  // gzip -> text -> TSV parse -> triple generation -> chunk writing
  const decompressed = response.body.pipeThrough(new DecompressionStream('gzip'));
  const text = decompressed.pipeThrough(new TextDecoderStream());
  const tsvParser = new TSVStreamParser();
  const parsed = text.pipeThrough(tsvParser);

  const reader = parsed.getReader();

  try {
    while (true) {
      const { done, value: fields } = await reader.read();
      if (done) break;

      linesProcessed++;

      // Generate triples from this row - batch add to avoid per-triple await overhead
      const triples = [...tripleGenerator(fields, txId)];
      if (triples.length > 0) {
        await writer.addTriples(triples);
        triplesGenerated += triples.length;
      }

      // Progress logging every 100K lines
      if (linesProcessed % 100_000 === 0) {
        console.log(
          `[Loader] ${datasetName}: ${linesProcessed.toLocaleString()} lines, ${triplesGenerated.toLocaleString()} triples`
        );
        if (onProgress) {
          onProgress({
            dataset: datasetName,
            linesProcessed,
            triplesGenerated,
            chunksUploaded: 0,
            bytesUploaded: 0,
            errors: 0,
          });
        }
      }
    }
  } finally {
    reader.releaseLock();
  }

  console.log(
    `[Loader] Completed ${datasetName}: ${linesProcessed.toLocaleString()} lines, ${triplesGenerated.toLocaleString()} triples`
  );

  // Note: entityCount is now tracked approximately in ChunkWriter via bloom filter
  return {
    tripleCount: triplesGenerated,
    entityCount: 0, // Actual count will come from ChunkWriter.finalize()
  };
}

// ============================================================================
// EXPLORER HELPERS
// ============================================================================

/**
 * Convert triples to an Entity object for the explorer
 */
function triplesToEntity(triples: Triple[]): Entity | null {
  if (triples.length === 0) return null;

  const entity: Entity = { $id: triples[0].subject };

  for (const triple of triples) {
    const predicate = triple.predicate;

    // Handle $type specially
    if (predicate === '$type' && triple.object.type === ObjectType.STRING) {
      entity.$type = triple.object.value;
      continue;
    }

    // Convert typed object to value
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
        value = triple.object.value; // Already a string URL
        break;
      case ObjectType.BOOL:
        value = triple.object.value;
        break;
      case ObjectType.TIMESTAMP:
        value = new Date(Number(triple.object.value)).toISOString();
        break;
      default:
        value = String(triple.object.value);
    }

    // Handle multi-valued predicates
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

/**
 * Fetch entity by ID from R2 chunks
 */
async function getEntityFromR2(
  bucket: R2Bucket,
  entityId: string
): Promise<Entity | null> {
  // Load manifest to find chunks
  const manifestObj = await bucket.get('datasets/imdb/index.json');
  if (!manifestObj) return null;

  const manifest = await manifestObj.json<ChunkManifest>();

  // Search through chunks for the entity
  for (const chunk of manifest.chunks) {
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

/**
 * Search entities by text (simple substring match on titles/names)
 */
async function searchEntitiesInR2(
  bucket: R2Bucket,
  query: string,
  limit: number = 50
): Promise<SearchResult[]> {
  const results: SearchResult[] = [];
  const lowerQuery = query.toLowerCase();

  // Load manifest
  const manifestObj = await bucket.get('datasets/imdb/index.json');
  if (!manifestObj) return results;

  const manifest = await manifestObj.json<ChunkManifest>();

  // Search through chunks (this is slow but works for exploration)
  for (const chunk of manifest.chunks) {
    if (results.length >= limit) break;

    const chunkObj = await bucket.get(chunk.path);
    if (!chunkObj) continue;

    const data = new Uint8Array(await chunkObj.arrayBuffer());
    const triples = decodeGraphCol(data);

    // Group triples by subject
    const bySubject = new Map<string, Triple[]>();
    for (const triple of triples) {
      const existing = bySubject.get(triple.subject) || [];
      existing.push(triple);
      bySubject.set(triple.subject, existing);
    }

    // Search for matching entities
    for (const [subject, subjectTriples] of bySubject) {
      if (results.length >= limit) break;

      // Look for title or name predicate
      let label: string | undefined;
      let type: string | undefined;

      for (const triple of subjectTriples) {
        if (triple.predicate === 'title' || triple.predicate === 'name') {
          if (triple.object.type === ObjectType.STRING) {
            label = triple.object.value;
          }
        }
        if (triple.predicate === '$type' && triple.object.type === ObjectType.STRING) {
          type = triple.object.value;
        }
      }

      if (label && label.toLowerCase().includes(lowerQuery)) {
        results.push({
          $id: subject,
          $type: type,
          label,
        });
      }
    }
  }

  return results;
}

/**
 * Get a random entity ID from the dataset
 */
async function getRandomEntityIdFromR2(bucket: R2Bucket): Promise<string | null> {
  const manifestObj = await bucket.get('datasets/imdb/index.json');
  if (!manifestObj) return null;

  const manifest = await manifestObj.json<ChunkManifest>();
  if (manifest.chunks.length === 0) return null;

  // Pick a random chunk
  const randomChunk = manifest.chunks[Math.floor(Math.random() * manifest.chunks.length)];
  const chunkObj = await bucket.get(randomChunk.path);
  if (!chunkObj) return null;

  const data = new Uint8Array(await chunkObj.arrayBuffer());
  const triples = decodeGraphCol(data);
  if (triples.length === 0) return null;

  // Get unique subjects
  const subjects = [...new Set(triples.map((t) => t.subject))];
  return subjects[Math.floor(Math.random() * subjects.length)];
}

/**
 * Get total entity count from manifest
 */
async function getEntityCountFromR2(bucket: R2Bucket): Promise<number> {
  const manifestObj = await bucket.get('datasets/imdb/index.json');
  if (!manifestObj) return 0;

  const manifest = await manifestObj.json<ChunkManifest>();
  return manifest.stats.totalEntities;
}

// ============================================================================
// MAIN WORKER
// ============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const baseUrl = `${url.protocol}//${url.host}`;

    // Create explorer routes
    const explorer = createExplorerRoutes({
      namespace: 'imdb',
      displayName: 'IMDB Graph Explorer',
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

    // Handle different endpoints
    if (url.pathname === '/load' || url.pathname === '/load/all') {
      return handleLoadAll(env);
    }

    if (url.pathname === '/load/title-basics') {
      return handleLoadSingle(env, 'titleBasics');
    }

    if (url.pathname === '/load/title-ratings') {
      return handleLoadSingle(env, 'titleRatings');
    }

    if (url.pathname === '/load/name-basics') {
      return handleLoadSingle(env, 'nameBasics');
    }

    if (url.pathname === '/load/title-principals') {
      return handleLoadSingle(env, 'titlePrincipals');
    }

    if (url.pathname === '/status') {
      return handleStatus(env);
    }

    // Default: show available endpoints
    return new Response(
      JSON.stringify(
        {
          name: 'IMDB Data Loader',
          endpoints: {
            '/load': 'Load all IMDB datasets',
            '/load/title-basics': 'Load title.basics.tsv.gz only',
            '/load/title-ratings': 'Load title.ratings.tsv.gz only',
            '/load/name-basics': 'Load name.basics.tsv.gz only',
            '/load/title-principals': 'Load title.principals.tsv.gz only',
            '/status': 'Get current manifest status',
            '/explore': 'Interactive graph explorer',
            '/entity/{id}': 'View entity by ID (URL-encoded)',
            '/search?q=term': 'Search entities',
            '/random': 'Redirect to random entity',
          },
          datasets: IMDB_DATASETS,
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  },
};

async function handleLoadAll(env: Env): Promise<Response> {
  const startTime = Date.now();
  const basePath = 'datasets/imdb';

  // Initialize manifest
  const manifest: ChunkManifest = {
    version: 1,
    namespace: NAMESPACE,
    datasets: [
      {
        name: 'title.basics',
        url: IMDB_DATASETS.titleBasics,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
      {
        name: 'title.ratings',
        url: IMDB_DATASETS.titleRatings,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
      {
        name: 'name.basics',
        url: IMDB_DATASETS.nameBasics,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
      {
        name: 'title.principals',
        url: IMDB_DATASETS.titlePrincipals,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
    ],
    chunks: [],
    stats: {
      totalTriples: 0,
      totalChunks: 0,
      totalEntities: 0,
      totalSizeBytes: 0,
    },
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };

  // Create chunk writer - use 'all' since we're loading multiple datasets
  const writer = new ChunkWriter(env.LAKEHOUSE, basePath, 'titlePrincipals', (info) => {
    manifest.stats.totalChunks = info.chunksUploaded;
    manifest.stats.totalSizeBytes = info.bytesUploaded;
  });

  const datasetConfigs = [
    {
      name: 'titleBasics',
      index: 0,
      generator: generateTitleBasicsTriples,
    },
    {
      name: 'titleRatings',
      index: 1,
      generator: generateTitleRatingsTriples,
    },
    {
      name: 'nameBasics',
      index: 2,
      generator: generateNameBasicsTriples,
    },
    {
      name: 'titlePrincipals',
      index: 3,
      generator: generateTitlePrincipalsTriples,
    },
  ] as const;

  // Load each dataset sequentially
  for (const config of datasetConfigs) {
    const datasetManifest = manifest.datasets[config.index];
    if (!datasetManifest) continue;

    datasetManifest.status = 'processing';
    datasetManifest.startedAt = new Date().toISOString();

    // Update manifest in R2
    await env.LAKEHOUSE.put(`${basePath}/index.json`, JSON.stringify(manifest, null, 2));

    try {
      const datasetKey = config.name as keyof typeof IMDB_DATASETS;
      const result = await loadDataset(
        datasetManifest.name,
        IMDB_DATASETS[datasetKey],
        config.generator,
        writer
      );

      datasetManifest.status = 'completed';
      datasetManifest.tripleCount = result.tripleCount;
      datasetManifest.entityCount = result.entityCount;
      datasetManifest.completedAt = new Date().toISOString();

      manifest.stats.totalTriples += result.tripleCount;
      manifest.stats.totalEntities += result.entityCount;
    } catch (error) {
      datasetManifest.status = 'error';
      datasetManifest.error = error instanceof Error ? error.message : String(error);
      console.error(`[Loader] Error loading ${datasetManifest.name}:`, error);
    }

    manifest.updatedAt = new Date().toISOString();
  }

  // Finalize and write master bloom filter
  const finalResult = await writer.finalize();
  manifest.chunks = finalResult.chunks;

  // Upload master bloom filter
  await env.LAKEHOUSE.put(
    `${basePath}/bloom/master.bloom`,
    JSON.stringify(finalResult.bloomFilter)
  );

  // Final manifest update
  manifest.updatedAt = new Date().toISOString();
  await env.LAKEHOUSE.put(`${basePath}/index.json`, JSON.stringify(manifest, null, 2));

  const duration = (Date.now() - startTime) / 1000;

  return new Response(
    JSON.stringify(
      {
        success: true,
        duration: `${duration.toFixed(1)}s`,
        stats: manifest.stats,
        datasets: manifest.datasets.map((d) => ({
          name: d.name,
          status: d.status,
          tripleCount: d.tripleCount,
          entityCount: d.entityCount,
          error: d.error,
        })),
      },
      null,
      2
    ),
    {
      headers: { 'Content-Type': 'application/json' },
    }
  );
}

async function handleLoadSingle(
  env: Env,
  datasetKey: keyof typeof IMDB_DATASETS
): Promise<Response> {
  const startTime = Date.now();
  const basePath = `datasets/imdb/${datasetKey}`;

  const generators: Record<
    keyof typeof IMDB_DATASETS,
    (fields: string[], txId: TransactionId) => Generator<Triple>
  > = {
    titleBasics: generateTitleBasicsTriples,
    titleRatings: generateTitleRatingsTriples,
    nameBasics: generateNameBasicsTriples,
    titlePrincipals: generateTitlePrincipalsTriples,
  };

  const writer = new ChunkWriter(env.LAKEHOUSE, basePath, datasetKey);

  try {
    const result = await loadDataset(
      datasetKey,
      IMDB_DATASETS[datasetKey],
      generators[datasetKey],
      writer
    );

    const finalResult = await writer.finalize();

    // Upload bloom filter
    await env.LAKEHOUSE.put(
      `${basePath}/bloom/master.bloom`,
      JSON.stringify(finalResult.bloomFilter)
    );

    // Upload manifest
    const manifest = {
      version: 1,
      namespace: NAMESPACE,
      dataset: datasetKey,
      chunks: finalResult.chunks,
      stats: {
        totalTriples: finalResult.totalTriples,
        totalChunks: finalResult.chunks.length,
        totalEntities: finalResult.totalEntities,
        totalSizeBytes: finalResult.chunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      },
      createdAt: new Date().toISOString(),
    };

    await env.LAKEHOUSE.put(`${basePath}/index.json`, JSON.stringify(manifest, null, 2));

    const duration = (Date.now() - startTime) / 1000;

    return new Response(
      JSON.stringify(
        {
          success: true,
          dataset: datasetKey,
          duration: `${duration.toFixed(1)}s`,
          stats: manifest.stats,
        },
        null,
        2
      ),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  } catch (error) {
    return new Response(
      JSON.stringify({
        success: false,
        dataset: datasetKey,
        error: error instanceof Error ? error.message : String(error),
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }
}

async function handleStatus(env: Env): Promise<Response> {
  try {
    const manifest = await env.LAKEHOUSE.get('datasets/imdb/index.json');
    if (!manifest) {
      return new Response(
        JSON.stringify({ status: 'not_loaded', message: 'No IMDB data loaded yet' }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    }

    const data = await manifest.json<ChunkManifest>();
    return new Response(JSON.stringify(data, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    return new Response(
      JSON.stringify({
        status: 'error',
        error: error instanceof Error ? error.message : String(error),
      }),
      {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }
}
