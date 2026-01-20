/**
 * Wikidata Data Loader Worker
 *
 * Streams Wikidata entities via SPARQL endpoint to R2 as GraphCol chunks.
 * Uses pagination through SPARQL results - fetches 10K entities per request.
 *
 * Entity Model:
 * - Entity: $id=https://wikidata.org/entity/{qid}
 *   - label (STRING), description (STRING), instanceOf (REF)
 * - Property: $id=https://wikidata.org/property/{pid}
 *   - label (STRING), datatype (STRING)
 * - Claims as separate triples with predicates mapped from property IDs
 *
 * Target: 1M entities, ~10M triples
 *
 * Endpoints:
 *   GET /load/humans - Load humans (Q5 instances) - largest subset
 *   GET /load/films - Load films (Q11424 instances)
 *   GET /load/cities - Load cities (Q515 instances)
 *   GET /load - Load all configured subsets
 *   GET /status - Get current manifest status
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

// ============================================================================
// CONSTANTS
// ============================================================================

const WIKIDATA_SPARQL_ENDPOINT = 'https://query.wikidata.org/sparql';
const NAMESPACE = 'https://wikidata.org/' as Namespace;
const CHUNK_TRIPLE_LIMIT = 50_000;
const BATCH_SIZE = 1_000; // SPARQL query batch size - keep small to avoid 504 timeouts
const MAX_ENTITIES = 1_000_000; // Stop at 1M entities
const MAX_RETRIES = 5; // Max retries for SPARQL queries
const INITIAL_BACKOFF_MS = 1000; // Initial backoff for retries

// Wikidata class QIDs for entity subsets
const WIKIDATA_CLASSES = {
  human: 'Q5', // ~9M humans
  film: 'Q11424', // ~500K films
  city: 'Q515', // ~500K cities
  book: 'Q571', // ~1M books
  company: 'Q4830453', // ~1M companies
  country: 'Q6256', // ~200 countries
  university: 'Q3918', // ~30K universities
  musician: 'Q639669', // ~200K musicians
  scientist: 'Q901', // ~100K scientists
  politician: 'Q82955', // ~500K politicians
} as const;

// Target entity counts per subset (for reasonable subset)
const TARGET_COUNTS: Record<string, number> = {
  human: 500_000, // 500K humans for feasibility
  film: 100_000,
  city: 50_000,
  book: 50_000,
  company: 50_000,
  country: 300, // All countries
  university: 30_000,
  musician: 50_000,
  scientist: 50_000,
  politician: 100_000,
};

// ============================================================================
// TYPES
// ============================================================================

interface Env {
  LAKEHOUSE: R2Bucket;
}

interface WikidataSparqlResult {
  head: {
    vars: string[];
  };
  results: {
    bindings: WikidataBinding[];
  };
}

interface WikidataBinding {
  item: { type: 'uri'; value: string };
  itemLabel?: { type: 'literal'; value: string; 'xml:lang'?: string };
  itemDescription?: { type: 'literal'; value: string; 'xml:lang'?: string };
  instanceOf?: { type: 'uri'; value: string };
  instanceOfLabel?: { type: 'literal'; value: string };
  // Additional claim properties (varies by query)
  [key: string]: { type: string; value: string; datatype?: string; 'xml:lang'?: string } | undefined;
}

interface ChunkManifest {
  version: 1;
  namespace: string;
  subsets: {
    name: string;
    classQid: string;
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
  subset: string;
  entitiesFetched: number;
  triplesGenerated: number;
  chunksUploaded: number;
  bytesUploaded: number;
  errors: number;
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

function int32Object(value: number): TypedObject {
  return { type: ObjectType.INT32, value: BigInt(value) };
}

function float64Object(value: number): TypedObject {
  return { type: ObjectType.FLOAT64, value };
}

function refObject(entityId: EntityId): TypedObject {
  return { type: ObjectType.REF, value: entityId };
}

function timestampObject(value: Date): TypedObject {
  return { type: ObjectType.TIMESTAMP, value: BigInt(value.getTime()) };
}

function geoPointObject(lat: number, lng: number): TypedObject {
  return { type: ObjectType.GEO_POINT, value: { lat, lng } };
}

// ============================================================================
// SPARQL QUERY BUILDER
// ============================================================================

/**
 * Build SPARQL query to fetch entities of a given class with pagination
 */
function buildEntityQuery(classQid: string, limit: number, offset: number): string {
  return `
SELECT DISTINCT ?item ?itemLabel ?itemDescription ?instanceOf ?instanceOfLabel WHERE {
  ?item wdt:P31 wd:${classQid} .
  OPTIONAL { ?item wdt:P31 ?instanceOf . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
LIMIT ${limit}
OFFSET ${offset}
`.trim();
}

/**
 * Build SPARQL query to fetch additional claims for entities
 * Fetches birth date, death date, coordinates, image, website for humans
 */
function buildClaimsQuery(qids: string[], classQid: string): string {
  const valuesClause = qids.map((qid) => `wd:${qid}`).join(' ');

  // Properties vary by class type
  if (classQid === WIKIDATA_CLASSES.human) {
    return `
SELECT ?item ?birthDate ?deathDate ?birthPlace ?birthPlaceLabel ?occupation ?occupationLabel ?citizenship ?citizenshipLabel ?gender ?genderLabel WHERE {
  VALUES ?item { ${valuesClause} }
  OPTIONAL { ?item wdt:P569 ?birthDate . }
  OPTIONAL { ?item wdt:P570 ?deathDate . }
  OPTIONAL { ?item wdt:P19 ?birthPlace . }
  OPTIONAL { ?item wdt:P106 ?occupation . }
  OPTIONAL { ?item wdt:P27 ?citizenship . }
  OPTIONAL { ?item wdt:P21 ?gender . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
`.trim();
  }

  if (classQid === WIKIDATA_CLASSES.film) {
    return `
SELECT ?item ?publicationDate ?director ?directorLabel ?duration ?genre ?genreLabel ?country ?countryLabel WHERE {
  VALUES ?item { ${valuesClause} }
  OPTIONAL { ?item wdt:P577 ?publicationDate . }
  OPTIONAL { ?item wdt:P57 ?director . }
  OPTIONAL { ?item wdt:P2047 ?duration . }
  OPTIONAL { ?item wdt:P136 ?genre . }
  OPTIONAL { ?item wdt:P495 ?country . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
`.trim();
  }

  if (classQid === WIKIDATA_CLASSES.city || classQid === WIKIDATA_CLASSES.country) {
    return `
SELECT ?item ?coordinate ?population ?country ?countryLabel ?area WHERE {
  VALUES ?item { ${valuesClause} }
  OPTIONAL { ?item wdt:P625 ?coordinate . }
  OPTIONAL { ?item wdt:P1082 ?population . }
  OPTIONAL { ?item wdt:P17 ?country . }
  OPTIONAL { ?item wdt:P2046 ?area . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
`.trim();
  }

  // Default: minimal additional claims
  return `
SELECT ?item ?coordinate ?website WHERE {
  VALUES ?item { ${valuesClause} }
  OPTIONAL { ?item wdt:P625 ?coordinate . }
  OPTIONAL { ?item wdt:P856 ?website . }
}
`.trim();
}

// ============================================================================
// SPARQL EXECUTOR
// ============================================================================

/**
 * Execute a SPARQL query with exponential backoff retry logic.
 * Handles 429 (rate limit), 504 (gateway timeout), and other transient errors.
 */
async function executeSparqlQuery(query: string, retries = MAX_RETRIES): Promise<WikidataSparqlResult> {
  const url = new URL(WIKIDATA_SPARQL_ENDPOINT);
  url.searchParams.set('query', query);
  url.searchParams.set('format', 'json');

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url.toString(), {
        headers: {
          Accept: 'application/sparql-results+json',
          'User-Agent': 'GraphDB-Wikidata-Loader/1.0 (https://graphdb.workers.do; graphdb@workers.do)',
        },
      });

      // Handle rate limiting (429) - exponential backoff
      if (response.status === 429) {
        const waitTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.log(`[SPARQL] Rate limited (429), attempt ${attempt}/${retries}, waiting ${waitTime}ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
        continue;
      }

      // Handle gateway timeout (504) - exponential backoff
      if (response.status === 504) {
        const waitTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.log(`[SPARQL] Gateway timeout (504), attempt ${attempt}/${retries}, waiting ${waitTime}ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
        continue;
      }

      // Handle service unavailable (503) - exponential backoff
      if (response.status === 503) {
        const waitTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.log(`[SPARQL] Service unavailable (503), attempt ${attempt}/${retries}, waiting ${waitTime}ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
        continue;
      }

      // Handle other server errors (5xx) - exponential backoff
      if (response.status >= 500) {
        const waitTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.log(`[SPARQL] Server error (${response.status}), attempt ${attempt}/${retries}, waiting ${waitTime}ms...`);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
        continue;
      }

      if (!response.ok) {
        throw new Error(`SPARQL query failed: ${response.status} ${response.statusText}`);
      }

      return (await response.json()) as WikidataSparqlResult;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      if (attempt < retries) {
        const waitTime = INITIAL_BACKOFF_MS * Math.pow(2, attempt - 1);
        console.log(`[SPARQL] Network error, attempt ${attempt}/${retries}, waiting ${waitTime}ms...`, lastError.message);
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }
  }

  throw lastError ?? new Error('SPARQL query failed after retries');
}

// ============================================================================
// ENTITY ID HELPERS
// ============================================================================

function extractQid(uri: string): string | null {
  // Extract QID from Wikidata URI: http://www.wikidata.org/entity/Q123 -> Q123
  const match = uri.match(/\/entity\/(Q\d+)$/);
  return match ? match[1] : null;
}

function extractPid(uri: string): string | null {
  // Extract property ID: http://www.wikidata.org/prop/direct/P123 -> P123
  const match = uri.match(/(?:\/prop\/direct\/|\/property\/)(P\d+)$/);
  return match ? match[1] : null;
}

function wikidataEntityId(qid: string): EntityId {
  return createEntityId(`https://wikidata.org/entity/${qid}`);
}

function wikidataPropertyId(pid: string): EntityId {
  return createEntityId(`https://wikidata.org/property/${pid}`);
}

// ============================================================================
// TRIPLE GENERATORS
// ============================================================================

function* generateEntityTriples(
  binding: WikidataBinding,
  txId: TransactionId,
  entityTypeName: string
): Generator<Triple> {
  const qid = extractQid(binding.item.value);
  if (!qid) return;

  const entityId = wikidataEntityId(qid);

  // $type predicate - using the subset name as type
  yield createTripleWithType(entityId, createPredicate('$type'), stringObject(entityTypeName), txId);

  // label
  if (binding.itemLabel?.value && binding.itemLabel.value !== qid) {
    yield createTripleWithType(entityId, createPredicate('label'), stringObject(binding.itemLabel.value), txId);
  }

  // description
  if (binding.itemDescription?.value) {
    yield createTripleWithType(
      entityId,
      createPredicate('description'),
      stringObject(binding.itemDescription.value),
      txId
    );
  }

  // instanceOf (REF to another Wikidata entity)
  if (binding.instanceOf?.value) {
    const instanceOfQid = extractQid(binding.instanceOf.value);
    if (instanceOfQid) {
      yield createTripleWithType(entityId, createPredicate('instanceOf'), refObject(wikidataEntityId(instanceOfQid)), txId);

      // Also add instanceOf label for convenience
      if (binding.instanceOfLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('instanceOfLabel'), stringObject(binding.instanceOfLabel.value), txId);
      }
    }
  }
}

function* generateHumanClaimTriples(binding: WikidataBinding, txId: TransactionId): Generator<Triple> {
  const qid = extractQid(binding.item.value);
  if (!qid) return;

  const entityId = wikidataEntityId(qid);

  // Birth date
  if (binding.birthDate?.value) {
    try {
      const date = new Date(binding.birthDate.value);
      if (!isNaN(date.getTime())) {
        yield createTripleWithType(entityId, createPredicate('birthDate'), timestampObject(date), txId);
      }
    } catch {
      // Invalid date format
    }
  }

  // Death date
  if (binding.deathDate?.value) {
    try {
      const date = new Date(binding.deathDate.value);
      if (!isNaN(date.getTime())) {
        yield createTripleWithType(entityId, createPredicate('deathDate'), timestampObject(date), txId);
      }
    } catch {
      // Invalid date format
    }
  }

  // Birth place (REF)
  if (binding.birthPlace?.value) {
    const placeQid = extractQid(binding.birthPlace.value);
    if (placeQid) {
      yield createTripleWithType(entityId, createPredicate('birthPlace'), refObject(wikidataEntityId(placeQid)), txId);
      if (binding.birthPlaceLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('birthPlaceLabel'), stringObject(binding.birthPlaceLabel.value), txId);
      }
    }
  }

  // Occupation (REF)
  if (binding.occupation?.value) {
    const occQid = extractQid(binding.occupation.value);
    if (occQid) {
      yield createTripleWithType(entityId, createPredicate('occupation'), refObject(wikidataEntityId(occQid)), txId);
      if (binding.occupationLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('occupationLabel'), stringObject(binding.occupationLabel.value), txId);
      }
    }
  }

  // Citizenship (REF)
  if (binding.citizenship?.value) {
    const citizenQid = extractQid(binding.citizenship.value);
    if (citizenQid) {
      yield createTripleWithType(entityId, createPredicate('citizenship'), refObject(wikidataEntityId(citizenQid)), txId);
      if (binding.citizenshipLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('citizenshipLabel'), stringObject(binding.citizenshipLabel.value), txId);
      }
    }
  }

  // Gender
  if (binding.gender?.value) {
    const genderQid = extractQid(binding.gender.value);
    if (genderQid) {
      yield createTripleWithType(entityId, createPredicate('gender'), refObject(wikidataEntityId(genderQid)), txId);
      if (binding.genderLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('genderLabel'), stringObject(binding.genderLabel.value), txId);
      }
    }
  }
}

function* generateFilmClaimTriples(binding: WikidataBinding, txId: TransactionId): Generator<Triple> {
  const qid = extractQid(binding.item.value);
  if (!qid) return;

  const entityId = wikidataEntityId(qid);

  // Publication date
  if (binding.publicationDate?.value) {
    try {
      const date = new Date(binding.publicationDate.value);
      if (!isNaN(date.getTime())) {
        yield createTripleWithType(entityId, createPredicate('releaseDate'), timestampObject(date), txId);
      }
    } catch {
      // Invalid date
    }
  }

  // Director (REF)
  if (binding.director?.value) {
    const dirQid = extractQid(binding.director.value);
    if (dirQid) {
      yield createTripleWithType(entityId, createPredicate('director'), refObject(wikidataEntityId(dirQid)), txId);
      if (binding.directorLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('directorLabel'), stringObject(binding.directorLabel.value), txId);
      }
    }
  }

  // Duration
  if (binding.duration?.value) {
    const duration = parseInt(binding.duration.value, 10);
    if (!isNaN(duration)) {
      yield createTripleWithType(entityId, createPredicate('duration'), int32Object(duration), txId);
    }
  }

  // Genre (REF)
  if (binding.genre?.value) {
    const genreQid = extractQid(binding.genre.value);
    if (genreQid) {
      yield createTripleWithType(entityId, createPredicate('genre'), refObject(wikidataEntityId(genreQid)), txId);
      if (binding.genreLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('genreLabel'), stringObject(binding.genreLabel.value), txId);
      }
    }
  }

  // Country (REF)
  if (binding.country?.value) {
    const countryQid = extractQid(binding.country.value);
    if (countryQid) {
      yield createTripleWithType(entityId, createPredicate('country'), refObject(wikidataEntityId(countryQid)), txId);
      if (binding.countryLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('countryLabel'), stringObject(binding.countryLabel.value), txId);
      }
    }
  }
}

function* generateGeoClaimTriples(binding: WikidataBinding, txId: TransactionId): Generator<Triple> {
  const qid = extractQid(binding.item.value);
  if (!qid) return;

  const entityId = wikidataEntityId(qid);

  // Coordinate (GEO_POINT)
  if (binding.coordinate?.value) {
    // Parse WKT Point: Point(lng lat)
    const match = binding.coordinate.value.match(/Point\(([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\)/i);
    if (match) {
      const lng = parseFloat(match[1]);
      const lat = parseFloat(match[2]);
      if (!isNaN(lat) && !isNaN(lng)) {
        yield createTripleWithType(entityId, createPredicate('coordinate'), geoPointObject(lat, lng), txId);
      }
    }
  }

  // Population
  if (binding.population?.value) {
    const pop = parseInt(binding.population.value, 10);
    if (!isNaN(pop)) {
      yield createTripleWithType(entityId, createPredicate('population'), int32Object(pop), txId);
    }
  }

  // Country (REF)
  if (binding.country?.value) {
    const countryQid = extractQid(binding.country.value);
    if (countryQid) {
      yield createTripleWithType(entityId, createPredicate('country'), refObject(wikidataEntityId(countryQid)), txId);
      if (binding.countryLabel?.value) {
        yield createTripleWithType(entityId, createPredicate('countryLabel'), stringObject(binding.countryLabel.value), txId);
      }
    }
  }

  // Area
  if (binding.area?.value) {
    const area = parseFloat(binding.area.value);
    if (!isNaN(area)) {
      yield createTripleWithType(entityId, createPredicate('area'), float64Object(area), txId);
    }
  }
}

// ============================================================================
// CHUNK WRITER
// ============================================================================

class ChunkWriter {
  private triples: Triple[] = [];
  private chunkIndex = 0;
  private totalTriples = 0;
  private entities = new Set<string>();
  private bloomFilter: BloomFilter;
  private uploadedChunks: ChunkManifest['chunks'] = [];

  constructor(
    private bucket: R2Bucket,
    private basePath: string,
    private onProgress?: (info: { chunksUploaded: number; bytesUploaded: number }) => void
  ) {
    this.bloomFilter = createBloomFilter({
      capacity: 1_000_000,
      targetFpr: 0.01,
    });
  }

  async addTriple(triple: Triple): Promise<void> {
    this.triples.push(triple);
    this.entities.add(triple.subject);
    addToFilter(this.bloomFilter, triple.subject);

    if (this.triples.length >= CHUNK_TRIPLE_LIMIT) {
      await this.flushChunk();
    }
  }

  async flushChunk(): Promise<void> {
    if (this.triples.length === 0) return;

    const chunkId = `chunk_${this.chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `${this.basePath}/chunks/${chunkId}.graphcol`;
    const bloomPath = `${this.basePath}/bloom/${chunkId}.bloom`;

    const encoded = encodeGraphCol(this.triples, NAMESPACE);

    const chunkBloom = createBloomFilter({
      capacity: this.triples.length,
      targetFpr: 0.01,
    });
    const chunkEntities = new Set<string>();
    for (const triple of this.triples) {
      chunkEntities.add(triple.subject);
      addToFilter(chunkBloom, triple.subject);
    }

    const predicates = [...new Set(this.triples.map((t) => t.predicate))];

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
    this.chunkIndex++;

    if (this.onProgress) {
      this.onProgress({
        chunksUploaded: this.chunkIndex,
        bytesUploaded: this.uploadedChunks.reduce((sum, c) => sum + c.sizeBytes, 0),
      });
    }

    console.log(`[ChunkWriter] Uploaded ${chunkId}: ${this.triples.length} triples, ${(encoded.length / 1024).toFixed(1)}KB`);

    this.triples = [];
  }

  async finalize(): Promise<{
    chunks: ChunkManifest['chunks'];
    totalTriples: number;
    totalEntities: number;
    bloomFilter: SerializedFilter;
  }> {
    await this.flushChunk();

    return {
      chunks: this.uploadedChunks,
      totalTriples: this.totalTriples,
      totalEntities: this.entities.size,
      bloomFilter: serializeFilter(this.bloomFilter),
    };
  }
}

// ============================================================================
// SUBSET LOADER
// ============================================================================

async function loadSubset(
  subsetName: string,
  classQid: string,
  targetCount: number,
  writer: ChunkWriter,
  onProgress?: (progress: LoaderProgress) => void
): Promise<{ tripleCount: number; entityCount: number }> {
  console.log(`[Loader] Starting ${subsetName} (${classQid}) - target: ${targetCount.toLocaleString()} entities`);

  const txId = generateULID();
  let entitiesFetched = 0;
  let triplesGenerated = 0;
  const entityQids = new Set<string>();

  // Entity type name for $type predicate (capitalize first letter)
  const entityTypeName = subsetName.charAt(0).toUpperCase() + subsetName.slice(1);

  // Phase 1: Fetch basic entity info with pagination
  let offset = 0;
  while (entitiesFetched < targetCount) {
    const batchSize = Math.min(BATCH_SIZE, targetCount - entitiesFetched);

    console.log(`[Loader] Fetching ${subsetName} batch: offset=${offset}, limit=${batchSize}`);

    const query = buildEntityQuery(classQid, batchSize, offset);
    const result = await executeSparqlQuery(query);

    if (result.results.bindings.length === 0) {
      console.log(`[Loader] No more ${subsetName} entities available`);
      break;
    }

    // Collect QIDs for claims query
    const batchQids: string[] = [];

    for (const binding of result.results.bindings) {
      const qid = extractQid(binding.item.value);
      if (!qid || entityQids.has(qid)) continue;

      entityQids.add(qid);
      batchQids.push(qid);
      entitiesFetched++;

      // Generate entity triples
      for (const triple of generateEntityTriples(binding, txId, entityTypeName)) {
        await writer.addTriple(triple);
        triplesGenerated++;
      }
    }

    // Phase 2: Fetch additional claims for this batch
    if (batchQids.length > 0) {
      await fetchAndWriteClaims(batchQids, classQid, writer, txId, (count) => {
        triplesGenerated += count;
      });
    }

    offset += batchSize;

    // Progress logging
    console.log(
      `[Loader] ${subsetName}: ${entitiesFetched.toLocaleString()} entities, ${triplesGenerated.toLocaleString()} triples`
    );

    if (onProgress) {
      onProgress({
        subset: subsetName,
        entitiesFetched,
        triplesGenerated,
        chunksUploaded: 0,
        bytesUploaded: 0,
        errors: 0,
      });
    }

    // Rate limiting: wait between batches
    await new Promise((resolve) => setTimeout(resolve, 500));
  }

  console.log(
    `[Loader] Completed ${subsetName}: ${entitiesFetched.toLocaleString()} entities, ${triplesGenerated.toLocaleString()} triples`
  );

  return {
    tripleCount: triplesGenerated,
    entityCount: entitiesFetched,
  };
}

async function fetchAndWriteClaims(
  qids: string[],
  classQid: string,
  writer: ChunkWriter,
  txId: TransactionId,
  onTriples: (count: number) => void
): Promise<void> {
  // Batch QIDs into chunks of 100 to avoid query timeouts
  const CLAIM_BATCH_SIZE = 100;

  for (let i = 0; i < qids.length; i += CLAIM_BATCH_SIZE) {
    const batchQids = qids.slice(i, i + CLAIM_BATCH_SIZE);
    const query = buildClaimsQuery(batchQids, classQid);

    try {
      const result = await executeSparqlQuery(query);
      let tripleCount = 0;

      for (const binding of result.results.bindings) {
        // Select appropriate claim generator based on class
        let generator: Generator<Triple>;

        if (classQid === WIKIDATA_CLASSES.human) {
          generator = generateHumanClaimTriples(binding, txId);
        } else if (classQid === WIKIDATA_CLASSES.film) {
          generator = generateFilmClaimTriples(binding, txId);
        } else if (classQid === WIKIDATA_CLASSES.city || classQid === WIKIDATA_CLASSES.country) {
          generator = generateGeoClaimTriples(binding, txId);
        } else {
          // Default: geo claims only
          generator = generateGeoClaimTriples(binding, txId);
        }

        for (const triple of generator) {
          await writer.addTriple(triple);
          tripleCount++;
        }
      }

      onTriples(tripleCount);
    } catch (error) {
      console.warn(`[Loader] Claims query failed for batch ${i}-${i + CLAIM_BATCH_SIZE}:`, error);
      // Continue with other batches
    }

    // Rate limiting between claim batches
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
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
      case ObjectType.TIMESTAMP:
        value = new Date(Number(triple.object.value)).toISOString();
        break;
      case ObjectType.GEO_POINT:
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
  const indexObj = await bucket.get('datasets/wikidata/index.json');
  if (!indexObj) return null;

  const manifest = await indexObj.json<ChunkManifest>();

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

async function searchEntitiesInR2(
  bucket: R2Bucket,
  query: string,
  limit: number = 50
): Promise<SearchResult[]> {
  const results: SearchResult[] = [];
  const lowerQuery = query.toLowerCase();

  const indexObj = await bucket.get('datasets/wikidata/index.json');
  if (!indexObj) return results;

  const manifest = await indexObj.json<ChunkManifest>();

  for (const chunk of manifest.chunks) {
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

      let label: string | undefined;
      let type: string | undefined;
      let description: string | undefined;

      for (const triple of subjectTriples) {
        if (triple.predicate === 'label' && triple.object.type === ObjectType.STRING) {
          label = triple.object.value;
        }
        if (triple.predicate === '$type' && triple.object.type === ObjectType.STRING) {
          type = triple.object.value;
        }
        if (triple.predicate === 'description' && triple.object.type === ObjectType.STRING) {
          description = triple.object.value;
        }
      }

      if (label && label.toLowerCase().includes(lowerQuery)) {
        results.push({ $id: subject, $type: type, label, description });
      }
    }
  }

  return results;
}

async function getRandomEntityIdFromR2(bucket: R2Bucket): Promise<string | null> {
  const indexObj = await bucket.get('datasets/wikidata/index.json');
  if (!indexObj) return null;

  const manifest = await indexObj.json<ChunkManifest>();
  if (manifest.chunks.length === 0) return null;

  const randomChunk = manifest.chunks[Math.floor(Math.random() * manifest.chunks.length)];
  const chunkObj = await bucket.get(randomChunk.path);
  if (!chunkObj) return null;

  const data = new Uint8Array(await chunkObj.arrayBuffer());
  const triples = decodeGraphCol(data);
  if (triples.length === 0) return null;

  const subjects = [...new Set(triples.map((t) => t.subject))];
  return subjects[Math.floor(Math.random() * subjects.length)];
}

async function getEntityCountFromR2(bucket: R2Bucket): Promise<number> {
  const indexObj = await bucket.get('datasets/wikidata/index.json');
  if (!indexObj) return 0;

  const manifest = await indexObj.json<ChunkManifest>();
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
      namespace: 'wikidata',
      displayName: 'Wikidata Graph Explorer',
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

    if (url.pathname === '/load/humans') {
      return handleLoadSingle(env, 'human');
    }

    if (url.pathname === '/load/films') {
      return handleLoadSingle(env, 'film');
    }

    if (url.pathname === '/load/cities') {
      return handleLoadSingle(env, 'city');
    }

    if (url.pathname === '/load/countries') {
      return handleLoadSingle(env, 'country');
    }

    if (url.pathname === '/status') {
      return handleStatus(env);
    }

    // Default: show available endpoints
    return new Response(
      JSON.stringify(
        {
          name: 'Wikidata Data Loader',
          endpoints: {
            '/load': 'Load all Wikidata subsets (humans, films, cities)',
            '/load/humans': 'Load humans only (500K target)',
            '/load/films': 'Load films only (100K target)',
            '/load/cities': 'Load cities only (50K target)',
            '/load/countries': 'Load countries only (~300)',
            '/status': 'Get current manifest status',
            '/explore': 'Interactive graph explorer',
            '/entity/{id}': 'View entity by ID (URL-encoded)',
            '/search?q=term': 'Search entities',
            '/random': 'Redirect to random entity',
          },
          subsets: Object.entries(WIKIDATA_CLASSES).map(([name, qid]) => ({
            name,
            qid,
            target: TARGET_COUNTS[name] ?? 0,
          })),
          limits: {
            maxEntities: MAX_ENTITIES,
            batchSize: BATCH_SIZE,
            chunkTripleLimit: CHUNK_TRIPLE_LIMIT,
          },
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
  const basePath = 'datasets/wikidata';

  // Initialize manifest with primary subsets
  const manifest: ChunkManifest = {
    version: 1,
    namespace: NAMESPACE,
    subsets: [
      {
        name: 'human',
        classQid: WIKIDATA_CLASSES.human,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
      {
        name: 'film',
        classQid: WIKIDATA_CLASSES.film,
        status: 'pending',
        chunks: [],
        tripleCount: 0,
        entityCount: 0,
      },
      {
        name: 'city',
        classQid: WIKIDATA_CLASSES.city,
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

  const writer = new ChunkWriter(env.LAKEHOUSE, basePath, (info) => {
    manifest.stats.totalChunks = info.chunksUploaded;
    manifest.stats.totalSizeBytes = info.bytesUploaded;
  });

  // Load each subset sequentially
  for (const subset of manifest.subsets) {
    subset.status = 'processing';
    subset.startedAt = new Date().toISOString();

    await env.LAKEHOUSE.put(`${basePath}/index.json`, JSON.stringify(manifest, null, 2));

    try {
      const result = await loadSubset(subset.name, subset.classQid, TARGET_COUNTS[subset.name] ?? 10000, writer);

      subset.status = 'completed';
      subset.tripleCount = result.tripleCount;
      subset.entityCount = result.entityCount;
      subset.completedAt = new Date().toISOString();

      manifest.stats.totalTriples += result.tripleCount;
      manifest.stats.totalEntities += result.entityCount;
    } catch (error) {
      subset.status = 'error';
      subset.error = error instanceof Error ? error.message : String(error);
      console.error(`[Loader] Error loading ${subset.name}:`, error);
    }

    manifest.updatedAt = new Date().toISOString();
  }

  // Finalize
  const finalResult = await writer.finalize();
  manifest.chunks = finalResult.chunks;

  // Upload master bloom filter
  await env.LAKEHOUSE.put(`${basePath}/bloom/master.bloom`, JSON.stringify(finalResult.bloomFilter));

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
        subsets: manifest.subsets.map((s) => ({
          name: s.name,
          status: s.status,
          tripleCount: s.tripleCount,
          entityCount: s.entityCount,
          error: s.error,
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

async function handleLoadSingle(env: Env, subsetName: keyof typeof WIKIDATA_CLASSES): Promise<Response> {
  const startTime = Date.now();
  const basePath = `datasets/wikidata/${subsetName}`;

  const classQid = WIKIDATA_CLASSES[subsetName];
  const targetCount = TARGET_COUNTS[subsetName] ?? 10000;

  const writer = new ChunkWriter(env.LAKEHOUSE, basePath);

  try {
    const result = await loadSubset(subsetName, classQid, targetCount, writer);

    const finalResult = await writer.finalize();

    // Upload bloom filter
    await env.LAKEHOUSE.put(`${basePath}/bloom/master.bloom`, JSON.stringify(finalResult.bloomFilter));

    // Upload manifest
    const manifest = {
      version: 1,
      namespace: NAMESPACE,
      subset: subsetName,
      classQid,
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
          subset: subsetName,
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
        subset: subsetName,
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
    const manifest = await env.LAKEHOUSE.get('datasets/wikidata/index.json');
    if (!manifest) {
      return new Response(JSON.stringify({ status: 'not_loaded', message: 'No Wikidata data loaded yet' }), {
        headers: { 'Content-Type': 'application/json' },
      });
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
