#!/usr/bin/env npx tsx
/**
 * Local IMDB Data Loader
 *
 * Runs locally to bypass Worker CPU time limits.
 * Streams IMDB datasets, converts to GraphCol, uploads to R2 via wrangler.
 *
 * Usage:
 *   npx tsx scripts/local-imdb-loader.ts [dataset]
 *
 * Datasets: title-basics, title-ratings, name-basics, title-principals, all
 */

import { createReadStream, createWriteStream, existsSync, mkdirSync, unlinkSync, readdirSync } from 'fs';
import { createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable, Transform } from 'stream';
import { execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const IMDB_DATASETS = {
  titleBasics: 'https://datasets.imdbws.com/title.basics.tsv.gz',
  titleRatings: 'https://datasets.imdbws.com/title.ratings.tsv.gz',
  nameBasics: 'https://datasets.imdbws.com/name.basics.tsv.gz',
  titlePrincipals: 'https://datasets.imdbws.com/title.principals.tsv.gz',
} as const;

const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://imdb.com/';
const CHUNK_TRIPLE_LIMIT = 250_000;
const TEMP_DIR = '/tmp/imdb-loader';

// Object types for GraphCol encoding
const ObjectType = {
  NULL: 0,
  BOOL: 1,
  INT32: 2,
  INT64: 3,
  FLOAT64: 4,
  STRING: 5,
  BYTES: 6,
  TIMESTAMP: 7,
  REF: 10,
  GEO_POINT: 11,
} as const;

interface Triple {
  subject: string;
  predicate: string;
  object: {
    type: number;
    value: any;
  };
  timestamp: bigint;
  txId: string;
}

// ============================================================================
// ULID GENERATOR
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
let lastTime = 0;
let lastRandom = new Uint8Array(10);

function generateULID(): string {
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
    for (let i = 0; i < lastRandom.length; i++) {
      lastRandom[i] = Math.floor(Math.random() * 256);
    }
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

  return ulid.slice(0, 26);
}

// ============================================================================
// SIMPLE GRAPHCOL ENCODER
// ============================================================================

function encodeGraphCol(triples: Triple[], namespace: string): Uint8Array {
  // Simple JSON-based encoding for now (can be optimized later)
  const data = {
    version: 1,
    namespace,
    triples: triples.map(t => ({
      s: t.subject,
      p: t.predicate,
      o: { t: t.object.type, v: t.object.type === ObjectType.INT32 || t.object.type === ObjectType.INT64
        ? Number(t.object.value)
        : t.object.value },
      ts: Number(t.timestamp),
      tx: t.txId,
    })),
  };
  return new TextEncoder().encode(JSON.stringify(data));
}

// ============================================================================
// TRIPLE GENERATORS
// ============================================================================

function* generateTitleBasicsTriples(fields: string[], txId: string): Generator<Triple> {
  const [tconst, titleType, primaryTitle, , , startYear, , runtimeMinutes, genres] = fields;
  if (!tconst || tconst === '\\N') return;
  if (titleType !== 'movie' && titleType !== 'tvMovie' && titleType !== 'tvSeries') return;

  const entityId = `https://imdb.com/title/${tconst}`;
  const ts = BigInt(Date.now());

  yield { subject: entityId, predicate: '$type', object: { type: ObjectType.STRING, value: 'Movie' }, timestamp: ts, txId };

  if (primaryTitle && primaryTitle !== '\\N') {
    yield { subject: entityId, predicate: 'title', object: { type: ObjectType.STRING, value: primaryTitle }, timestamp: ts, txId };
  }
  if (startYear && startYear !== '\\N') {
    const year = parseInt(startYear, 10);
    if (!isNaN(year)) {
      yield { subject: entityId, predicate: 'year', object: { type: ObjectType.INT32, value: BigInt(year) }, timestamp: ts, txId };
    }
  }
  if (runtimeMinutes && runtimeMinutes !== '\\N') {
    const runtime = parseInt(runtimeMinutes, 10);
    if (!isNaN(runtime)) {
      yield { subject: entityId, predicate: 'runtime', object: { type: ObjectType.INT32, value: BigInt(runtime) }, timestamp: ts, txId };
    }
  }
  if (genres && genres !== '\\N') {
    yield { subject: entityId, predicate: 'genres', object: { type: ObjectType.STRING, value: genres }, timestamp: ts, txId };
  }
}

function* generateTitleRatingsTriples(fields: string[], txId: string): Generator<Triple> {
  const [tconst, averageRating] = fields;
  if (!tconst || tconst === '\\N') return;

  const entityId = `https://imdb.com/title/${tconst}`;
  const ts = BigInt(Date.now());

  if (averageRating && averageRating !== '\\N') {
    const rating = parseFloat(averageRating);
    if (!isNaN(rating)) {
      yield { subject: entityId, predicate: 'rating', object: { type: ObjectType.FLOAT64, value: rating }, timestamp: ts, txId };
    }
  }
}

function* generateNameBasicsTriples(fields: string[], txId: string): Generator<Triple> {
  const [nconst, primaryName, birthYear, , primaryProfession] = fields;
  if (!nconst || nconst === '\\N') return;

  const entityId = `https://imdb.com/name/${nconst}`;
  const ts = BigInt(Date.now());

  yield { subject: entityId, predicate: '$type', object: { type: ObjectType.STRING, value: 'Person' }, timestamp: ts, txId };

  if (primaryName && primaryName !== '\\N') {
    yield { subject: entityId, predicate: 'name', object: { type: ObjectType.STRING, value: primaryName }, timestamp: ts, txId };
  }
  if (birthYear && birthYear !== '\\N') {
    const year = parseInt(birthYear, 10);
    if (!isNaN(year)) {
      yield { subject: entityId, predicate: 'birthYear', object: { type: ObjectType.INT32, value: BigInt(year) }, timestamp: ts, txId };
    }
  }
  if (primaryProfession && primaryProfession !== '\\N') {
    yield { subject: entityId, predicate: 'profession', object: { type: ObjectType.STRING, value: primaryProfession }, timestamp: ts, txId };
  }
}

function* generateTitlePrincipalsTriples(fields: string[], txId: string): Generator<Triple> {
  const [tconst, , nconst, category] = fields;
  if (!tconst || tconst === '\\N' || !nconst || nconst === '\\N') return;

  const movieId = `https://imdb.com/title/${tconst}`;
  const personId = `https://imdb.com/name/${nconst}`;
  const ts = BigInt(Date.now());

  if (category === 'director') {
    yield { subject: movieId, predicate: 'directedBy', object: { type: ObjectType.REF, value: personId }, timestamp: ts, txId };
  } else if (category === 'actor' || category === 'actress' || category === 'self') {
    yield { subject: movieId, predicate: 'starring', object: { type: ObjectType.REF, value: personId }, timestamp: ts, txId };
  }
}

// ============================================================================
// LOADER
// ============================================================================

async function loadDataset(
  datasetName: string,
  url: string,
  generator: (fields: string[], txId: string) => Generator<Triple>
): Promise<{ tripleCount: number; chunkCount: number; sizeBytes: number }> {
  console.log(`\n📥 Loading ${datasetName} from ${url}...`);

  // Create temp dir
  if (!existsSync(TEMP_DIR)) {
    mkdirSync(TEMP_DIR, { recursive: true });
  }

  const txId = generateULID();
  let linesProcessed = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalSizeBytes = 0;
  let headers: string[] | null = null;

  // Flush function
  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/imdb/${datasetName}/chunks/${chunkId}.graphcol`;
    const localPath = join(TEMP_DIR, `${chunkId}.graphcol`);

    // Encode to GraphCol
    const encoded = encodeGraphCol(triples, NAMESPACE);
    totalSizeBytes += encoded.length;

    // Write to temp file
    const writeStream = createWriteStream(localPath);
    writeStream.write(encoded);
    writeStream.end();
    await new Promise(resolve => writeStream.on('finish', resolve));

    // Upload to R2 via wrangler
    console.log(`  📤 Uploading ${chunkId}: ${triples.length.toLocaleString()} triples, ${(encoded.length / 1024 / 1024).toFixed(2)}MB...`);
    try {
      execSync(`npx wrangler r2 object put ${R2_BUCKET}/${chunkPath} --file=${localPath} --content-type=application/octet-stream`, {
        stdio: 'pipe',
        cwd: join(__dirname, '..'),
      });
    } catch (err) {
      console.error(`  ❌ Failed to upload ${chunkId}:`, err);
      throw err;
    }

    // Clean up temp file
    unlinkSync(localPath);

    chunkIndex++;
    triples = [];
  };

  // Fetch and stream
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }

  // Stream processing
  const nodeStream = Readable.fromWeb(response.body as any);
  const gunzip = createGunzip();
  let buffer = '';

  const processLine = (line: string) => {
    if (!line.trim()) return;
    const fields = line.split('\t');

    // First line is headers
    if (!headers) {
      headers = fields;
      return;
    }

    linesProcessed++;

    // Generate triples
    for (const triple of generator(fields, txId)) {
      triples.push(triple);
      triplesGenerated++;
    }

    // Progress
    if (linesProcessed % 500_000 === 0) {
      console.log(`  📊 ${linesProcessed.toLocaleString()} lines, ${triplesGenerated.toLocaleString()} triples, ${chunkIndex} chunks`);
    }
  };

  // Transform stream to process lines
  const lineProcessor = new Transform({
    transform(chunk, encoding, callback) {
      buffer += chunk.toString();
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';

      for (const line of lines) {
        processLine(line);

        // Flush when chunk is full (sync check, async flush)
        if (triples.length >= CHUNK_TRIPLE_LIMIT) {
          flushChunk().then(() => callback()).catch(callback);
          return;
        }
      }
      callback();
    },
    async flush(callback) {
      if (buffer.trim()) {
        processLine(buffer);
      }
      try {
        await flushChunk();
        callback();
      } catch (err) {
        callback(err as Error);
      }
    },
  });

  await pipeline(nodeStream, gunzip, lineProcessor);

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: datasetName,
    stats: {
      totalTriples: triplesGenerated,
      totalChunks: chunkIndex,
      totalSizeBytes,
      linesProcessed,
    },
    createdAt: new Date().toISOString(),
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/imdb/${datasetName}/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  console.log(`\n✅ Completed ${datasetName}:`);
  console.log(`   Lines: ${linesProcessed.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalSizeBytes / 1024 / 1024).toFixed(2)}MB`);

  return { tripleCount: triplesGenerated, chunkCount: chunkIndex, sizeBytes: totalSizeBytes };
}

// ============================================================================
// MAIN
// ============================================================================

const GENERATORS: Record<string, (fields: string[], txId: string) => Generator<Triple>> = {
  titleBasics: generateTitleBasicsTriples,
  titleRatings: generateTitleRatingsTriples,
  nameBasics: generateNameBasicsTriples,
  titlePrincipals: generateTitlePrincipalsTriples,
};

async function main() {
  const arg = process.argv[2] ?? 'all';

  console.log('🎬 IMDB Local Loader');
  console.log('====================');

  const datasets: Array<keyof typeof IMDB_DATASETS> = arg === 'all'
    ? ['titleBasics', 'titleRatings', 'nameBasics', 'titlePrincipals']
    : [arg.replace(/-([a-z])/g, (_, c) => c.toUpperCase()) as keyof typeof IMDB_DATASETS];

  const results: Array<{ name: string; triples: number; chunks: number; size: number }> = [];

  for (const dataset of datasets) {
    if (!IMDB_DATASETS[dataset]) {
      console.error(`Unknown dataset: ${dataset}`);
      continue;
    }

    const result = await loadDataset(dataset, IMDB_DATASETS[dataset], GENERATORS[dataset]);
    results.push({
      name: dataset,
      triples: result.tripleCount,
      chunks: result.chunkCount,
      size: result.sizeBytes,
    });
  }

  console.log('\n📈 Summary');
  console.log('==========');
  let totalTriples = 0;
  let totalChunks = 0;
  let totalSize = 0;
  for (const r of results) {
    console.log(`${r.name}: ${r.triples.toLocaleString()} triples, ${r.chunks} chunks, ${(r.size / 1024 / 1024).toFixed(2)}MB`);
    totalTriples += r.triples;
    totalChunks += r.chunks;
    totalSize += r.size;
  }
  console.log('---');
  console.log(`Total: ${totalTriples.toLocaleString()} triples, ${totalChunks} chunks, ${(totalSize / 1024 / 1024).toFixed(2)}MB`);
}

main().catch(console.error);
