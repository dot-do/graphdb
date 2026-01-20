#!/usr/bin/env npx tsx
/**
 * Local Wikidata Loader
 *
 * Downloads WikiDataSets pre-processed subsets from graphs.telecom-paris.fr
 * and uploads to R2.
 *
 * Datasets available:
 * - humans (409 MB) - People from Wikidata
 * - films (28 MB) - Movies
 * - companies (16 MB) - Companies
 * - countries (<1 MB) - Countries
 * - animals (105 MB) - Animal species
 *
 * Usage:
 *   npx tsx scripts/local-wikidata-loader.ts [subset]
 *   npx tsx scripts/local-wikidata-loader.ts all
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync, createReadStream, readdirSync, rmSync } from 'fs';
import { createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable } from 'stream';
import { execSync } from 'child_process';
import { join, dirname, basename } from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';
import { extract } from 'tar';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const BASE_URL = 'https://graphs.telecom-paris.fr/data/WikiDataSets';
const DATASETS: Record<string, { url: string; size: string }> = {
  humans: { url: `${BASE_URL}/humans.tar.gz`, size: '409MB' },
  films: { url: `${BASE_URL}/films.tar.gz`, size: '28MB' },
  companies: { url: `${BASE_URL}/companies.tar.gz`, size: '16MB' },
  countries: { url: `${BASE_URL}/countries.tar.gz`, size: '<1MB' },
  animals: { url: `${BASE_URL}/animals.tar.gz`, size: '105MB' },
};

const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://wikidata.org/';
const CHUNK_TRIPLE_LIMIT = 250_000;
const TEMP_DIR = '/tmp/wikidata-loader';

const ObjectType = {
  STRING: 5,
  INT64: 3,
  REF: 10,
} as const;

interface Triple {
  subject: string;
  predicate: string;
  object: { type: number; value: any };
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
      if (lastRandom[i] < 255) { lastRandom[i]++; break; }
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
// GRAPHCOL ENCODER
// ============================================================================

function encodeGraphCol(triples: Triple[], namespace: string): Uint8Array {
  const data = {
    version: 1,
    namespace,
    triples: triples.map(t => ({
      s: t.subject,
      p: t.predicate,
      o: { t: t.object.type, v: typeof t.object.value === 'bigint' ? Number(t.object.value) : t.object.value },
      ts: Number(t.timestamp),
      tx: t.txId,
    })),
  };
  return new TextEncoder().encode(JSON.stringify(data));
}

// ============================================================================
// LOADER
// ============================================================================

async function loadDataset(name: string): Promise<{ triples: number; chunks: number; bytes: number; entities: number }> {
  const dataset = DATASETS[name];
  if (!dataset) throw new Error(`Unknown dataset: ${name}`);

  console.log(`\n📥 Loading Wikidata ${name} (${dataset.size})...`);

  const dataDir = join(TEMP_DIR, name);
  if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

  // Download and extract
  console.log(`  Downloading ${dataset.url}...`);
  const tarPath = join(TEMP_DIR, `${name}.tar.gz`);

  const response = await fetch(dataset.url);
  if (!response.ok) throw new Error(`Failed to download: ${response.status}`);

  const fileStream = createWriteStream(tarPath);
  const nodeStream = Readable.fromWeb(response.body as any);
  await pipeline(nodeStream, fileStream);

  console.log(`  Extracting...`);
  await extract({ file: tarPath, cwd: TEMP_DIR });
  unlinkSync(tarPath);

  // Find extracted directory
  const extractedDir = readdirSync(TEMP_DIR).find(d => d.startsWith(name) || d === name);
  const dataPath = join(TEMP_DIR, extractedDir || name);

  // Read edges.txt (format: head_idx TAB relation_idx TAB tail_idx)
  const edgesFile = join(dataPath, 'edges.txt');
  const entitiesFile = join(dataPath, 'entities.txt');
  const relationsFile = join(dataPath, 'relations.txt');

  // Load entity labels (idx TAB wikidata_id TAB label)
  console.log(`  Loading entity labels...`);
  const entityLabels = new Map<string, { qid: string; label: string }>();
  const entityStream = createReadStream(entitiesFile);
  const entityRl = createInterface({ input: entityStream, crlfDelay: Infinity });
  for await (const line of entityRl) {
    const [idx, qid, label] = line.split('\t');
    if (idx && qid) entityLabels.set(idx, { qid, label: label || '' });
  }
  console.log(`    ${entityLabels.size.toLocaleString()} entities`);

  // Load relation labels (idx TAB pid TAB label)
  console.log(`  Loading relation labels...`);
  const relationLabels = new Map<string, { pid: string; label: string }>();
  const relationStream = createReadStream(relationsFile);
  const relationRl = createInterface({ input: relationStream, crlfDelay: Infinity });
  for await (const line of relationRl) {
    const [idx, pid, label] = line.split('\t');
    if (idx && pid) relationLabels.set(idx, { pid, label: label || '' });
  }
  console.log(`    ${relationLabels.size.toLocaleString()} relations`);

  // Process edges
  const txId = generateULID();
  const ts = BigInt(Date.now());
  let triples: Triple[] = [];
  let chunkIndex = 0;
  let totalTriples = 0;
  let totalBytes = 0;
  let edgesProcessed = 0;

  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/wikidata/${name}/chunks/${chunkId}.graphcol`;
    const localPath = join(TEMP_DIR, `${chunkId}.graphcol`);

    const encoded = encodeGraphCol(triples, NAMESPACE);
    totalBytes += encoded.length;

    const ws = createWriteStream(localPath);
    ws.write(encoded);
    ws.end();
    await new Promise(resolve => ws.on('finish', resolve));

    console.log(`  📤 Uploading ${chunkId}: ${triples.length.toLocaleString()} triples, ${(encoded.length / 1024 / 1024).toFixed(2)}MB...`);
    try {
      execSync(`npx wrangler r2 object put ${R2_BUCKET}/${chunkPath} --file=${localPath} --content-type=application/octet-stream`, {
        stdio: 'pipe',
        cwd: join(__dirname, '..'),
      });
    } catch (err) {
      console.error(`  ❌ Failed to upload ${chunkId}`);
      throw err;
    }

    unlinkSync(localPath);
    chunkIndex++;
    triples = [];
  };

  console.log(`  Processing edges...`);
  const edgeStream = createReadStream(edgesFile);
  const edgeRl = createInterface({ input: edgeStream, crlfDelay: Infinity });

  for await (const line of edgeRl) {
    const [headIdx, relIdx, tailIdx] = line.split('\t');
    if (!headIdx || !relIdx || !tailIdx) continue;

    const head = entityLabels.get(headIdx);
    const tail = entityLabels.get(tailIdx);
    const rel = relationLabels.get(relIdx);

    if (!head || !tail || !rel) continue;

    const subj = `https://wikidata.org/entity/${head.qid}`;
    const obj = `https://wikidata.org/entity/${tail.qid}`;
    const pred = rel.pid;

    triples.push({
      subject: subj,
      predicate: pred,
      object: { type: ObjectType.REF, value: obj },
      timestamp: ts,
      txId,
    });
    totalTriples++;
    edgesProcessed++;

    if (triples.length >= CHUNK_TRIPLE_LIMIT) {
      await flushChunk();
    }

    if (edgesProcessed % 100_000 === 0) {
      console.log(`    📊 ${edgesProcessed.toLocaleString()} edges processed`);
    }
  }

  // Add entity labels as separate triples
  console.log(`  Adding entity labels...`);
  for (const [, { qid, label }] of entityLabels) {
    if (!label) continue;

    triples.push({
      subject: `https://wikidata.org/entity/${qid}`,
      predicate: 'label',
      object: { type: ObjectType.STRING, value: label },
      timestamp: ts,
      txId,
    });
    totalTriples++;

    if (triples.length >= CHUNK_TRIPLE_LIMIT) {
      await flushChunk();
    }
  }

  // Final flush
  await flushChunk();

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: `wikidata/${name}`,
    source: 'WikiDataSets (graphs.telecom-paris.fr)',
    stats: {
      totalTriples,
      totalChunks: chunkIndex,
      totalEntities: entityLabels.size,
      totalRelations: relationLabels.size,
      totalEdges: edgesProcessed,
      totalSizeBytes: totalBytes,
    },
    createdAt: new Date().toISOString(),
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/wikidata/${name}/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  // Cleanup extracted files
  rmSync(dataPath, { recursive: true, force: true });

  console.log(`\n✅ Completed ${name}:`);
  console.log(`   Entities: ${entityLabels.size.toLocaleString()}`);
  console.log(`   Edges: ${edgesProcessed.toLocaleString()}`);
  console.log(`   Triples: ${totalTriples.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalBytes / 1024 / 1024).toFixed(2)}MB`);

  return { triples: totalTriples, chunks: chunkIndex, bytes: totalBytes, entities: entityLabels.size };
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const arg = process.argv[2] ?? 'all';

  console.log('📚 Wikidata Local Loader (WikiDataSets)');
  console.log('=======================================');
  console.log('Source: graphs.telecom-paris.fr');
  console.log('');

  const datasetNames = arg === 'all'
    ? Object.keys(DATASETS)
    : [arg];

  const results: Array<{ name: string; triples: number; chunks: number; bytes: number; entities: number }> = [];

  for (const name of datasetNames) {
    try {
      const result = await loadDataset(name);
      results.push({ name, ...result });
    } catch (err) {
      console.error(`❌ Failed to load ${name}:`, err);
    }
  }

  if (results.length > 1) {
    console.log('\n📈 Summary');
    console.log('==========');
    let totalTriples = 0, totalChunks = 0, totalBytes = 0, totalEntities = 0;
    for (const r of results) {
      console.log(`${r.name}: ${r.entities.toLocaleString()} entities, ${r.triples.toLocaleString()} triples`);
      totalTriples += r.triples;
      totalChunks += r.chunks;
      totalBytes += r.bytes;
      totalEntities += r.entities;
    }
    console.log('---');
    console.log(`Total: ${totalEntities.toLocaleString()} entities, ${totalTriples.toLocaleString()} triples, ${totalChunks} chunks, ${(totalBytes / 1024 / 1024).toFixed(2)}MB`);
  }
}

main().catch(console.error);
