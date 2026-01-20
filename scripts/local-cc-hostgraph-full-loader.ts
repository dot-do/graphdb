#!/usr/bin/env npx tsx
/**
 * Full Common Crawl Host Graph Loader
 *
 * Streams the COMPLETE Common Crawl host-level web graph to R2.
 * Uses numeric vertex IDs as entity identifiers to avoid memory issues.
 *
 * Scale (Oct-Nov-Dec 2024):
 * - 250M+ hosts (vertices)
 * - 10.9 BILLION edges
 *
 * Data model:
 * - Hosts: https://cc.org/host/{vertexId} with hostname property
 * - Edges: linksTo relationship between vertex IDs
 *
 * Usage:
 *   npx tsx scripts/local-cc-hostgraph-full-loader.ts
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync } from 'fs';
import { createGunzip } from 'zlib';
import { Readable } from 'stream';
import { execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const CC_BASE_URL = 'https://data.commoncrawl.org';
// Using the most recent complete crawl
const CC_GRAPH_PATH = '/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host';

const PATHS_URLS = {
  vertices: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-vertices.paths.gz`,
  edges: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-edges.paths.gz`,
};

const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://cc.org/';
const CHUNK_TRIPLE_LIMIT = 500_000; // Larger chunks for efficiency at scale
const TEMP_DIR = '/tmp/cc-hostgraph-full-loader';

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
// GRAPHCOL ENCODER - Optimized for large scale
// ============================================================================

function encodeGraphColCompact(triples: Triple[], namespace: string): Uint8Array {
  // More compact format for large datasets
  const data = {
    v: 1,
    ns: namespace,
    t: triples.map(t => ([
      t.subject,
      t.predicate,
      t.object.type,
      typeof t.object.value === 'bigint' ? Number(t.object.value) : t.object.value,
      Number(t.timestamp),
      t.txId,
    ])),
  };
  return new TextEncoder().encode(JSON.stringify(data));
}

// ============================================================================
// HELPERS
// ============================================================================

function reverseHostname(reversed: string): string {
  return reversed.split('.').reverse().join('.');
}

async function fetchWithRetry(url: string, retries = 3): Promise<Response> {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await fetch(url);
      if (response.ok) return response;
      console.log(`    Retry ${i + 1}/${retries} for ${url} (status ${response.status})`);
    } catch (err: any) {
      console.log(`    Retry ${i + 1}/${retries} for ${url} (${err.message})`);
      if (i === retries - 1) throw err;
    }
    await new Promise(r => setTimeout(r, 2000 * (i + 1))); // Exponential backoff
  }
  throw new Error(`Failed to fetch ${url} after ${retries} retries`);
}

async function fetchPathsList(url: string): Promise<string[]> {
  console.log(`  Fetching paths list from ${url}...`);
  const response = await fetchWithRetry(url);

  const nodeStream = Readable.fromWeb(response.body as any);
  const gunzip = createGunzip();
  const rl = createInterface({
    input: nodeStream.pipe(gunzip),
    crlfDelay: Infinity,
  });

  const paths: string[] = [];
  for await (const line of rl) {
    if (line.trim()) paths.push(line.trim());
  }
  return paths;
}

// ============================================================================
// MAIN LOADER
// ============================================================================

async function loadCCHostGraphFull(): Promise<void> {
  console.log('🌐 Full Common Crawl Host Graph Loader');
  console.log('======================================');
  console.log('Scale: ~250M hosts, ~10.9B edges');
  console.log('This will take many hours to complete.');
  console.log('');

  if (!existsSync(TEMP_DIR)) {
    mkdirSync(TEMP_DIR, { recursive: true });
  }

  const txId = generateULID();
  const ts = BigInt(Date.now());
  const startTime = Date.now();

  let hostsLoaded = 0;
  let edgesLoaded = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalSizeBytes = 0;

  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/cc-hostgraph-full/chunks/${chunkId}.graphcol`;
    const localPath = join(TEMP_DIR, `${chunkId}.graphcol`);

    const encoded = encodeGraphColCompact(triples, NAMESPACE);
    totalSizeBytes += encoded.length;

    const writeStream = createWriteStream(localPath);
    writeStream.write(encoded);
    writeStream.end();
    await new Promise(resolve => writeStream.on('finish', resolve));

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

  // ========================================
  // PHASE 1: Load ALL vertices (hosts)
  // ========================================
  console.log('📥 Phase 1: Loading ALL vertices (hosts)...');

  const vertexPaths = await fetchPathsList(PATHS_URLS.vertices);
  console.log(`  Found ${vertexPaths.length} vertex files`);

  for (const path of vertexPaths) {
    const url = `${CC_BASE_URL}/${path}`;
    console.log(`  Processing ${path}...`);

    let retries = 3;
    while (retries > 0) {
      try {
        const response = await fetchWithRetry(url);

        const nodeStream = Readable.fromWeb(response.body as any);
        const gunzip = createGunzip();
        const rl = createInterface({
          input: nodeStream.pipe(gunzip),
          crlfDelay: Infinity,
        });

        for await (const line of rl) {
          const parts = line.split('\t');
          if (parts.length < 2) continue;

          const vertexId = parts[0];
          const reversedHostname = parts[1];
          const hostname = reverseHostname(reversedHostname);

          // Use numeric vertex ID as entity identifier
          const entityId = `h/${vertexId}`;

          // Generate host triples - minimal for scale
          triples.push({
            subject: entityId,
            predicate: '$type',
            object: { type: ObjectType.STRING, value: 'Host' },
            timestamp: ts,
            txId,
          });
          triples.push({
            subject: entityId,
            predicate: 'hostname',
            object: { type: ObjectType.STRING, value: hostname },
            timestamp: ts,
            txId,
          });

          triplesGenerated += 2;
          hostsLoaded++;

          if (triples.length >= CHUNK_TRIPLE_LIMIT) {
            await flushChunk();
          }

          if (hostsLoaded % 1_000_000 === 0) {
            const elapsed = (Date.now() - startTime) / 1000;
            const rate = hostsLoaded / elapsed;
            console.log(`    📊 ${hostsLoaded.toLocaleString()} hosts, ${triplesGenerated.toLocaleString()} triples, ${rate.toFixed(0)}/s`);
          }
        }
        break; // Success, exit retry loop
      } catch (err: any) {
        retries--;
        console.log(`    Error: ${err.message}, retries left: ${retries}`);
        if (retries === 0) console.log(`    Skipping file after all retries failed`);
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }

  // Flush remaining host triples
  await flushChunk();

  const vertexDuration = (Date.now() - startTime) / 1000 / 3600;
  console.log(`  ✅ Loaded ${hostsLoaded.toLocaleString()} hosts in ${vertexDuration.toFixed(2)}h`);
  console.log('');

  // ========================================
  // PHASE 2: Load ALL edges (links)
  // ========================================
  console.log('📥 Phase 2: Loading ALL edges (~10.9B links)...');

  const edgeStartTime = Date.now();
  const edgePaths = await fetchPathsList(PATHS_URLS.edges);
  console.log(`  Found ${edgePaths.length} edge files`);

  for (const path of edgePaths) {
    const url = `${CC_BASE_URL}/${path}`;
    console.log(`  Processing ${path}...`);

    let retries = 3;
    while (retries > 0) {
      try {
        const response = await fetchWithRetry(url);

        const nodeStream = Readable.fromWeb(response.body as any);
        const gunzip = createGunzip();
        const rl = createInterface({
          input: nodeStream.pipe(gunzip),
          crlfDelay: Infinity,
        });

        for await (const line of rl) {
          const parts = line.split('\t');
          if (parts.length < 2) continue;

          const fromId = parts[0];
          const toId = parts[1];

          // Direct vertex ID references - no lookup needed
          const fromEntity = `h/${fromId}`;
          const toEntity = `h/${toId}`;

          triples.push({
            subject: fromEntity,
            predicate: 'linksTo',
            object: { type: ObjectType.REF, value: toEntity },
            timestamp: ts,
            txId,
          });

          triplesGenerated++;
          edgesLoaded++;

          if (triples.length >= CHUNK_TRIPLE_LIMIT) {
            await flushChunk();
          }

          if (edgesLoaded % 10_000_000 === 0) {
            const elapsed = (Date.now() - edgeStartTime) / 1000;
            const rate = edgesLoaded / elapsed;
            const remaining = (10_900_000_000 - edgesLoaded) / rate / 3600;
            console.log(`    📊 ${(edgesLoaded / 1_000_000_000).toFixed(2)}B edges, ${triplesGenerated.toLocaleString()} triples, ${(rate / 1000).toFixed(0)}K/s, ETA: ${remaining.toFixed(1)}h`);
          }
        }
        break; // Success, exit retry loop
      } catch (err: any) {
        retries--;
        console.log(`    Error: ${err.message}, retries left: ${retries}`);
        if (retries === 0) console.log(`    Skipping file after all retries failed`);
        await new Promise(r => setTimeout(r, 5000));
      }
    }
  }

  // Final flush
  await flushChunk();

  const totalDuration = (Date.now() - startTime) / 1000 / 3600;

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: 'cc-hostgraph-full',
    source: 'Common Crawl Web Graph Aug-Sep-Oct 2024 (Full)',
    stats: {
      totalTriples: triplesGenerated,
      totalChunks: chunkIndex,
      totalHosts: hostsLoaded,
      totalEdges: edgesLoaded,
      totalSizeBytes,
    },
    createdAt: new Date().toISOString(),
    loadDuration: `${totalDuration.toFixed(2)} hours`,
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/cc-hostgraph-full/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  console.log('');
  console.log('✅ Completed FULL CC Host Graph load:');
  console.log(`   Hosts: ${hostsLoaded.toLocaleString()}`);
  console.log(`   Edges: ${edgesLoaded.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalSizeBytes / 1024 / 1024 / 1024).toFixed(2)}GB`);
  console.log(`   Duration: ${totalDuration.toFixed(2)} hours`);
}

loadCCHostGraphFull().catch(console.error);
