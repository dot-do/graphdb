#!/usr/bin/env npx tsx
/**
 * Local Common Crawl Host Graph Loader
 *
 * Runs locally to bypass Worker memory limits.
 * Downloads CC host graph data and uploads to R2.
 *
 * Source: Common Crawl Web Graph (Aug-Sep-Oct 2024)
 * https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/
 *
 * Full graph: ~300M hosts, 2.6B edges
 * This loader limits to configurable max hosts/edges for feasibility.
 *
 * Usage:
 *   npx tsx scripts/local-cc-hostgraph-loader.ts [maxHosts] [maxEdges]
 *   npx tsx scripts/local-cc-hostgraph-loader.ts 1000000 10000000
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync, readFileSync } from 'fs';
import { createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
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
const CC_GRAPH_PATH = '/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host';

const PATHS_URLS = {
  vertices: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-vertices.paths.gz`,
  edges: `${CC_BASE_URL}${CC_GRAPH_PATH}/cc-main-2024-aug-sep-oct-host-edges.paths.gz`,
};

const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://cc.org/';
const CHUNK_TRIPLE_LIMIT = 250_000;
const TEMP_DIR = '/tmp/cc-hostgraph-loader';

// Default limits
const DEFAULT_MAX_HOSTS = 1_000_000;
const DEFAULT_MAX_EDGES = 10_000_000;

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
// HELPERS
// ============================================================================

function reverseHostname(reversed: string): string {
  // Convert "com.example.www" back to "www.example.com"
  return reversed.split('.').reverse().join('.');
}

async function fetchPathsList(url: string): Promise<string[]> {
  console.log(`  Fetching paths list from ${url}...`);
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to fetch paths: ${response.status}`);

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

async function loadCCHostGraph(maxHosts: number, maxEdges: number): Promise<void> {
  console.log('🌐 Common Crawl Host Graph Local Loader');
  console.log('=======================================');
  console.log(`Max hosts: ${maxHosts.toLocaleString()}`);
  console.log(`Max edges: ${maxEdges.toLocaleString()}`);
  console.log('');

  if (!existsSync(TEMP_DIR)) {
    mkdirSync(TEMP_DIR, { recursive: true });
  }

  const txId = generateULID();
  let hostsLoaded = 0;
  let edgesLoaded = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalSizeBytes = 0;

  // Map vertex ID to hostname for edge resolution
  const idToHostname = new Map<number, string>();

  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/cc-hostgraph/chunks/${chunkId}.graphcol`;
    const localPath = join(TEMP_DIR, `${chunkId}.graphcol`);

    const encoded = encodeGraphCol(triples, NAMESPACE);
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
  // PHASE 1: Load vertices (hosts)
  // ========================================
  console.log('📥 Phase 1: Loading vertices (hosts)...');

  const vertexPaths = await fetchPathsList(PATHS_URLS.vertices);
  console.log(`  Found ${vertexPaths.length} vertex files`);

  const ts = BigInt(Date.now());

  for (const path of vertexPaths) {
    if (hostsLoaded >= maxHosts) break;

    const url = `${CC_BASE_URL}/${path}`;
    console.log(`  Processing ${path}...`);

    try {
      const response = await fetch(url);
      if (!response.ok) {
        console.log(`    Skipping (${response.status})`);
        continue;
      }

      const nodeStream = Readable.fromWeb(response.body as any);
      const gunzip = createGunzip();
      const rl = createInterface({
        input: nodeStream.pipe(gunzip),
        crlfDelay: Infinity,
      });

      for await (const line of rl) {
        if (hostsLoaded >= maxHosts) break;

        const parts = line.split('\t');
        if (parts.length < 2) continue;

        const vertexId = parseInt(parts[0], 10);
        const reversedHostname = parts[1];
        const hostname = reverseHostname(reversedHostname);

        // Store mapping for edge resolution
        idToHostname.set(vertexId, hostname);

        const entityId = `https://cc.org/host/${hostname}`;

        // Generate host triples
        triples.push({ subject: entityId, predicate: '$type', object: { type: ObjectType.STRING, value: 'Host' }, timestamp: ts, txId });
        triples.push({ subject: entityId, predicate: 'hostname', object: { type: ObjectType.STRING, value: hostname }, timestamp: ts, txId });
        triples.push({ subject: entityId, predicate: 'vertexId', object: { type: ObjectType.INT64, value: BigInt(vertexId) }, timestamp: ts, txId });

        triplesGenerated += 3;
        hostsLoaded++;

        if (triples.length >= CHUNK_TRIPLE_LIMIT) {
          await flushChunk();
        }

        if (hostsLoaded % 100_000 === 0) {
          console.log(`    📊 ${hostsLoaded.toLocaleString()} hosts, ${triplesGenerated.toLocaleString()} triples`);
        }
      }
    } catch (err) {
      console.log(`    Error processing file, continuing...`);
    }
  }

  // Flush remaining host triples
  await flushChunk();

  console.log(`  ✅ Loaded ${hostsLoaded.toLocaleString()} hosts`);
  console.log('');

  // ========================================
  // PHASE 2: Load edges (links)
  // ========================================
  console.log('📥 Phase 2: Loading edges (links)...');

  const edgePaths = await fetchPathsList(PATHS_URLS.edges);
  console.log(`  Found ${edgePaths.length} edge files`);

  for (const path of edgePaths) {
    if (edgesLoaded >= maxEdges) break;

    const url = `${CC_BASE_URL}/${path}`;
    console.log(`  Processing ${path}...`);

    try {
      const response = await fetch(url);
      if (!response.ok) {
        console.log(`    Skipping (${response.status})`);
        continue;
      }

      const nodeStream = Readable.fromWeb(response.body as any);
      const gunzip = createGunzip();
      const rl = createInterface({
        input: nodeStream.pipe(gunzip),
        crlfDelay: Infinity,
      });

      for await (const line of rl) {
        if (edgesLoaded >= maxEdges) break;

        const parts = line.split('\t');
        if (parts.length < 2) continue;

        const fromId = parseInt(parts[0], 10);
        const toId = parseInt(parts[1], 10);

        // Only create edges for hosts we loaded
        const fromHostname = idToHostname.get(fromId);
        const toHostname = idToHostname.get(toId);

        if (!fromHostname || !toHostname) continue;

        const fromEntity = `https://cc.org/host/${fromHostname}`;
        const toEntity = `https://cc.org/host/${toHostname}`;

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

        if (edgesLoaded % 500_000 === 0) {
          console.log(`    📊 ${edgesLoaded.toLocaleString()} edges, ${triplesGenerated.toLocaleString()} triples`);
        }
      }
    } catch (err) {
      console.log(`    Error processing file, continuing...`);
    }
  }

  // Final flush
  await flushChunk();

  console.log(`  ✅ Loaded ${edgesLoaded.toLocaleString()} edges`);
  console.log('');

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: 'cc-hostgraph',
    source: 'Common Crawl Web Graph Aug-Sep-Oct 2024',
    limits: { maxHosts, maxEdges },
    stats: {
      totalTriples: triplesGenerated,
      totalChunks: chunkIndex,
      totalHosts: hostsLoaded,
      totalEdges: edgesLoaded,
      totalSizeBytes,
    },
    createdAt: new Date().toISOString(),
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/cc-hostgraph/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  console.log('✅ Completed CC Host Graph load:');
  console.log(`   Hosts: ${hostsLoaded.toLocaleString()}`);
  console.log(`   Edges: ${edgesLoaded.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalSizeBytes / 1024 / 1024).toFixed(2)}MB`);
}

const maxHosts = parseInt(process.argv[2] || '', 10) || DEFAULT_MAX_HOSTS;
const maxEdges = parseInt(process.argv[3] || '', 10) || DEFAULT_MAX_EDGES;

loadCCHostGraph(maxHosts, maxEdges).catch(console.error);
