#!/usr/bin/env npx tsx
/**
 * Full Wikidata Dump Loader
 *
 * Streams the complete Wikidata JSON dump (~100GB compressed, 500GB uncompressed)
 * and uploads to R2 incrementally.
 *
 * Source: https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
 * - 100M+ entities
 * - Each line is one JSON entity
 * - bz2 compressed
 *
 * Usage:
 *   npx tsx scripts/local-wikidata-full-loader.ts [maxEntities]
 *   npx tsx scripts/local-wikidata-full-loader.ts 10000000  # Load 10M entities
 *   npx tsx scripts/local-wikidata-full-loader.ts           # Load all (~100M)
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync } from 'fs';
import { spawn } from 'child_process';
import { execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';
import { Readable } from 'stream';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const WIKIDATA_DUMP_URL = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2';
const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://wikidata.org/';
const CHUNK_TRIPLE_LIMIT = 250_000;
const TEMP_DIR = '/tmp/wikidata-full-loader';

const ObjectType = {
  STRING: 5,
  INT64: 3,
  FLOAT64: 4,
  REF: 10,
  TIMESTAMP: 7,
} as const;

interface Triple {
  subject: string;
  predicate: string;
  object: { type: number; value: any };
  timestamp: bigint;
  txId: string;
}

interface WikidataEntity {
  id: string;
  type: string;
  labels?: Record<string, { language: string; value: string }>;
  descriptions?: Record<string, { language: string; value: string }>;
  claims?: Record<string, WikidataClaim[]>;
  sitelinks?: Record<string, { site: string; title: string }>;
}

interface WikidataClaim {
  mainsnak: {
    snaktype: string;
    property: string;
    datavalue?: {
      type: string;
      value: any;
    };
  };
  rank?: string;
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
// TRIPLE GENERATOR
// ============================================================================

function* generateTriples(entity: WikidataEntity, txId: string, ts: bigint): Generator<Triple> {
  const entityId = `https://wikidata.org/entity/${entity.id}`;

  // $type
  yield {
    subject: entityId,
    predicate: '$type',
    object: { type: ObjectType.STRING, value: entity.type === 'property' ? 'Property' : 'Entity' },
    timestamp: ts,
    txId,
  };

  // English label
  const enLabel = entity.labels?.en?.value;
  if (enLabel) {
    yield {
      subject: entityId,
      predicate: 'label',
      object: { type: ObjectType.STRING, value: enLabel },
      timestamp: ts,
      txId,
    };
  }

  // English description
  const enDesc = entity.descriptions?.en?.value;
  if (enDesc) {
    yield {
      subject: entityId,
      predicate: 'description',
      object: { type: ObjectType.STRING, value: enDesc.slice(0, 500) },
      timestamp: ts,
      txId,
    };
  }

  // Claims (statements)
  if (entity.claims) {
    for (const [propId, claims] of Object.entries(entity.claims)) {
      for (const claim of claims) {
        if (claim.mainsnak.snaktype !== 'value' || !claim.mainsnak.datavalue) continue;

        const dv = claim.mainsnak.datavalue;
        let triple: Triple | null = null;

        switch (dv.type) {
          case 'wikibase-entityid':
            // Reference to another entity
            if (dv.value.id) {
              triple = {
                subject: entityId,
                predicate: propId,
                object: { type: ObjectType.REF, value: `https://wikidata.org/entity/${dv.value.id}` },
                timestamp: ts,
                txId,
              };
            }
            break;

          case 'string':
            triple = {
              subject: entityId,
              predicate: propId,
              object: { type: ObjectType.STRING, value: dv.value.slice(0, 1000) },
              timestamp: ts,
              txId,
            };
            break;

          case 'quantity':
            if (dv.value.amount) {
              const amount = parseFloat(dv.value.amount);
              if (!isNaN(amount)) {
                triple = {
                  subject: entityId,
                  predicate: propId,
                  object: { type: ObjectType.FLOAT64, value: amount },
                  timestamp: ts,
                  txId,
                };
              }
            }
            break;

          case 'time':
            if (dv.value.time) {
              triple = {
                subject: entityId,
                predicate: propId,
                object: { type: ObjectType.STRING, value: dv.value.time },
                timestamp: ts,
                txId,
              };
            }
            break;

          case 'monolingualtext':
            if (dv.value.text) {
              triple = {
                subject: entityId,
                predicate: propId,
                object: { type: ObjectType.STRING, value: dv.value.text.slice(0, 1000) },
                timestamp: ts,
                txId,
              };
            }
            break;
        }

        if (triple) yield triple;
      }
    }
  }

  // Wikipedia sitelinks count
  if (entity.sitelinks) {
    yield {
      subject: entityId,
      predicate: 'sitelinkCount',
      object: { type: ObjectType.INT64, value: BigInt(Object.keys(entity.sitelinks).length) },
      timestamp: ts,
      txId,
    };
  }
}

// ============================================================================
// MAIN LOADER
// ============================================================================

async function loadWikidata(maxEntities: number = Infinity): Promise<void> {
  console.log('📚 Full Wikidata Dump Loader');
  console.log('============================');
  console.log(`Source: ${WIKIDATA_DUMP_URL}`);
  console.log(`Max entities: ${maxEntities === Infinity ? 'unlimited (~100M)' : maxEntities.toLocaleString()}`);
  console.log('');

  if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

  const txId = generateULID();
  const ts = BigInt(Date.now());

  let entitiesProcessed = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalBytes = 0;
  let errors = 0;

  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/wikidata-full/chunks/${chunkId}.graphcol`;
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

  // Use curl + bzcat to stream and decompress
  console.log('📥 Streaming Wikidata dump (this will take many hours)...');
  console.log('   Using: curl | bzcat | process line by line');
  console.log('');

  // Spawn curl to stream the download
  const curl = spawn('curl', ['-s', '-L', WIKIDATA_DUMP_URL], { stdio: ['ignore', 'pipe', 'inherit'] });

  // Spawn bzcat to decompress
  const bzcat = spawn('bzcat', [], { stdio: ['pipe', 'pipe', 'inherit'] });

  // Pipe curl output to bzcat
  curl.stdout.pipe(bzcat.stdin);

  // Process decompressed output line by line
  const rl = createInterface({
    input: bzcat.stdout,
    crlfDelay: Infinity,
  });

  const startTime = Date.now();

  for await (const line of rl) {
    if (entitiesProcessed >= maxEntities) {
      console.log(`\n  Reached max entities limit (${maxEntities.toLocaleString()})`);
      break;
    }

    // Skip array brackets
    let jsonLine = line.trim();
    if (jsonLine === '[' || jsonLine === ']') continue;
    if (jsonLine.endsWith(',')) jsonLine = jsonLine.slice(0, -1);
    if (!jsonLine || jsonLine.length < 10) continue;

    try {
      const entity = JSON.parse(jsonLine) as WikidataEntity;
      if (!entity.id) continue;

      for (const triple of generateTriples(entity, txId, ts)) {
        triples.push(triple);
        triplesGenerated++;
      }

      entitiesProcessed++;

      if (triples.length >= CHUNK_TRIPLE_LIMIT) {
        await flushChunk();
      }

      if (entitiesProcessed % 100_000 === 0) {
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = entitiesProcessed / elapsed;
        const eta = maxEntities === Infinity ? 'unknown' : `${((maxEntities - entitiesProcessed) / rate / 3600).toFixed(1)}h`;
        console.log(`  📊 ${entitiesProcessed.toLocaleString()} entities, ${triplesGenerated.toLocaleString()} triples, ${chunkIndex} chunks, ${rate.toFixed(0)}/s, ETA: ${eta}`);
      }
    } catch {
      errors++;
    }
  }

  // Cleanup subprocesses
  curl.kill();
  bzcat.kill();

  // Final flush
  await flushChunk();

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: 'wikidata-full',
    source: WIKIDATA_DUMP_URL,
    stats: {
      totalTriples: triplesGenerated,
      totalChunks: chunkIndex,
      totalEntities: entitiesProcessed,
      totalSizeBytes: totalBytes,
      parseErrors: errors,
    },
    createdAt: new Date().toISOString(),
    loadDuration: `${((Date.now() - startTime) / 1000 / 3600).toFixed(2)} hours`,
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/wikidata-full/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  const duration = (Date.now() - startTime) / 1000 / 3600;
  console.log('');
  console.log('✅ Completed Wikidata full load:');
  console.log(`   Entities: ${entitiesProcessed.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalBytes / 1024 / 1024 / 1024).toFixed(2)}GB`);
  console.log(`   Duration: ${duration.toFixed(2)} hours`);
  console.log(`   Rate: ${(entitiesProcessed / duration / 3600).toFixed(0)} entities/sec`);
}

const maxEntities = parseInt(process.argv[2] || '', 10) || Infinity;
loadWikidata(maxEntities).catch(console.error);
