#!/usr/bin/env npx tsx
/**
 * Local Wiktionary Data Loader
 *
 * Runs locally to bypass Worker memory limits.
 * Downloads kaikki.org Wiktionary JSONL and uploads to R2.
 *
 * Source: https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl
 * - 2.6GB NDJSON, ~1M English word entries
 *
 * Usage:
 *   npx tsx scripts/local-wiktionary-loader.ts
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync } from 'fs';
import { createGunzip } from 'zlib';
import { pipeline } from 'stream/promises';
import { Readable, Transform } from 'stream';
import { execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const WIKTIONARY_URL = 'https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl';
const R2_BUCKET = 'graphdb-lakehouse-prod';
const NAMESPACE = 'https://wiktionary.org/';
const CHUNK_TRIPLE_LIMIT = 250_000;
const TEMP_DIR = '/tmp/wiktionary-loader';

const ObjectType = {
  STRING: 5,
  INT32: 2,
  REF: 10,
} as const;

interface Triple {
  subject: string;
  predicate: string;
  object: { type: number; value: any };
  timestamp: bigint;
  txId: string;
}

interface KaikkiEntry {
  word: string;
  pos: string;
  senses?: Array<{
    glosses?: string[];
    tags?: string[];
  }>;
  sounds?: Array<{
    ipa?: string;
    audio?: string;
  }>;
  etymology_text?: string;
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
      o: { t: t.object.type, v: t.object.value },
      ts: Number(t.timestamp),
      tx: t.txId,
    })),
  };
  return new TextEncoder().encode(JSON.stringify(data));
}

// ============================================================================
// TRIPLE GENERATOR
// ============================================================================

function* generateTriples(entry: KaikkiEntry, txId: string, idx: number): Generator<Triple> {
  const word = entry.word;
  const pos = entry.pos || 'unknown';
  const entityId = `https://wiktionary.org/word/${encodeURIComponent(word)}/${pos}/${idx}`;
  const ts = BigInt(Date.now());

  // $type
  yield { subject: entityId, predicate: '$type', object: { type: ObjectType.STRING, value: 'WordEntry' }, timestamp: ts, txId };

  // word
  yield { subject: entityId, predicate: 'word', object: { type: ObjectType.STRING, value: word }, timestamp: ts, txId };

  // partOfSpeech
  yield { subject: entityId, predicate: 'partOfSpeech', object: { type: ObjectType.STRING, value: pos }, timestamp: ts, txId };

  // First sense definition
  if (entry.senses?.[0]?.glosses?.[0]) {
    yield { subject: entityId, predicate: 'definition', object: { type: ObjectType.STRING, value: entry.senses[0].glosses[0] }, timestamp: ts, txId };
  }

  // IPA pronunciation
  const ipa = entry.sounds?.find(s => s.ipa)?.ipa;
  if (ipa) {
    yield { subject: entityId, predicate: 'pronunciation', object: { type: ObjectType.STRING, value: ipa }, timestamp: ts, txId };
  }

  // Etymology
  if (entry.etymology_text) {
    yield { subject: entityId, predicate: 'etymology', object: { type: ObjectType.STRING, value: entry.etymology_text.slice(0, 1000) }, timestamp: ts, txId };
  }
}

// ============================================================================
// MAIN LOADER
// ============================================================================

async function loadWiktionary(): Promise<void> {
  console.log('📚 Wiktionary Local Loader');
  console.log('==========================');
  console.log(`Source: ${WIKTIONARY_URL}`);
  console.log('');

  if (!existsSync(TEMP_DIR)) {
    mkdirSync(TEMP_DIR, { recursive: true });
  }

  const txId = generateULID();
  let entriesProcessed = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalSizeBytes = 0;
  const wordCounts = new Map<string, number>();

  const flushChunk = async () => {
    if (triples.length === 0) return;

    const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
    const chunkPath = `datasets/wiktionary/chunks/${chunkId}.graphcol`;
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

  console.log('📥 Fetching Wiktionary data (2.6GB)...');
  const response = await fetch(WIKTIONARY_URL);
  if (!response.ok) {
    throw new Error(`Failed to fetch: ${response.status}`);
  }

  const nodeStream = Readable.fromWeb(response.body as any);

  // Process line by line
  const rl = createInterface({
    input: nodeStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (!line.trim()) continue;

    try {
      const entry = JSON.parse(line) as KaikkiEntry;
      const word = entry.word;

      // Track unique word+pos combinations
      const key = `${word}:${entry.pos}`;
      const idx = (wordCounts.get(key) || 0);
      wordCounts.set(key, idx + 1);

      for (const triple of generateTriples(entry, txId, idx)) {
        triples.push(triple);
        triplesGenerated++;
      }

      entriesProcessed++;

      if (triples.length >= CHUNK_TRIPLE_LIMIT) {
        await flushChunk();
      }

      if (entriesProcessed % 100_000 === 0) {
        console.log(`  📊 ${entriesProcessed.toLocaleString()} entries, ${triplesGenerated.toLocaleString()} triples, ${chunkIndex} chunks`);
      }
    } catch {
      // Skip malformed lines
    }
  }

  // Final flush
  await flushChunk();

  // Upload manifest
  const manifest = {
    version: 1,
    namespace: NAMESPACE,
    dataset: 'wiktionary',
    stats: {
      totalTriples: triplesGenerated,
      totalChunks: chunkIndex,
      totalEntries: entriesProcessed,
      totalSizeBytes,
      uniqueWords: wordCounts.size,
    },
    createdAt: new Date().toISOString(),
  };

  const manifestPath = join(TEMP_DIR, 'index.json');
  createWriteStream(manifestPath).end(JSON.stringify(manifest, null, 2));
  await new Promise(resolve => setTimeout(resolve, 100));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/wiktionary/index.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  console.log('');
  console.log('✅ Completed Wiktionary load:');
  console.log(`   Entries: ${entriesProcessed.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalSizeBytes / 1024 / 1024).toFixed(2)}MB`);
  console.log(`   Unique words: ${wordCounts.size.toLocaleString()}`);
}

loadWiktionary().catch(console.error);
