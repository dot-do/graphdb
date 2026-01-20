#!/usr/bin/env npx tsx
/**
 * Wikidata Parquet Loader
 *
 * Streams the full Wikidata JSON dump and outputs Parquet files with:
 * - Actual URLs as identifiers (dereferenceable)
 * - Sort keys for storage locality (reversed hostname)
 * - Full Wikidata type support (coords, dates, quantities, etc.)
 * - External IDs normalized to canonical URLs
 *
 * Schema optimized for graph queries and prefix scans.
 *
 * Usage:
 *   npx tsx scripts/wikidata-parquet-loader.ts [maxEntities]
 */

import { createWriteStream, existsSync, mkdirSync, unlinkSync, writeFileSync, statSync } from 'fs';
import { spawn } from 'child_process';
import { execSync } from 'child_process';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { createInterface } from 'readline';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONSTANTS
// ============================================================================

const WIKIDATA_DUMP_URL = 'https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2';
const R2_BUCKET = 'graphdb-lakehouse-prod';
const CHUNK_SIZE = 500_000; // Rows per parquet file
const TEMP_DIR = '/tmp/wikidata-parquet-loader';

// Object types
const ObjectType = {
  INT64: 3,
  FLOAT64: 4,
  STRING: 5,
  URL: 6,
  TIMESTAMP: 7,
  REF: 10,
  GEO_POINT: 11,
  MONOLINGUAL: 12,
  QUANTITY: 13,
} as const;

// External ID mappings to canonical URLs
const EXTERNAL_ID_URLS: Record<string, (v: string) => string> = {
  // Social
  P2002: v => `https://x.com/${v}`,
  P2003: v => `https://instagram.com/${v}`,
  P2013: v => `https://facebook.com/${v}`,
  P2037: v => `https://github.com/${v}`,
  P2397: v => `https://youtube.com/channel/${v}`,
  P4264: v => `https://linkedin.com/company/${v}`,
  P6634: v => `https://linkedin.com/in/${v}`,

  // Entertainment
  P345: v => `https://imdb.com/name/${v}`,
  P4947: v => `https://imdb.com/title/${v}`,
  P1651: v => `https://youtube.com/watch?v=${v}`,
  P1902: v => `https://open.spotify.com/artist/${v}`,
  P2205: v => `https://open.spotify.com/album/${v}`,
  P4903: v => `https://open.spotify.com/track/${v}`,

  // Academic
  P496: v => `https://orcid.org/${v}`,
  P214: v => `https://viaf.org/viaf/${v}`,
  P227: v => `https://d-nb.info/gnd/${v}`,
  P244: v => `https://id.loc.gov/authorities/${v}`,
  P698: v => `https://pubmed.ncbi.nlm.nih.gov/${v}`,
  P356: v => `https://doi.org/${v}`,
  P818: v => `https://arxiv.org/abs/${v}`,

  // Geography
  P1566: v => `https://geonames.org/${v}`,
  P402: v => `https://openstreetmap.org/relation/${v}`,

  // Reference
  P18: v => `https://commons.wikimedia.org/wiki/File:${v}`,
  P373: v => `https://commons.wikimedia.org/wiki/Category:${v}`,
};

interface WikidataEntity {
  id: string;
  type: string;
  labels?: Record<string, { language: string; value: string }>;
  descriptions?: Record<string, { language: string; value: string }>;
  aliases?: Record<string, Array<{ language: string; value: string }>>;
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

interface Triple {
  subject: string;
  subject_sort_key: string;
  predicate: string;
  obj_type: number;
  obj_ref: string | null;
  obj_ref_sort_key: string | null;
  obj_string: string | null;
  obj_url: string | null;
  obj_int64: bigint | null;
  obj_float64: number | null;
  obj_lat: number | null;
  obj_lng: number | null;
  obj_timestamp: bigint | null;
  obj_precision: number | null;
  obj_unit: string | null;
  obj_globe: string | null;
  obj_lang: string | null;
  obj_calendar: string | null;
  ts: bigint;
  tx_id: string;
}

// ============================================================================
// HELPERS
// ============================================================================

function urlToSortKey(url: string): string {
  try {
    const parsed = new URL(url);
    const hostParts = parsed.hostname.split('.').reverse();
    const pathParts = parsed.pathname.split('/').filter(p => p).map(p => `/${p}`);
    return [...hostParts, ...pathParts].join(',');
  } catch {
    return '_invalid';
  }
}

function wikidataUrl(id: string): string {
  return `https://www.wikidata.org/entity/${id}`;
}

function wikidataSortKey(id: string): string {
  return `org,wikidata,www,/entity,/${id}`;
}

// ULID generator
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

function makeTriple(
  subject: string,
  predicate: string,
  objType: number,
  objValue: any,
  ts: bigint,
  txId: string,
  extras: Partial<Triple> = {}
): Triple {
  return {
    subject,
    subject_sort_key: subject.startsWith('https://www.wikidata.org')
      ? wikidataSortKey(subject.split('/').pop()!)
      : urlToSortKey(subject),
    predicate,
    obj_type: objType,
    obj_ref: null,
    obj_ref_sort_key: null,
    obj_string: null,
    obj_url: null,
    obj_int64: null,
    obj_float64: null,
    obj_lat: null,
    obj_lng: null,
    obj_timestamp: null,
    obj_precision: null,
    obj_unit: null,
    obj_globe: null,
    obj_lang: null,
    obj_calendar: null,
    ts,
    tx_id: txId,
    ...extras,
  };
}

// ============================================================================
// TRIPLE GENERATOR
// ============================================================================

function* generateTriples(entity: WikidataEntity, txId: string, ts: bigint): Generator<Triple> {
  const entityUrl = wikidataUrl(entity.id);

  // $type
  yield makeTriple(entityUrl, '$type', ObjectType.STRING, null, ts, txId, {
    obj_string: entity.type === 'property' ? 'Property' : 'Entity',
  });

  // Labels (all languages)
  if (entity.labels) {
    for (const [lang, label] of Object.entries(entity.labels)) {
      yield makeTriple(entityUrl, 'label', ObjectType.MONOLINGUAL, null, ts, txId, {
        obj_string: label.value,
        obj_lang: lang,
      });
    }
  }

  // Descriptions (all languages)
  if (entity.descriptions) {
    for (const [lang, desc] of Object.entries(entity.descriptions)) {
      yield makeTriple(entityUrl, 'description', ObjectType.MONOLINGUAL, null, ts, txId, {
        obj_string: desc.value.slice(0, 1000),
        obj_lang: lang,
      });
    }
  }

  // Aliases (all languages)
  if (entity.aliases) {
    for (const [lang, aliases] of Object.entries(entity.aliases)) {
      for (const alias of aliases) {
        yield makeTriple(entityUrl, 'alias', ObjectType.MONOLINGUAL, null, ts, txId, {
          obj_string: alias.value,
          obj_lang: lang,
        });
      }
    }
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
            if (dv.value.id) {
              const refUrl = wikidataUrl(dv.value.id);
              triple = makeTriple(entityUrl, propId, ObjectType.REF, null, ts, txId, {
                obj_ref: refUrl,
                obj_ref_sort_key: wikidataSortKey(dv.value.id),
              });
            }
            break;

          case 'string':
            // Check if this is an external ID with known URL mapping
            const urlMapper = EXTERNAL_ID_URLS[propId];
            if (urlMapper) {
              const canonicalUrl = urlMapper(dv.value);
              triple = makeTriple(entityUrl, propId, ObjectType.URL, null, ts, txId, {
                obj_url: canonicalUrl,
                obj_string: dv.value, // Keep original ID too
              });
            } else {
              triple = makeTriple(entityUrl, propId, ObjectType.STRING, null, ts, txId, {
                obj_string: dv.value.slice(0, 2000),
              });
            }
            break;

          case 'url':
            triple = makeTriple(entityUrl, propId, ObjectType.URL, null, ts, txId, {
              obj_url: dv.value,
            });
            break;

          case 'quantity':
            if (dv.value.amount) {
              const amount = parseFloat(dv.value.amount);
              if (!isNaN(amount)) {
                triple = makeTriple(entityUrl, propId, ObjectType.QUANTITY, null, ts, txId, {
                  obj_float64: amount,
                  obj_unit: dv.value.unit !== '1' ? dv.value.unit : null,
                });
              }
            }
            break;

          case 'time':
            if (dv.value.time) {
              // Parse Wikidata time format: +2024-01-15T00:00:00Z
              let timestamp: bigint | null = null;
              try {
                const timeStr = dv.value.time.replace(/^[+-]/, '');
                const date = new Date(timeStr);
                if (!isNaN(date.getTime())) {
                  timestamp = BigInt(date.getTime());
                }
              } catch {}

              triple = makeTriple(entityUrl, propId, ObjectType.TIMESTAMP, null, ts, txId, {
                obj_timestamp: timestamp,
                obj_string: dv.value.time, // Keep original for display
                obj_precision: dv.value.precision,
                obj_calendar: dv.value.calendarmodel?.split('/').pop() || null,
              });
            }
            break;

          case 'globecoordinate':
            if (dv.value.latitude !== undefined && dv.value.longitude !== undefined) {
              triple = makeTriple(entityUrl, propId, ObjectType.GEO_POINT, null, ts, txId, {
                obj_lat: dv.value.latitude,
                obj_lng: dv.value.longitude,
                obj_globe: dv.value.globe?.split('/').pop() || 'Q2', // Default Earth
              });
            }
            break;

          case 'monolingualtext':
            if (dv.value.text) {
              triple = makeTriple(entityUrl, propId, ObjectType.MONOLINGUAL, null, ts, txId, {
                obj_string: dv.value.text.slice(0, 2000),
                obj_lang: dv.value.language,
              });
            }
            break;

          case 'commonsMedia':
            if (dv.value) {
              const commonsUrl = `https://commons.wikimedia.org/wiki/File:${encodeURIComponent(dv.value)}`;
              triple = makeTriple(entityUrl, propId, ObjectType.URL, null, ts, txId, {
                obj_url: commonsUrl,
                obj_string: dv.value,
              });
            }
            break;

          case 'external-id':
            // Same handling as string with external ID
            const extUrlMapper = EXTERNAL_ID_URLS[propId];
            if (extUrlMapper) {
              const canonicalUrl = extUrlMapper(dv.value);
              triple = makeTriple(entityUrl, propId, ObjectType.URL, null, ts, txId, {
                obj_url: canonicalUrl,
                obj_string: dv.value,
              });
            } else {
              triple = makeTriple(entityUrl, propId, ObjectType.STRING, null, ts, txId, {
                obj_string: dv.value,
              });
            }
            break;

          case 'geo-shape':
          case 'tabular-data':
            // Store as URL to Commons Data
            if (dv.value) {
              triple = makeTriple(entityUrl, propId, ObjectType.URL, null, ts, txId, {
                obj_url: `https://commons.wikimedia.org/wiki/Data:${encodeURIComponent(dv.value)}`,
                obj_string: dv.value,
              });
            }
            break;

          case 'math':
          case 'musical-notation':
            // Store as string
            if (dv.value) {
              triple = makeTriple(entityUrl, propId, ObjectType.STRING, null, ts, txId, {
                obj_string: dv.value.slice(0, 5000),
              });
            }
            break;
        }

        if (triple) yield triple;
      }
    }
  }

  // Sitelinks
  if (entity.sitelinks) {
    for (const [site, link] of Object.entries(entity.sitelinks)) {
      // Convert sitelink to URL
      let siteUrl: string | null = null;
      if (site.endsWith('wiki')) {
        const lang = site.replace('wiki', '');
        siteUrl = `https://${lang}.wikipedia.org/wiki/${encodeURIComponent(link.title)}`;
      } else if (site.endsWith('wikisource')) {
        const lang = site.replace('wikisource', '');
        siteUrl = `https://${lang}.wikisource.org/wiki/${encodeURIComponent(link.title)}`;
      }

      if (siteUrl) {
        yield makeTriple(entityUrl, 'sitelink', ObjectType.URL, null, ts, txId, {
          obj_url: siteUrl,
          obj_string: site,
        });
      }
    }
  }
}

// ============================================================================
// PARQUET WRITER (via DuckDB)
// ============================================================================

async function writeParquetChunk(triples: Triple[], chunkIndex: number): Promise<{ path: string; size: number }> {
  const chunkId = `chunk_${chunkIndex.toString().padStart(6, '0')}`;
  const jsonlPath = join(TEMP_DIR, `${chunkId}.jsonl`);
  const parquetPath = join(TEMP_DIR, `${chunkId}.parquet`);
  const r2Path = `datasets/wikidata-parquet/chunks/${chunkId}.parquet`;

  // Write JSONL (more robust than CSV for complex data)
  const jsonlLines: string[] = [];
  for (const t of triples) {
    jsonlLines.push(JSON.stringify({
      subject: t.subject,
      subject_sort_key: t.subject_sort_key,
      predicate: t.predicate,
      obj_type: t.obj_type,
      obj_ref: t.obj_ref,
      obj_ref_sort_key: t.obj_ref_sort_key,
      obj_string: t.obj_string,
      obj_url: t.obj_url,
      obj_int64: t.obj_int64 !== null ? Number(t.obj_int64) : null,
      obj_float64: t.obj_float64,
      obj_lat: t.obj_lat,
      obj_lng: t.obj_lng,
      obj_timestamp: t.obj_timestamp !== null ? Number(t.obj_timestamp) : null,
      obj_precision: t.obj_precision,
      obj_unit: t.obj_unit,
      obj_globe: t.obj_globe,
      obj_lang: t.obj_lang,
      obj_calendar: t.obj_calendar,
      ts: Number(t.ts),
      tx_id: t.tx_id,
    }));
  }
  writeFileSync(jsonlPath, jsonlLines.join('\n'));

  // Use DuckDB to convert to Parquet with sorting
  const sqlPath = join(TEMP_DIR, `${chunkId}.sql`);
  const duckdbSql = `
COPY (
  SELECT * FROM read_json_auto('${jsonlPath}')
  ORDER BY subject_sort_key, subject
)
TO '${parquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
`;

  writeFileSync(sqlPath, duckdbSql);
  execSync(`duckdb < "${sqlPath}"`, { stdio: 'pipe' });
  unlinkSync(sqlPath);

  // Get file size
  const { size } = statSync(parquetPath);

  // Upload to R2
  execSync(`npx wrangler r2 object put ${R2_BUCKET}/${r2Path} --file=${parquetPath} --content-type=application/octet-stream`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });

  // Cleanup
  unlinkSync(jsonlPath);
  unlinkSync(parquetPath);

  return { path: r2Path, size };
}

// ============================================================================
// MAIN LOADER
// ============================================================================

async function loadWikidataParquet(maxEntities: number = Infinity): Promise<void> {
  console.log('📚 Wikidata Parquet Loader');
  console.log('==========================');
  console.log(`Source: ${WIKIDATA_DUMP_URL}`);
  console.log(`Max entities: ${maxEntities === Infinity ? 'unlimited (~100M)' : maxEntities.toLocaleString()}`);
  console.log(`Output: Parquet with full Wikidata types`);
  console.log('');

  if (!existsSync(TEMP_DIR)) mkdirSync(TEMP_DIR, { recursive: true });

  const txId = generateULID();
  const ts = BigInt(Date.now());
  const startTime = Date.now();

  let entitiesProcessed = 0;
  let triplesGenerated = 0;
  let chunkIndex = 0;
  let triples: Triple[] = [];
  let totalBytes = 0;
  let errors = 0;

  const flushChunk = async () => {
    if (triples.length === 0) return;

    console.log(`  📤 Writing chunk_${chunkIndex.toString().padStart(6, '0')}: ${triples.length.toLocaleString()} triples...`);

    try {
      const { size } = await writeParquetChunk(triples, chunkIndex);
      totalBytes += size;
      console.log(`      → ${(size / 1024 / 1024).toFixed(2)}MB parquet`);
    } catch (err: any) {
      console.error(`  ❌ Failed to write chunk: ${err.message}`);
      errors++;
    }

    chunkIndex++;
    triples = [];
  };

  console.log('📥 Streaming Wikidata dump...');
  console.log('   Using: curl | bzcat | process line by line');
  console.log('');

  const curl = spawn('curl', ['-s', '-L', WIKIDATA_DUMP_URL], { stdio: ['ignore', 'pipe', 'inherit'] });
  const bzcat = spawn('bzcat', [], { stdio: ['pipe', 'pipe', 'inherit'] });
  curl.stdout.pipe(bzcat.stdin);

  const rl = createInterface({
    input: bzcat.stdout,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    if (entitiesProcessed >= maxEntities) {
      console.log(`\n  Reached max entities limit (${maxEntities.toLocaleString()})`);
      break;
    }

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

      if (triples.length >= CHUNK_SIZE) {
        await flushChunk();
      }

      if (entitiesProcessed % 50_000 === 0) {
        const elapsed = (Date.now() - startTime) / 1000;
        const rate = entitiesProcessed / elapsed;
        console.log(`  📊 ${entitiesProcessed.toLocaleString()} entities, ${triplesGenerated.toLocaleString()} triples, ${rate.toFixed(0)}/s`);
      }
    } catch {
      errors++;
    }
  }

  curl.kill();
  bzcat.kill();

  await flushChunk();

  // Upload manifest
  const manifest = {
    version: 2,
    format: 'parquet',
    dataset: 'wikidata-parquet',
    source: WIKIDATA_DUMP_URL,
    schema: {
      subject: 'VARCHAR - actual URL',
      subject_sort_key: 'VARCHAR - reversed host for sorting',
      predicate: 'VARCHAR - property ID or built-in',
      obj_type: 'TINYINT - type enum',
      obj_ref: 'VARCHAR - entity reference URL',
      obj_ref_sort_key: 'VARCHAR - ref sort key',
      obj_string: 'VARCHAR - string value',
      obj_url: 'VARCHAR - URL value',
      obj_int64: 'BIGINT - integer value',
      obj_float64: 'DOUBLE - float value',
      obj_lat: 'DOUBLE - latitude',
      obj_lng: 'DOUBLE - longitude',
      obj_timestamp: 'BIGINT - epoch ms',
      obj_precision: 'TINYINT - time precision',
      obj_unit: 'VARCHAR - quantity unit',
      obj_globe: 'VARCHAR - coordinate globe',
      obj_lang: 'VARCHAR - language code',
      obj_calendar: 'VARCHAR - calendar model',
    },
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

  const manifestPath = join(TEMP_DIR, 'manifest.json');
  writeFileSync(manifestPath, JSON.stringify(manifest, null, 2));

  execSync(`npx wrangler r2 object put ${R2_BUCKET}/datasets/wikidata-parquet/manifest.json --file=${manifestPath} --content-type=application/json`, {
    stdio: 'pipe',
    cwd: join(__dirname, '..'),
  });
  unlinkSync(manifestPath);

  const duration = (Date.now() - startTime) / 1000 / 3600;
  console.log('');
  console.log('✅ Completed Wikidata Parquet load:');
  console.log(`   Entities: ${entitiesProcessed.toLocaleString()}`);
  console.log(`   Triples: ${triplesGenerated.toLocaleString()}`);
  console.log(`   Chunks: ${chunkIndex}`);
  console.log(`   Size: ${(totalBytes / 1024 / 1024 / 1024).toFixed(2)}GB`);
  console.log(`   Duration: ${duration.toFixed(2)} hours`);
}

const maxEntities = parseInt(process.argv[2] || '', 10) || Infinity;
loadWikidataParquet(maxEntities).catch(console.error);
