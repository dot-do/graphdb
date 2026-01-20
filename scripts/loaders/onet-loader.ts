/**
 * O*NET Data Loader Worker
 *
 * Downloads O*NET occupational data from source, parses TSV files,
 * converts to GraphDB triples, and uploads GraphCol chunks to R2.
 *
 * O*NET Data Source: https://www.onetcenter.org/dl_files/database/db_29_1_text.zip
 *
 * Entity Model:
 * - Occupation: $id=https://onet.org/occupation/{code}
 *   - predicates: title, description, jobZone
 * - Skill: $id=https://onet.org/skill/{id}
 *   - predicates: name, category
 * - Ability: $id=https://onet.org/ability/{id}
 *   - predicates: name, category
 * - Knowledge: $id=https://onet.org/knowledge/{id}
 *   - predicates: name, category
 *
 * Relations (as separate triples with level/importance):
 * - requiresSkill: REF from Occupation to Skill
 * - requiresAbility: REF from Occupation to Ability
 * - requiresKnowledge: REF from Occupation to Knowledge
 * - skillLevel, skillImportance: FLOAT64 scores
 *
 * R2 Output:
 * - datasets/onet/chunks/chunk_{n}.gcol
 * - datasets/onet/bloom/filter.json
 * - datasets/onet/index.json
 *
 * @packageDocumentation
 */

import type { Triple } from '../../src/core/triple';
import type { EntityId, Predicate, TransactionId, Namespace } from '../../src/core/types';
import { ObjectType, createEntityId, createPredicate, createTransactionId, createNamespace } from '../../src/core/types';
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
// Types
// ============================================================================

interface Env {
  LAKEHOUSE: R2Bucket;
}

interface ONetOccupation {
  code: string;
  title: string;
  description: string;
}

interface ONetSkill {
  elementId: string;
  elementName: string;
  scaleId: string;
  scaleName: string;
}

interface ONetOccupationSkill {
  occupationCode: string;
  elementId: string;
  elementName: string;
  scaleId: string;
  dataValue: number;
}

interface LoaderIndex {
  version: string;
  source: string;
  loadedAt: string;
  namespace: string;
  stats: {
    occupations: number;
    skills: number;
    abilities: number;
    knowledge: number;
    totalTriples: number;
    totalChunks: number;
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

// ============================================================================
// Constants
// ============================================================================

const ONET_DATA_URL = 'https://www.onetcenter.org/dl_files/database/db_29_1_text.zip';
const ONET_NAMESPACE = 'https://onet.org' as Namespace;
const CHUNK_SIZE = 10000; // Triples per chunk
const R2_PREFIX = 'datasets/onet';

// ============================================================================
// ULID GENERATOR (Proper Crockford Base32)
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
let lastTime = 0;
let lastRandom = new Uint8Array(10);

function generateTxId(): TransactionId {
  let now = Date.now();

  if (now === lastTime) {
    // Increment random part to ensure monotonicity
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

  // Encode timestamp (first 10 chars) using Crockford Base32
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

  return createTransactionId(ulid.slice(0, 26));
}

// ============================================================================
// TSV Parsing
// ============================================================================

/**
 * Parse a TSV line respecting quoted fields
 */
function parseTsvLine(line: string): string[] {
  const fields: string[] = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        // Escaped quote
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === '\t' && !inQuotes) {
      fields.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  fields.push(current);
  return fields;
}

/**
 * Parse TSV content into array of objects
 */
function parseTsv<T>(content: string, mapper: (fields: string[], headers: string[]) => T | null): T[] {
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  if (lines.length < 2) return [];

  const headers = parseTsvLine(lines[0]);
  const results: T[] = [];

  for (let i = 1; i < lines.length; i++) {
    const fields = parseTsvLine(lines[i]);
    const item = mapper(fields, headers);
    if (item) results.push(item);
  }

  return results;
}

/**
 * Get field by header name
 */
function getField(fields: string[], headers: string[], name: string): string {
  const idx = headers.indexOf(name);
  return idx >= 0 ? (fields[idx] ?? '') : '';
}

// ============================================================================
// Zip Extraction (using JSZip-compatible approach)
// ============================================================================

/**
 * Simple ZIP file parser for small archives
 * O*NET data is ~20MB so buffering is acceptable
 *
 * ZIP format reference: https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
 */
interface ZipEntry {
  filename: string;
  compressedSize: number;
  uncompressedSize: number;
  compressionMethod: number;
  offset: number;
}

/**
 * Parse ZIP central directory to get file entries
 */
function parseZipDirectory(data: Uint8Array): ZipEntry[] {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
  const entries: ZipEntry[] = [];

  // Find End of Central Directory signature (0x06054b50)
  let eocdOffset = -1;
  for (let i = data.length - 22; i >= 0; i--) {
    if (view.getUint32(i, true) === 0x06054b50) {
      eocdOffset = i;
      break;
    }
  }

  if (eocdOffset === -1) {
    throw new Error('Invalid ZIP: EOCD signature not found');
  }

  // Read EOCD
  const centralDirOffset = view.getUint32(eocdOffset + 16, true);
  const centralDirSize = view.getUint32(eocdOffset + 12, true);
  const entryCount = view.getUint16(eocdOffset + 10, true);

  // Parse central directory entries
  let offset = centralDirOffset;
  const decoder = new TextDecoder();

  for (let i = 0; i < entryCount; i++) {
    if (view.getUint32(offset, true) !== 0x02014b50) {
      throw new Error(`Invalid ZIP: Central directory signature mismatch at entry ${i}`);
    }

    const compressionMethod = view.getUint16(offset + 10, true);
    const compressedSize = view.getUint32(offset + 20, true);
    const uncompressedSize = view.getUint32(offset + 24, true);
    const filenameLength = view.getUint16(offset + 28, true);
    const extraLength = view.getUint16(offset + 30, true);
    const commentLength = view.getUint16(offset + 32, true);
    const localHeaderOffset = view.getUint32(offset + 42, true);

    const filename = decoder.decode(data.subarray(offset + 46, offset + 46 + filenameLength));

    entries.push({
      filename,
      compressionMethod,
      compressedSize,
      uncompressedSize,
      offset: localHeaderOffset,
    });

    offset += 46 + filenameLength + extraLength + commentLength;
  }

  return entries;
}

/**
 * Extract a file from the ZIP archive
 */
async function extractZipEntry(data: Uint8Array, entry: ZipEntry): Promise<Uint8Array> {
  const view = new DataView(data.buffer, data.byteOffset, data.byteLength);

  // Verify local file header
  if (view.getUint32(entry.offset, true) !== 0x04034b50) {
    throw new Error('Invalid ZIP: Local file header signature mismatch');
  }

  const filenameLength = view.getUint16(entry.offset + 26, true);
  const extraLength = view.getUint16(entry.offset + 28, true);
  const dataStart = entry.offset + 30 + filenameLength + extraLength;
  const compressedData = data.subarray(dataStart, dataStart + entry.compressedSize);

  if (entry.compressionMethod === 0) {
    // Stored (no compression)
    return compressedData;
  } else if (entry.compressionMethod === 8) {
    // Deflate - use DecompressionStream
    const ds = new DecompressionStream('deflate-raw');
    const writer = ds.writable.getWriter();
    const reader = ds.readable.getReader();

    writer.write(compressedData);
    writer.close();

    const chunks: Uint8Array[] = [];
    let totalLength = 0;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      totalLength += value.length;
    }

    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      result.set(chunk, offset);
      offset += chunk.length;
    }

    return result;
  } else {
    throw new Error(`Unsupported compression method: ${entry.compressionMethod}`);
  }
}

// ============================================================================
// Triple Generation
// ============================================================================

/**
 * Create a triple with proper types
 */
function makeTriple(
  subject: string,
  predicate: string,
  value: unknown,
  objectType: ObjectType,
  txId: TransactionId
): Triple {
  const subjectId = createEntityId(subject);
  const pred = createPredicate(predicate);

  let object: Triple['object'];

  switch (objectType) {
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: String(value) };
      break;
    case ObjectType.FLOAT64:
      object = { type: ObjectType.FLOAT64, value: Number(value) };
      break;
    case ObjectType.INT32:
      object = { type: ObjectType.INT32, value: BigInt(Math.round(Number(value))) };
      break;
    case ObjectType.REF:
      object = { type: ObjectType.REF, value: createEntityId(String(value)) };
      break;
    default:
      object = { type: ObjectType.STRING, value: String(value) };
  }

  return {
    subject: subjectId,
    predicate: pred,
    object,
    timestamp: BigInt(Date.now()),
    txId,
  };
}

/**
 * Generate triples for an occupation
 */
function* generateOccupationTriples(occ: ONetOccupation, txId: TransactionId): Generator<Triple> {
  const subjectUrl = `https://onet.org/occupation/${encodeURIComponent(occ.code)}`;

  yield makeTriple(subjectUrl, '$type', 'Occupation', ObjectType.STRING, txId);
  yield makeTriple(subjectUrl, 'code', occ.code, ObjectType.STRING, txId);
  yield makeTriple(subjectUrl, 'title', occ.title, ObjectType.STRING, txId);
  yield makeTriple(subjectUrl, 'description', occ.description, ObjectType.STRING, txId);
}

/**
 * Generate triples for a skill/ability/knowledge element
 */
function* generateElementTriples(
  elementType: 'skill' | 'ability' | 'knowledge',
  elementId: string,
  elementName: string,
  txId: TransactionId
): Generator<Triple> {
  const subjectUrl = `https://onet.org/${elementType}/${encodeURIComponent(elementId)}`;

  yield makeTriple(subjectUrl, '$type', elementType.charAt(0).toUpperCase() + elementType.slice(1), ObjectType.STRING, txId);
  yield makeTriple(subjectUrl, 'elementId', elementId, ObjectType.STRING, txId);
  yield makeTriple(subjectUrl, 'name', elementName, ObjectType.STRING, txId);
}

/**
 * Generate triples for an occupation-element relationship
 */
function* generateRelationTriples(
  occupationCode: string,
  elementType: 'skill' | 'ability' | 'knowledge',
  elementId: string,
  scaleId: string,
  dataValue: number,
  txId: TransactionId
): Generator<Triple> {
  const occupationUrl = `https://onet.org/occupation/${encodeURIComponent(occupationCode)}`;
  const elementUrl = `https://onet.org/${elementType}/${encodeURIComponent(elementId)}`;

  // Main relationship triple
  const relationName = `requires${elementType.charAt(0).toUpperCase() + elementType.slice(1)}`;
  yield makeTriple(occupationUrl, relationName, elementUrl, ObjectType.REF, txId);

  // Create a relationship entity for level/importance
  // Using a composite ID for the relationship
  const relationId = `${occupationCode}_${elementType}_${elementId}_${scaleId}`;
  const relationUrl = `https://onet.org/relation/${encodeURIComponent(relationId)}`;

  yield makeTriple(relationUrl, '$type', `${elementType.charAt(0).toUpperCase() + elementType.slice(1)}Rating`, ObjectType.STRING, txId);
  yield makeTriple(relationUrl, 'occupation', occupationUrl, ObjectType.REF, txId);
  yield makeTriple(relationUrl, 'element', elementUrl, ObjectType.REF, txId);
  yield makeTriple(relationUrl, 'scaleId', scaleId, ObjectType.STRING, txId);
  yield makeTriple(relationUrl, 'dataValue', dataValue, ObjectType.FLOAT64, txId);
}

// ============================================================================
// Main Loader Logic
// ============================================================================

/**
 * Download and extract O*NET data
 */
async function downloadAndExtract(): Promise<Map<string, string>> {
  console.log(`Downloading O*NET data from ${ONET_DATA_URL}...`);

  const response = await fetch(ONET_DATA_URL);
  if (!response.ok) {
    throw new Error(`Failed to download: ${response.status} ${response.statusText}`);
  }

  const zipData = new Uint8Array(await response.arrayBuffer());
  console.log(`Downloaded ${zipData.length} bytes`);

  // Parse ZIP and extract relevant files
  const entries = parseZipDirectory(zipData);
  console.log(`ZIP contains ${entries.length} files`);

  const files = new Map<string, string>();
  const decoder = new TextDecoder('utf-8');

  // Files we need (they're in a subdirectory)
  const neededFiles = [
    'Occupation Data.txt',
    'Skills.txt',
    'Abilities.txt',
    'Knowledge.txt',
  ];

  for (const entry of entries) {
    const basename = entry.filename.split('/').pop() ?? '';
    if (neededFiles.includes(basename)) {
      console.log(`Extracting ${entry.filename}...`);
      const content = await extractZipEntry(zipData, entry);
      files.set(basename, decoder.decode(content));
      console.log(`  ${content.length} bytes`);
    }
  }

  return files;
}

/**
 * Parse occupation data
 */
function parseOccupations(content: string): ONetOccupation[] {
  return parseTsv(content, (fields, headers) => {
    const code = getField(fields, headers, 'O*NET-SOC Code');
    const title = getField(fields, headers, 'Title');
    const description = getField(fields, headers, 'Description');

    if (!code) return null;

    return { code, title, description };
  });
}

/**
 * Parse skills/abilities/knowledge data
 */
function parseElements(content: string): ONetOccupationSkill[] {
  return parseTsv(content, (fields, headers) => {
    const occupationCode = getField(fields, headers, 'O*NET-SOC Code');
    const elementId = getField(fields, headers, 'Element ID');
    const elementName = getField(fields, headers, 'Element Name');
    const scaleId = getField(fields, headers, 'Scale ID');
    const dataValue = parseFloat(getField(fields, headers, 'Data Value') || '0');

    if (!occupationCode || !elementId) return null;

    return { occupationCode, elementId, elementName, scaleId, dataValue };
  });
}

/**
 * Main loader function
 */
async function loadONetData(bucket: R2Bucket): Promise<LoaderIndex> {
  const txId = generateTxId();
  const startTime = Date.now();

  // Download and extract
  const files = await downloadAndExtract();

  // Parse data
  console.log('Parsing occupation data...');
  const occupations = parseOccupations(files.get('Occupation Data.txt') ?? '');
  console.log(`  ${occupations.length} occupations`);

  console.log('Parsing skills...');
  const skills = parseElements(files.get('Skills.txt') ?? '');
  console.log(`  ${skills.length} skill ratings`);

  console.log('Parsing abilities...');
  const abilities = parseElements(files.get('Abilities.txt') ?? '');
  console.log(`  ${abilities.length} ability ratings`);

  console.log('Parsing knowledge...');
  const knowledge = parseElements(files.get('Knowledge.txt') ?? '');
  console.log(`  ${knowledge.length} knowledge ratings`);

  // Generate triples
  console.log('Generating triples...');
  const triples: Triple[] = [];
  const entityIds = new Set<string>();

  // Occupation triples
  for (const occ of occupations) {
    const subjectUrl = `https://onet.org/occupation/${encodeURIComponent(occ.code)}`;
    entityIds.add(subjectUrl);
    for (const triple of generateOccupationTriples(occ, txId)) {
      triples.push(triple);
    }
  }

  // Unique elements
  const uniqueSkills = new Map<string, string>();
  const uniqueAbilities = new Map<string, string>();
  const uniqueKnowledge = new Map<string, string>();

  // Process skills
  for (const skill of skills) {
    if (!uniqueSkills.has(skill.elementId)) {
      uniqueSkills.set(skill.elementId, skill.elementName);
      const url = `https://onet.org/skill/${encodeURIComponent(skill.elementId)}`;
      entityIds.add(url);
      for (const triple of generateElementTriples('skill', skill.elementId, skill.elementName, txId)) {
        triples.push(triple);
      }
    }
    for (const triple of generateRelationTriples(
      skill.occupationCode,
      'skill',
      skill.elementId,
      skill.scaleId,
      skill.dataValue,
      txId
    )) {
      triples.push(triple);
      // Add relation entity ID
      const relationId = `${skill.occupationCode}_skill_${skill.elementId}_${skill.scaleId}`;
      entityIds.add(`https://onet.org/relation/${encodeURIComponent(relationId)}`);
    }
  }

  // Process abilities
  for (const ability of abilities) {
    if (!uniqueAbilities.has(ability.elementId)) {
      uniqueAbilities.set(ability.elementId, ability.elementName);
      const url = `https://onet.org/ability/${encodeURIComponent(ability.elementId)}`;
      entityIds.add(url);
      for (const triple of generateElementTriples('ability', ability.elementId, ability.elementName, txId)) {
        triples.push(triple);
      }
    }
    for (const triple of generateRelationTriples(
      ability.occupationCode,
      'ability',
      ability.elementId,
      ability.scaleId,
      ability.dataValue,
      txId
    )) {
      triples.push(triple);
      const relationId = `${ability.occupationCode}_ability_${ability.elementId}_${ability.scaleId}`;
      entityIds.add(`https://onet.org/relation/${encodeURIComponent(relationId)}`);
    }
  }

  // Process knowledge
  for (const k of knowledge) {
    if (!uniqueKnowledge.has(k.elementId)) {
      uniqueKnowledge.set(k.elementId, k.elementName);
      const url = `https://onet.org/knowledge/${encodeURIComponent(k.elementId)}`;
      entityIds.add(url);
      for (const triple of generateElementTriples('knowledge', k.elementId, k.elementName, txId)) {
        triples.push(triple);
      }
    }
    for (const triple of generateRelationTriples(
      k.occupationCode,
      'knowledge',
      k.elementId,
      k.scaleId,
      k.dataValue,
      txId
    )) {
      triples.push(triple);
      const relationId = `${k.occupationCode}_knowledge_${k.elementId}_${k.scaleId}`;
      entityIds.add(`https://onet.org/relation/${encodeURIComponent(relationId)}`);
    }
  }

  console.log(`Generated ${triples.length} triples for ${entityIds.size} entities`);

  // Create bloom filter
  console.log('Building bloom filter...');
  const bloomFilter = createBloomFilter({
    capacity: entityIds.size,
    targetFpr: 0.01,
  });

  for (const id of entityIds) {
    addToFilter(bloomFilter, id);
  }

  // Upload chunks
  console.log('Uploading chunks to R2...');
  const chunks: LoaderIndex['chunks'] = [];

  for (let i = 0; i < triples.length; i += CHUNK_SIZE) {
    const chunkTriples = triples.slice(i, i + CHUNK_SIZE);
    const chunkData = encodeGraphCol(chunkTriples, ONET_NAMESPACE);
    const chunkNumber = Math.floor(i / CHUNK_SIZE);
    const chunkPath = `${R2_PREFIX}/chunks/chunk_${String(chunkNumber).padStart(4, '0')}.gcol`;

    await bucket.put(chunkPath, chunkData);
    console.log(`  Uploaded ${chunkPath} (${chunkData.length} bytes, ${chunkTriples.length} triples)`);

    chunks.push({
      path: chunkPath,
      tripleCount: chunkTriples.length,
      sizeBytes: chunkData.length,
    });
  }

  // Upload bloom filter
  const bloomPath = `${R2_PREFIX}/bloom/filter.json`;
  const serializedBloom = serializeFilter(bloomFilter);
  await bucket.put(bloomPath, JSON.stringify(serializedBloom, null, 2));
  console.log(`Uploaded bloom filter to ${bloomPath}`);

  // Create and upload index
  const index: LoaderIndex = {
    version: '29.1',
    source: ONET_DATA_URL,
    loadedAt: new Date().toISOString(),
    namespace: ONET_NAMESPACE,
    stats: {
      occupations: occupations.length,
      skills: uniqueSkills.size,
      abilities: uniqueAbilities.size,
      knowledge: uniqueKnowledge.size,
      totalTriples: triples.length,
      totalChunks: chunks.length,
    },
    chunks,
    bloom: {
      path: bloomPath,
      entityCount: entityIds.size,
    },
  };

  const indexPath = `${R2_PREFIX}/index.json`;
  await bucket.put(indexPath, JSON.stringify(index, null, 2));
  console.log(`Uploaded index to ${indexPath}`);

  const elapsed = Date.now() - startTime;
  console.log(`\nLoad complete in ${elapsed}ms`);
  console.log(`  ${occupations.length} occupations`);
  console.log(`  ${uniqueSkills.size} skills`);
  console.log(`  ${uniqueAbilities.size} abilities`);
  console.log(`  ${uniqueKnowledge.size} knowledge areas`);
  console.log(`  ${triples.length} total triples`);
  console.log(`  ${chunks.length} chunks`);

  return index;
}

// ============================================================================
// Explorer Helpers
// ============================================================================

/**
 * Convert triples to an Entity object for the explorer
 */
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

      let label: string | undefined;
      let type: string | undefined;
      let description: string | undefined;

      for (const triple of subjectTriples) {
        if ((triple.predicate === 'title' || triple.predicate === 'name') && triple.object.type === ObjectType.STRING) {
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
  return index.stats.occupations + index.stats.skills + index.stats.abilities + index.stats.knowledge;
}

// ============================================================================
// Worker Handler
// ============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const baseUrl = `${url.protocol}//${url.host}`;

    // Create explorer routes
    const explorer = createExplorerRoutes({
      namespace: 'onet',
      displayName: 'O*NET Graph Explorer',
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

    // Root endpoint - show available endpoints
    if (url.pathname === '/' || url.pathname === '') {
      return new Response(JSON.stringify({
        name: 'O*NET Data Loader',
        endpoints: {
          'POST /load': 'Trigger O*NET data load',
          'GET /status': 'Check load status',
          '/explore': 'Interactive graph explorer',
          '/entity/{id}': 'View entity by ID (URL-encoded)',
          '/search?q=term': 'Search entities',
          '/random': 'Redirect to random entity',
        },
      }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (url.pathname === '/status' && request.method === 'GET') {
      // Check if index exists
      const indexObj = await env.LAKEHOUSE.get(`${R2_PREFIX}/index.json`);
      if (!indexObj) {
        return new Response(JSON.stringify({ status: 'not_loaded' }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      const index = await indexObj.json<LoaderIndex>();
      return new Response(JSON.stringify({
        status: 'loaded',
        ...index,
      }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (url.pathname === '/load' && request.method === 'POST') {
      try {
        const index = await loadONetData(env.LAKEHOUSE);
        return new Response(JSON.stringify({
          success: true,
          index,
        }), {
          headers: { 'Content-Type': 'application/json' },
        });
      } catch (error) {
        console.error('Load failed:', error);
        return new Response(JSON.stringify({
          success: false,
          error: error instanceof Error ? error.message : String(error),
        }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }
    }

    return new Response('Not Found', { status: 404 });
  },
};

// Export types for testing
export type { LoaderIndex, ONetOccupation, ONetSkill, ONetOccupationSkill };
