/**
 * Wiktionary Data Loader Worker with Chunked Loading
 *
 * Streams pre-parsed Wiktionary dictionary data from kaikki.org to R2 as GraphCol chunks.
 * Uses Range requests for chunked downloading of the 2.6GB source file to avoid timeouts.
 * Progress is persisted in a Durable Object for resumable loading.
 *
 * Data Source: https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl
 * - Pre-parsed Wiktionary data in NDJSON format (one entry per line)
 * - ~1M English word entries with senses, etymologies, sounds
 *
 * Entity Model:
 * - WordEntry: $id=https://wiktionary.org/word/{word}/{pos}/{idx}
 *   - word (STRING): The word itself
 *   - partOfSpeech (STRING): noun, verb, adjective, etc.
 *   - senses (JSON): Array of sense definitions
 *   - sounds (JSON): Pronunciation data (IPA, audio files, etc.)
 *   - etymology (STRING): Word origin text
 *
 * Relations (if available in data):
 * - relatedTo: REF to related WordEntry
 * - derivedFrom: REF to source WordEntry
 *
 * R2 Output:
 * - datasets/wiktionary/chunks/chunk_{n}.gcol
 * - datasets/wiktionary/bloom/filter.json
 * - datasets/wiktionary/index.json
 *
 * Expected: ~1M entries, ~5M triples
 *
 * @packageDocumentation
 */

import type { Triple, TypedObject } from '../../src/core/triple';
import type { EntityId, Predicate, TransactionId, Namespace } from '../../src/core/types';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types';
import { encodeGraphCol, decodeGraphCol } from '../../src/storage/graphcol';
import {
  createBloomFilter,
  addToFilter,
  serializeFilter,
  deserializeFilter,
  type BloomFilter,
  type SerializedFilter,
} from '../../src/snippet/bloom';
import { createExplorerRoutes, type Entity, type SearchResult } from './lib/explorer';
import { createStreamingLineReader, type StreamingLineReader, type LineReaderState } from './lib/import-utils';

// ============================================================================
// Constants
// ============================================================================

/**
 * Kaikki.org pre-parsed Wiktionary JSON
 * NDJSON format: one JSON object per line
 * ~2.6GB, ~1M entries
 */
const WIKTIONARY_DATA_URL = 'https://kaikki.org/dictionary/English/kaikki.org-dictionary-English.jsonl';

const WIKTIONARY_NAMESPACE = 'https://wiktionary.org' as Namespace;
const CHUNK_SIZE = 50_000; // Triples per chunk
const R2_PREFIX = 'datasets/wiktionary';

// Chunked loading configuration - use 10MB chunks (safer for DO memory)
const DOWNLOAD_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB per request chunk (was 50MB)
const MAX_ENTRIES_PER_ITERATION = 50_000; // Process up to 50K entries per /load/continue call
const TOTAL_FILE_SIZE = 2_843_666_319; // Known file size from Content-Length

// ============================================================================
// Types
// ============================================================================

interface Env {
  LAKEHOUSE: R2Bucket;
  WIKTIONARY_LOADER: DurableObjectNamespace;
}

/**
 * Kaikki.org word entry structure
 * See: https://kaikki.org/dictionary/English/
 */
interface KaikkiWordEntry {
  word: string;
  pos: string; // Part of speech: noun, verb, adj, etc.
  senses?: Array<{
    glosses?: string[];
    tags?: string[];
    raw_glosses?: string[];
    examples?: Array<{
      text: string;
      ref?: string;
    }>;
    synonyms?: Array<{
      word: string;
      sense?: string;
    }>;
    antonyms?: Array<{
      word: string;
      sense?: string;
    }>;
    related?: Array<{
      word: string;
      sense?: string;
    }>;
    derived?: Array<{
      word: string;
      sense?: string;
    }>;
  }>;
  sounds?: Array<{
    ipa?: string;
    enpr?: string;
    audio?: string;
    text?: string;
    tags?: string[];
  }>;
  etymology_text?: string;
  etymology_templates?: unknown[];
  forms?: Array<{
    form: string;
    tags?: string[];
  }>;
  lang?: string;
  lang_code?: string;
  categories?: string[];
  translations?: unknown[];
  related?: Array<{
    word: string;
    sense?: string;
  }>;
  derived?: Array<{
    word: string;
    sense?: string;
  }>;
}

interface LoaderIndex {
  version: string;
  source: string;
  loadedAt: string;
  namespace: string;
  stats: {
    totalEntries: number;
    totalTriples: number;
    totalChunks: number;
    byPartOfSpeech: Record<string, number>;
  };
  chunks: Array<{
    path: string;
    tripleCount: number;
    sizeBytes: number;
  }>;
  bloom: {
    path: string;
    entityCount: number;
  };
}

/**
 * Persistent loading state stored in Durable Object
 */
interface LoaderState {
  status: 'idle' | 'loading' | 'completed' | 'error';
  startedAt?: string;
  completedAt?: string;
  error?: string;

  // Byte position tracking for Range requests
  byteOffset: number;
  totalBytes: number;

  // Entry processing tracking
  entriesProcessed: number;
  triplesGenerated: number;
  chunksUploaded: number;
  bytesUploaded: number;
  errors: number;

  // Part of speech statistics
  posCounts: Record<string, number>;

  // Word index tracking (for unique entity IDs)
  // Note: This is stored as a serialized JSON for large maps
  wordPosIndexPath?: string; // R2 path to serialized word index

  // Chunk info accumulator
  uploadedChunks: Array<{
    path: string;
    tripleCount: number;
    sizeBytes: number;
  }>;

  // Entity tracking for bloom filter
  entityCount: number;

  // Transaction ID for this load session
  txId?: string;

  // Buffer for incomplete line from previous chunk
  lineBuffer: string;
}

interface LoaderProgress {
  status: LoaderState['status'];
  byteOffset: number;
  totalBytes: number;
  percentComplete: number;
  entriesProcessed: number;
  triplesGenerated: number;
  chunksUploaded: number;
  bytesUploaded: number;
  errors: number;
  posCounts: Record<string, number>;
  startedAt?: string;
  completedAt?: string;
  error?: string;
  estimatedTimeRemaining?: string;
}

// ============================================================================
// ULID Generator
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
// Helper Functions
// ============================================================================

/**
 * Sanitize a word for URL encoding
 * Handles special characters that might break URL parsing
 */
function sanitizeWord(word: string): string {
  return encodeURIComponent(word.toLowerCase().replace(/\s+/g, '_'));
}

/**
 * Create a unique entity ID for a word entry
 * Format: https://wiktionary.org/word/{word}/{pos}/{idx}
 */
function createWordEntityId(word: string, pos: string, idx: number): string {
  const sanitized = sanitizeWord(word);
  const sanitizedPos = sanitizeWord(pos);
  return `https://wiktionary.org/word/${sanitized}/${sanitizedPos}/${idx}`;
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

  let object: TypedObject;

  switch (objectType) {
    case ObjectType.STRING:
      object = { type: ObjectType.STRING, value: String(value) };
      break;
    case ObjectType.JSON:
      object = { type: ObjectType.JSON, value };
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
 * Generate triples for a Wiktionary word entry
 */
function* generateWordEntryTriples(
  entry: KaikkiWordEntry,
  idx: number,
  txId: TransactionId
): Generator<Triple> {
  if (!entry.word || !entry.pos) {
    return;
  }

  const entityUrl = createWordEntityId(entry.word, entry.pos, idx);

  // $type predicate
  yield makeTriple(entityUrl, '$type', 'WordEntry', ObjectType.STRING, txId);

  // word
  yield makeTriple(entityUrl, 'word', entry.word, ObjectType.STRING, txId);

  // partOfSpeech
  yield makeTriple(entityUrl, 'partOfSpeech', entry.pos, ObjectType.STRING, txId);

  // senses (as JSON array)
  if (entry.senses && entry.senses.length > 0) {
    // Simplify senses for storage - just glosses and tags
    const simplifiedSenses = entry.senses.map((sense) => ({
      glosses: sense.glosses ?? sense.raw_glosses ?? [],
      tags: sense.tags ?? [],
    }));
    yield makeTriple(entityUrl, 'senses', simplifiedSenses, ObjectType.JSON, txId);
  }

  // sounds (as JSON array) - IPA pronunciations
  if (entry.sounds && entry.sounds.length > 0) {
    const simplifiedSounds = entry.sounds.map((sound) => ({
      ipa: sound.ipa,
      enpr: sound.enpr,
      audio: sound.audio,
    })).filter((s) => s.ipa || s.enpr || s.audio);

    if (simplifiedSounds.length > 0) {
      yield makeTriple(entityUrl, 'sounds', simplifiedSounds, ObjectType.JSON, txId);
    }
  }

  // etymology
  if (entry.etymology_text) {
    yield makeTriple(entityUrl, 'etymology', entry.etymology_text, ObjectType.STRING, txId);
  }

  // forms (inflections)
  if (entry.forms && entry.forms.length > 0) {
    const simplifiedForms = entry.forms.map((f) => ({
      form: f.form,
      tags: f.tags ?? [],
    }));
    yield makeTriple(entityUrl, 'forms', simplifiedForms, ObjectType.JSON, txId);
  }

  // categories
  if (entry.categories && entry.categories.length > 0) {
    yield makeTriple(entityUrl, 'categories', entry.categories, ObjectType.JSON, txId);
  }

  // Related words (from senses or top-level)
  const relatedWords = new Set<string>();
  const derivedWords = new Set<string>();

  // Collect from senses
  if (entry.senses) {
    for (const sense of entry.senses) {
      if (sense.related) {
        for (const rel of sense.related) {
          if (rel.word) relatedWords.add(rel.word);
        }
      }
      if (sense.derived) {
        for (const der of sense.derived) {
          if (der.word) derivedWords.add(der.word);
        }
      }
      if (sense.synonyms) {
        for (const syn of sense.synonyms) {
          if (syn.word) relatedWords.add(syn.word);
        }
      }
    }
  }

  // Collect from top-level
  if (entry.related) {
    for (const rel of entry.related) {
      if (rel.word) relatedWords.add(rel.word);
    }
  }
  if (entry.derived) {
    for (const der of entry.derived) {
      if (der.word) derivedWords.add(der.word);
    }
  }

  // Store related words as JSON arrays (we can't create REF without knowing target entity IDs)
  if (relatedWords.size > 0) {
    yield makeTriple(entityUrl, 'relatedWords', Array.from(relatedWords), ObjectType.JSON, txId);
  }

  if (derivedWords.size > 0) {
    yield makeTriple(entityUrl, 'derivedWords', Array.from(derivedWords), ObjectType.JSON, txId);
  }
}

// ============================================================================
// Durable Object: WiktionaryLoaderDO
// ============================================================================

export class WiktionaryLoaderDO implements DurableObject {
  private state: DurableObjectState;
  private env: Env;
  private loaderState: LoaderState;
  private wordPosIndex: Map<string, number>;
  private triples: Triple[] = [];
  private bloomFilter: BloomFilter;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
    this.wordPosIndex = new Map();
    this.bloomFilter = createBloomFilter({
      capacity: 2_000_000,
      targetFpr: 0.01,
    });

    // Initialize default loader state
    this.loaderState = {
      status: 'idle',
      byteOffset: 0,
      totalBytes: TOTAL_FILE_SIZE,
      entriesProcessed: 0,
      triplesGenerated: 0,
      chunksUploaded: 0,
      bytesUploaded: 0,
      errors: 0,
      posCounts: {},
      uploadedChunks: [],
      entityCount: 0,
      lineBuffer: '',
    };

    // Load state from storage on initialization
    this.state.blockConcurrencyWhile(async () => {
      const stored = await this.state.storage.get<LoaderState>('loaderState');
      if (stored) {
        this.loaderState = stored;
      }

      // Load word index if path exists
      if (this.loaderState.wordPosIndexPath) {
        try {
          const indexObj = await this.env.LAKEHOUSE.get(this.loaderState.wordPosIndexPath);
          if (indexObj) {
            const indexData = await indexObj.json<Record<string, number>>();
            this.wordPosIndex = new Map(Object.entries(indexData));
          }
        } catch (e) {
          console.warn('[DO] Failed to load word index from R2:', e);
        }
      }

      // Load bloom filter if we have chunks
      if (this.loaderState.chunksUploaded > 0) {
        try {
          const bloomObj = await this.env.LAKEHOUSE.get(`${R2_PREFIX}/bloom/progress.json`);
          if (bloomObj) {
            const serialized = await bloomObj.json<SerializedFilter>();
            this.bloomFilter = deserializeFilter(serialized);
          }
        } catch (e) {
          console.warn('[DO] Failed to load bloom filter from R2:', e);
        }
      }
    });
  }

  private async saveState(): Promise<void> {
    await this.state.storage.put('loaderState', this.loaderState);
  }

  private async saveWordIndex(): Promise<void> {
    // Save word index to R2 (it can get large)
    const indexPath = `${R2_PREFIX}/progress/word-index.json`;
    const indexData = Object.fromEntries(this.wordPosIndex);
    await this.env.LAKEHOUSE.put(indexPath, JSON.stringify(indexData));
    this.loaderState.wordPosIndexPath = indexPath;
  }

  private async saveBloomFilter(): Promise<void> {
    const serialized = serializeFilter(this.bloomFilter);
    await this.env.LAKEHOUSE.put(`${R2_PREFIX}/bloom/progress.json`, JSON.stringify(serialized));
  }

  private getNextWordIndex(word: string, pos: string): number {
    const key = `${word.toLowerCase()}|${pos.toLowerCase()}`;
    const current = this.wordPosIndex.get(key) ?? 0;
    this.wordPosIndex.set(key, current + 1);
    return current;
  }

  private async flushTriples(): Promise<void> {
    if (this.triples.length === 0) return;

    const chunkId = `chunk_${this.loaderState.chunksUploaded.toString().padStart(6, '0')}`;
    const chunkPath = `${R2_PREFIX}/chunks/${chunkId}.gcol`;

    // Encode triples to GraphCol format
    const encoded = encodeGraphCol(this.triples, WIKTIONARY_NAMESPACE);

    // Upload chunk
    await this.env.LAKEHOUSE.put(chunkPath, encoded, {
      customMetadata: {
        tripleCount: this.triples.length.toString(),
        createdAt: new Date().toISOString(),
      },
    });

    // Track chunk info
    this.loaderState.uploadedChunks.push({
      path: chunkPath,
      tripleCount: this.triples.length,
      sizeBytes: encoded.length,
    });

    this.loaderState.bytesUploaded += encoded.length;
    this.loaderState.chunksUploaded++;

    console.log(
      `[DO] Uploaded ${chunkId}: ${this.triples.length} triples, ${(encoded.length / 1024).toFixed(1)}KB`
    );

    // Clear buffer
    this.triples = [];
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // GET /status - Return current progress
    if (url.pathname === '/status' && request.method === 'GET') {
      return this.handleStatus();
    }

    // POST /start - Start or restart loading
    if (url.pathname === '/start' && request.method === 'POST') {
      return this.handleStart();
    }

    // POST /continue - Continue loading from current position
    if (url.pathname === '/continue' && request.method === 'POST') {
      return this.handleContinue();
    }

    // POST /reset - Reset loading state
    if (url.pathname === '/reset' && request.method === 'POST') {
      return this.handleReset();
    }

    // POST /finalize - Finalize loading and create index
    if (url.pathname === '/finalize' && request.method === 'POST') {
      return this.handleFinalize();
    }

    return new Response('Not Found', { status: 404 });
  }

  private handleStatus(): Response {
    const progress: LoaderProgress = {
      status: this.loaderState.status,
      byteOffset: this.loaderState.byteOffset,
      totalBytes: this.loaderState.totalBytes,
      percentComplete: (this.loaderState.byteOffset / this.loaderState.totalBytes) * 100,
      entriesProcessed: this.loaderState.entriesProcessed,
      triplesGenerated: this.loaderState.triplesGenerated,
      chunksUploaded: this.loaderState.chunksUploaded,
      bytesUploaded: this.loaderState.bytesUploaded,
      errors: this.loaderState.errors,
      posCounts: this.loaderState.posCounts,
      startedAt: this.loaderState.startedAt,
      completedAt: this.loaderState.completedAt,
      error: this.loaderState.error,
    };

    // Estimate time remaining based on processing rate
    if (this.loaderState.status === 'loading' && this.loaderState.startedAt) {
      const elapsed = Date.now() - new Date(this.loaderState.startedAt).getTime();
      const bytesRemaining = this.loaderState.totalBytes - this.loaderState.byteOffset;
      const bytesPerMs = this.loaderState.byteOffset / elapsed;
      if (bytesPerMs > 0) {
        const msRemaining = bytesRemaining / bytesPerMs;
        const secondsRemaining = Math.round(msRemaining / 1000);
        if (secondsRemaining < 60) {
          progress.estimatedTimeRemaining = `${secondsRemaining}s`;
        } else if (secondsRemaining < 3600) {
          progress.estimatedTimeRemaining = `${Math.round(secondsRemaining / 60)}m`;
        } else {
          progress.estimatedTimeRemaining = `${(secondsRemaining / 3600).toFixed(1)}h`;
        }
      }
    }

    return new Response(JSON.stringify(progress, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleStart(): Promise<Response> {
    // Reset state for fresh start
    this.loaderState = {
      status: 'loading',
      startedAt: new Date().toISOString(),
      byteOffset: 0,
      totalBytes: TOTAL_FILE_SIZE,
      entriesProcessed: 0,
      triplesGenerated: 0,
      chunksUploaded: 0,
      bytesUploaded: 0,
      errors: 0,
      posCounts: {},
      uploadedChunks: [],
      entityCount: 0,
      txId: generateULID(),
      lineBuffer: '',
    };

    this.wordPosIndex = new Map();
    this.triples = [];
    this.bloomFilter = createBloomFilter({
      capacity: 2_000_000,
      targetFpr: 0.01,
    });

    await this.saveState();

    console.log(`[DO] Started new load session with txId: ${this.loaderState.txId}`);

    return new Response(JSON.stringify({
      message: 'Loading started',
      txId: this.loaderState.txId,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleContinue(): Promise<Response> {
    if (this.loaderState.status === 'completed') {
      return new Response(JSON.stringify({
        message: 'Loading already completed',
        status: this.loaderState.status,
      }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (this.loaderState.status === 'idle') {
      return new Response(JSON.stringify({
        message: 'Loading not started. Call /start first.',
        status: this.loaderState.status,
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    if (this.loaderState.status === 'error') {
      // Allow resuming from error state
      this.loaderState.status = 'loading';
      this.loaderState.error = undefined;
    }

    const txId = this.loaderState.txId as TransactionId;
    const startByteOffset = this.loaderState.byteOffset;
    const entriesAtStart = this.loaderState.entriesProcessed;

    try {
      // Calculate range for this chunk
      const rangeStart = this.loaderState.byteOffset;
      const rangeEnd = Math.min(rangeStart + DOWNLOAD_CHUNK_SIZE - 1, this.loaderState.totalBytes - 1);

      console.log(`[DO] Fetching bytes ${rangeStart}-${rangeEnd} (${((rangeEnd - rangeStart + 1) / 1024 / 1024).toFixed(1)}MB)`);

      // Fetch chunk with Range header
      const response = await fetch(WIKTIONARY_DATA_URL, {
        headers: {
          Range: `bytes=${rangeStart}-${rangeEnd}`,
        },
      });

      if (!response.ok && response.status !== 206) {
        throw new Error(`Fetch failed: ${response.status} ${response.statusText}`);
      }

      const data = new Uint8Array(await response.arrayBuffer());

      // Use streaming line reader instead of split('\n') to avoid memory issues
      const lineReader = createStreamingLineReader();
      // Restore partial line state from previous chunk
      lineReader.restoreState({
        bytesProcessed: 0,
        linesEmitted: 0,
        partialLine: this.loaderState.lineBuffer,
      });

      let entriesThisIteration = 0;
      let linesCollected: string[] = [];

      // Process chunk with streaming line reader - yields complete lines only
      for await (const line of lineReader.processChunk(data)) {
        linesCollected.push(line);
      }

      // Save partial line for next iteration (if not at end of file)
      const lineState = lineReader.getState();
      if (rangeEnd < this.loaderState.totalBytes - 1) {
        this.loaderState.lineBuffer = lineState.partialLine;
      } else {
        // At end of file, flush any remaining content
        const remaining = lineReader.flush();
        if (remaining) {
          linesCollected.push(remaining);
        }
        this.loaderState.lineBuffer = '';
      }

      // Process complete lines
      for (const trimmed of linesCollected) {
        // Limit entries per iteration to avoid timeout
        if (entriesThisIteration >= MAX_ENTRIES_PER_ITERATION) {
          // Save remaining lines back to buffer for next iteration
          const remainingIdx = linesCollected.indexOf(trimmed);
          const remaining = linesCollected.slice(remainingIdx);
          this.loaderState.lineBuffer = remaining.join('\n') + (this.loaderState.lineBuffer ? '\n' + this.loaderState.lineBuffer : '');
          break;
        }

        try {
          const entry = JSON.parse(trimmed) as KaikkiWordEntry;

          // Only process English entries
          if (entry.lang_code !== 'en' && entry.lang_code !== undefined) {
            continue;
          }

          // Get unique index for this word/pos combination
          const idx = this.getNextWordIndex(entry.word, entry.pos);

          // Generate triples for this entry
          for (const triple of generateWordEntryTriples(entry, idx, txId)) {
            this.triples.push(triple);
            addToFilter(this.bloomFilter, triple.subject);
            this.loaderState.triplesGenerated++;
          }

          // Track entity
          this.loaderState.entityCount++;

          // Track POS statistics
          const pos = entry.pos?.toLowerCase() ?? 'unknown';
          this.loaderState.posCounts[pos] = (this.loaderState.posCounts[pos] ?? 0) + 1;

          this.loaderState.entriesProcessed++;
          entriesThisIteration++;

          // Flush triples to R2 if we have enough
          if (this.triples.length >= CHUNK_SIZE) {
            await this.flushTriples();
          }
        } catch (e) {
          // Skip malformed JSON lines
          this.loaderState.errors++;
        }
      }

      // Update byte offset
      // If we processed all lines, move to end of fetched range
      // If we hit entry limit, we'll re-process from current buffer next time
      if (entriesThisIteration < MAX_ENTRIES_PER_ITERATION) {
        this.loaderState.byteOffset = rangeEnd + 1;
      }

      // Check if we're done
      const isComplete = this.loaderState.byteOffset >= this.loaderState.totalBytes &&
                         this.loaderState.lineBuffer.length === 0;

      if (isComplete) {
        this.loaderState.status = 'completed';
        this.loaderState.completedAt = new Date().toISOString();

        // Flush any remaining triples
        await this.flushTriples();
      }

      // Save progress
      await this.saveWordIndex();
      await this.saveBloomFilter();
      await this.saveState();

      const progress: LoaderProgress = {
        status: this.loaderState.status,
        byteOffset: this.loaderState.byteOffset,
        totalBytes: this.loaderState.totalBytes,
        percentComplete: (this.loaderState.byteOffset / this.loaderState.totalBytes) * 100,
        entriesProcessed: this.loaderState.entriesProcessed,
        triplesGenerated: this.loaderState.triplesGenerated,
        chunksUploaded: this.loaderState.chunksUploaded,
        bytesUploaded: this.loaderState.bytesUploaded,
        errors: this.loaderState.errors,
        posCounts: this.loaderState.posCounts,
        startedAt: this.loaderState.startedAt,
        completedAt: this.loaderState.completedAt,
      };

      console.log(
        `[DO] Processed ${entriesThisIteration} entries (total: ${this.loaderState.entriesProcessed}), ` +
        `${((this.loaderState.byteOffset / this.loaderState.totalBytes) * 100).toFixed(1)}% complete`
      );

      return new Response(JSON.stringify({
        message: isComplete ? 'Loading completed' : 'Chunk processed',
        entriesThisIteration,
        bytesProcessed: rangeEnd - rangeStart + 1,
        progress,
        needsContinue: !isComplete,
      }, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });

    } catch (error) {
      this.loaderState.status = 'error';
      this.loaderState.error = error instanceof Error ? error.message : String(error);
      await this.saveState();

      console.error('[DO] Error during loading:', error);

      return new Response(JSON.stringify({
        message: 'Error during loading',
        error: this.loaderState.error,
        progress: {
          status: this.loaderState.status,
          byteOffset: this.loaderState.byteOffset,
          entriesProcessed: this.loaderState.entriesProcessed,
        },
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  private async handleReset(): Promise<Response> {
    // Reset all state
    this.loaderState = {
      status: 'idle',
      byteOffset: 0,
      totalBytes: TOTAL_FILE_SIZE,
      entriesProcessed: 0,
      triplesGenerated: 0,
      chunksUploaded: 0,
      bytesUploaded: 0,
      errors: 0,
      posCounts: {},
      uploadedChunks: [],
      entityCount: 0,
      lineBuffer: '',
    };

    this.wordPosIndex = new Map();
    this.triples = [];
    this.bloomFilter = createBloomFilter({
      capacity: 2_000_000,
      targetFpr: 0.01,
    });

    await this.saveState();

    console.log('[DO] State reset');

    return new Response(JSON.stringify({
      message: 'Loading state reset',
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleFinalize(): Promise<Response> {
    if (this.loaderState.status !== 'completed') {
      return new Response(JSON.stringify({
        message: 'Cannot finalize - loading not complete',
        status: this.loaderState.status,
        percentComplete: (this.loaderState.byteOffset / this.loaderState.totalBytes) * 100,
      }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    try {
      // Upload final bloom filter
      const bloomPath = `${R2_PREFIX}/bloom/filter.json`;
      const serializedBloom = serializeFilter(this.bloomFilter);
      await this.env.LAKEHOUSE.put(bloomPath, JSON.stringify(serializedBloom, null, 2));

      // Create and upload index
      const index: LoaderIndex = {
        version: '1.0',
        source: WIKTIONARY_DATA_URL,
        loadedAt: this.loaderState.completedAt ?? new Date().toISOString(),
        namespace: WIKTIONARY_NAMESPACE,
        stats: {
          totalEntries: this.loaderState.entriesProcessed,
          totalTriples: this.loaderState.triplesGenerated,
          totalChunks: this.loaderState.chunksUploaded,
          byPartOfSpeech: this.loaderState.posCounts,
        },
        chunks: this.loaderState.uploadedChunks,
        bloom: {
          path: bloomPath,
          entityCount: this.loaderState.entityCount,
        },
      };

      const indexPath = `${R2_PREFIX}/index.json`;
      await this.env.LAKEHOUSE.put(indexPath, JSON.stringify(index, null, 2));

      console.log(`[DO] Finalized loading: ${index.stats.totalEntries} entries, ${index.stats.totalTriples} triples`);

      return new Response(JSON.stringify({
        message: 'Loading finalized',
        index,
      }, null, 2), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      return new Response(JSON.stringify({
        message: 'Error finalizing',
        error: error instanceof Error ? error.message : String(error),
      }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }
}

// ============================================================================
// Explorer Helpers
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
      case ObjectType.JSON:
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
      let partOfSpeech: string | undefined;

      for (const triple of subjectTriples) {
        if (triple.predicate === 'word' && triple.object.type === ObjectType.STRING) {
          label = triple.object.value;
        }
        if (triple.predicate === '$type' && triple.object.type === ObjectType.STRING) {
          type = triple.object.value;
        }
        if (triple.predicate === 'partOfSpeech' && triple.object.type === ObjectType.STRING) {
          partOfSpeech = triple.object.value;
        }
      }

      if (label && label.toLowerCase().includes(lowerQuery)) {
        results.push({
          $id: subject,
          $type: type,
          label,
          description: partOfSpeech ? `(${partOfSpeech})` : undefined,
        });
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
  return index.stats.totalEntries;
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
      namespace: 'wiktionary',
      displayName: 'Wiktionary Graph Explorer',
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
      return new Response(
        JSON.stringify(
          {
            name: 'Wiktionary Data Loader',
            description: 'Streams pre-parsed Wiktionary dictionary data to R2 as GraphCol chunks',
            source: WIKTIONARY_DATA_URL,
            sourceSize: `${(TOTAL_FILE_SIZE / 1024 / 1024 / 1024).toFixed(2)}GB`,
            endpoints: {
              'GET /': 'Show this help',
              'GET /status': 'Check load status and progress',
              'POST /load/start': 'Start a new loading session',
              'POST /load/continue': 'Continue loading from current position',
              'POST /load/reset': 'Reset loading state',
              'POST /load/finalize': 'Finalize completed load and create index',
              'POST /load/auto': 'Auto-load with continuation (runs multiple iterations)',
              '/explore': 'Interactive graph explorer',
              '/entity/{id}': 'View entity by ID (URL-encoded)',
              '/search?q=term': 'Search entities',
              '/random': 'Redirect to random entity',
            },
            loadingStrategy: {
              description: 'Chunked loading with Range requests to handle 2.6GB source',
              chunkSize: `${DOWNLOAD_CHUNK_SIZE / 1024 / 1024}MB per request`,
              entriesPerIteration: MAX_ENTRIES_PER_ITERATION,
              stateManagement: 'Durable Object with R2 persistence',
            },
            entityModel: {
              WordEntry: {
                $id: 'https://wiktionary.org/word/{word}/{pos}/{idx}',
                predicates: {
                  word: 'STRING - The word itself',
                  partOfSpeech: 'STRING - noun, verb, adj, etc.',
                  senses: 'JSON - Array of definitions and tags',
                  sounds: 'JSON - IPA pronunciations',
                  etymology: 'STRING - Word origin',
                  forms: 'JSON - Inflections',
                  categories: 'JSON - Wiktionary categories',
                  relatedWords: 'JSON - Related word strings',
                  derivedWords: 'JSON - Derived word strings',
                },
              },
            },
            expected: {
              entries: '~1M word entries',
              triples: '~5M triples',
              size: '~2.6GB source, ~100MB compressed output',
            },
          },
          null,
          2
        ),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Status endpoint - proxied to DO
    if (url.pathname === '/status' && request.method === 'GET') {
      // First check if we have a completed index
      try {
        const indexObj = await env.LAKEHOUSE.get(`${R2_PREFIX}/index.json`);
        if (indexObj) {
          const index = await indexObj.json<LoaderIndex>();

          // Also get DO status for any in-progress loading
          const doId = env.WIKTIONARY_LOADER.idFromName('main');
          const stub = env.WIKTIONARY_LOADER.get(doId);
          const doResponse = await stub.fetch(new Request('http://internal/status'));
          const doStatus = await doResponse.json() as LoaderProgress;

          return new Response(
            JSON.stringify(
              {
                indexStatus: 'loaded',
                index,
                loadingProgress: doStatus,
              },
              null,
              2
            ),
            {
              headers: { 'Content-Type': 'application/json' },
            }
          );
        }
      } catch (e) {
        // Index doesn't exist, fall through to DO status
      }

      // Get DO status
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);
      return stub.fetch(new Request('http://internal/status'));
    }

    // Load control endpoints - proxied to DO
    if (url.pathname === '/load/start' && request.method === 'POST') {
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);
      return stub.fetch(new Request('http://internal/start', { method: 'POST' }));
    }

    if (url.pathname === '/load/continue' && request.method === 'POST') {
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);
      return stub.fetch(new Request('http://internal/continue', { method: 'POST' }));
    }

    if (url.pathname === '/load/reset' && request.method === 'POST') {
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);
      return stub.fetch(new Request('http://internal/reset', { method: 'POST' }));
    }

    if (url.pathname === '/load/finalize' && request.method === 'POST') {
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);
      return stub.fetch(new Request('http://internal/finalize', { method: 'POST' }));
    }

    // Auto-load endpoint - runs multiple continue iterations until done or timeout
    if (url.pathname === '/load/auto' && request.method === 'POST') {
      const doId = env.WIKTIONARY_LOADER.idFromName('main');
      const stub = env.WIKTIONARY_LOADER.get(doId);

      // Check current status
      const statusResponse = await stub.fetch(new Request('http://internal/status'));
      const status = await statusResponse.json() as LoaderProgress;

      // If idle, start first
      if (status.status === 'idle') {
        await stub.fetch(new Request('http://internal/start', { method: 'POST' }));
      }

      // Run continue iterations until done or we've used ~4 minutes (leaving buffer for response)
      const startTime = Date.now();
      const maxDuration = 4 * 60 * 1000; // 4 minutes
      let iterations = 0;
      let lastResult: unknown = null;

      while (Date.now() - startTime < maxDuration) {
        const continueResponse = await stub.fetch(new Request('http://internal/continue', { method: 'POST' }));
        lastResult = await continueResponse.json();
        iterations++;

        // Check if done
        if (!(lastResult as Record<string, unknown>).needsContinue) {
          break;
        }
      }

      // Get final status
      const finalStatusResponse = await stub.fetch(new Request('http://internal/status'));
      const finalStatus = await finalStatusResponse.json() as LoaderProgress;

      return new Response(
        JSON.stringify(
          {
            message: finalStatus.status === 'completed' ? 'Loading completed' : 'Auto-load paused (timeout)',
            iterations,
            duration: `${((Date.now() - startTime) / 1000).toFixed(1)}s`,
            progress: finalStatus,
            needsMoreIterations: finalStatus.status === 'loading',
          },
          null,
          2
        ),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Legacy /load endpoint - redirect to /load/auto
    if (url.pathname === '/load' && request.method === 'POST') {
      return Response.redirect(`${baseUrl}/load/auto`, 307);
    }

    return new Response('Not Found', { status: 404 });
  },
};

// Export types for testing
export type { LoaderIndex, KaikkiWordEntry, LoaderProgress, LoaderState };
