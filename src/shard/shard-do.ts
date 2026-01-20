/**
 * ShardDO - Triple Storage Durable Object with Hibernation Support
 *
 * SQLite-backed Durable Object for storing graph triples with typed object columns.
 * Supports multiple access patterns via indexes (SPO, POS, OSP, timestamp, tx).
 *
 * Current functionality:
 * - Schema initialization and migration
 * - Triple CRUD operations
 * - Counter/echo endpoint for subrequest quota testing
 * - Request tracking per message ID
 * - Health and stats endpoints
 * - WebSocket hibernation for cost-effective long-lived connections
 * - Alarm-based maintenance scheduling
 * - Pending operation queue with hibernation persistence
 *
 * @see schema.ts for SQLite schema definition
 * @see crud.ts for TripleStore implementation
 */

import type { Env } from '../core/index.js';
import { initializeSchema, getCurrentVersion, SCHEMA_VERSION } from './schema.js';
import type { TripleStore } from './crud.js';
import { createTripleStore } from './crud.js';
import type { ChunkStore } from './chunk-store.js';
import { createChunkStore } from './chunk-store.js';
import type { Triple } from '../core/triple.js';
import type { EntityId, Namespace } from '../core/types.js';
import {
  createNamespace,
  ObjectType,
  assertEntityId,
  assertPredicate,
  assertTransactionId,
  assertEntityIdArray,
  BrandedTypeValidationError,
} from '../core/types.js';
import { resolveNamespace, type Entity } from '../core/entity.js';
import { jsonToTypedObject, typedObjectToJson, type JsonTypedObjectValue } from '../core/type-converters.js';
import { errorResponse, ErrorCode } from '../errors/api-error.js';
import { SQLiteIndexStore } from '../index/sqlite-index-store.js';

/**
 * Attachment data stored with hibernated WebSocket connections for ShardDO
 */
export interface ShardWebSocketAttachment {
  clientId: string;
  connectedAt: number;
  totalMessagesReceived: number;
  pendingOperations: number;
  lastActivityTimestamp: number;
}

/**
 * Pending operation for hibernation persistence
 */
export interface PendingOperation {
  operationId: string;
  type: string;
  data: unknown;
  status: 'pending' | 'completed' | 'failed';
  createdAt: number;
  completedAt?: number;
  result?: unknown;
  error?: string;
}

/**
 * Maintenance task for alarm-based processing
 */
export interface MaintenanceTask {
  task: string;
  priority: 'high' | 'medium' | 'low';
  scheduledAt: number;
  completedAt?: number;
}

/**
 * Statistics tracked by ShardDO
 */
export interface ShardStats {
  totalRequests: number;
  requestsPerMessage: Map<string, number>;
  lastRequestTimestamp: number;
  startupTimestamp: number;
  // Hibernation metrics
  totalWakes: number;
  alarmCount: number;
  lastAlarmTimestamp: number;
  staleConnectionsCleanedUp: number;
  activeConnections: number;
}

export class ShardDO implements DurableObject {
  private readonly ctx: DurableObjectState;
  // @ts-expect-error env is stored for future use
  private readonly env: Env;

  // In-memory stats
  private totalRequests: number = 0;
  private requestsPerMessage: Map<string, number> = new Map();
  private lastRequestTimestamp: number = 0;
  private readonly startupTimestamp: number;

  // Schema initialization flag
  private schemaInitialized: boolean = false;

  // Triple store for CRUD operations
  private tripleStore: TripleStore | null = null;

  // Chunk store for 2MB BLOB storage optimization
  private chunkStore: ChunkStore | null = null;

  // Index store for secondary indexes (POS, OSP, FTS, Geo)
  private indexStore: SQLiteIndexStore | null = null;

  // Default namespace (can be overridden per shard)
  private namespace: Namespace = createNamespace('https://graphdb.example.com/');

  // Hibernation tracking
  private totalWakes: number = 0;
  private alarmCount: number = 0;
  private lastAlarmTimestamp: number = 0;
  private staleConnectionsCleanedUp: number = 0;

  // State preservation across hibernation
  private stateValue: number = 0;

  // Pending operations for hibernation persistence
  private pendingOperations: Map<string, PendingOperation> = new Map();

  // Maintenance tasks
  private maintenanceTasks: MaintenanceTask[] = [];
  private completedMaintenanceTasks: number = 0;

  // Connection timeout configuration (default 5 minutes)
  private connectionTimeoutMs: number = 5 * 60 * 1000;

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
    this.startupTimestamp = Date.now();

    // Initialize schema and restore state from storage
    ctx.blockConcurrencyWhile(async () => {
      // Initialize SQLite schema on first request
      this.initializeSchemaIfNeeded();

      const stored = await ctx.storage.get<number>('totalRequests');
      if (stored !== undefined) {
        this.totalRequests = stored;
      }

      // Restore hibernation metrics
      const storedWakes = await ctx.storage.get<number>('totalWakes');
      if (storedWakes !== undefined) {
        this.totalWakes = storedWakes;
      }

      const storedAlarmCount = await ctx.storage.get<number>('alarmCount');
      if (storedAlarmCount !== undefined) {
        this.alarmCount = storedAlarmCount;
      }

      const storedStateValue = await ctx.storage.get<number>('stateValue');
      if (storedStateValue !== undefined) {
        this.stateValue = storedStateValue;
      }

      // Restore pending operations
      const storedOps = await ctx.storage.get<PendingOperation[]>('pendingOperations');
      if (storedOps) {
        for (const op of storedOps) {
          this.pendingOperations.set(op.operationId, op);
        }
      }

      // Restore maintenance tasks
      const storedTasks = await ctx.storage.get<MaintenanceTask[]>('maintenanceTasks');
      if (storedTasks) {
        this.maintenanceTasks = storedTasks;
      }

      const storedCompletedTasks = await ctx.storage.get<number>('completedMaintenanceTasks');
      if (storedCompletedTasks !== undefined) {
        this.completedMaintenanceTasks = storedCompletedTasks;
      }

      const storedStaleCleanedUp = await ctx.storage.get<number>('staleConnectionsCleanedUp');
      if (storedStaleCleanedUp !== undefined) {
        this.staleConnectionsCleanedUp = storedStaleCleanedUp;
      }
    });
  }

  /**
   * Initialize the SQLite schema if not already done
   */
  private initializeSchemaIfNeeded(): void {
    if (this.schemaInitialized) {
      return;
    }

    const sql = this.ctx.storage.sql;
    initializeSchema(sql);
    this.tripleStore = createTripleStore(sql);
    this.chunkStore = createChunkStore(sql, this.namespace);
    this.indexStore = new SQLiteIndexStore(sql);
    this.schemaInitialized = true;
  }

  /**
   * Get the triple store, initializing schema if needed
   */
  private getTripleStore(): TripleStore {
    if (!this.tripleStore) {
      this.initializeSchemaIfNeeded();
    }
    return this.tripleStore!;
  }

  /**
   * Get the chunk store, initializing schema if needed
   */
  private getChunkStore(): ChunkStore {
    if (!this.chunkStore) {
      this.initializeSchemaIfNeeded();
    }
    return this.chunkStore!;
  }

  /**
   * Get the index store, initializing schema if needed
   */
  private getIndexStore(): SQLiteIndexStore {
    if (!this.indexStore) {
      this.initializeSchemaIfNeeded();
    }
    return this.indexStore!;
  }

  /**
   * Handle incoming requests
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;

    // WebSocket upgrade for hibernation support
    if (request.headers.get('Upgrade') === 'websocket' || url.pathname === '/ws') {
      if (request.headers.get('Upgrade') === 'websocket') {
        return this.handleWebSocketUpgrade(request);
      }
      return new Response('WebSocket upgrade required', { status: 426 });
    }

    // Connection count endpoint
    if (url.pathname === '/connections/count' && method === 'GET') {
      return this.handleConnectionCount();
    }

    // Maintenance endpoints
    if (url.pathname === '/maintenance/schedule' && method === 'POST') {
      return this.handleScheduleMaintenance(request);
    }

    if (url.pathname === '/maintenance/status' && method === 'GET') {
      return this.handleMaintenanceStatus();
    }

    // Operations queue endpoints
    if (url.pathname === '/operations/queue' && method === 'POST') {
      return this.handleQueueOperations(request);
    }

    if (url.pathname === '/operations/pending' && method === 'GET') {
      return this.handleGetPendingOperations();
    }

    // Configuration endpoint
    if (url.pathname === '/config' && method === 'POST') {
      return this.handleConfig(request);
    }

    // Orchestrator query endpoints (lookup, traverse, filter)
    if (url.pathname === '/lookup' && method === 'GET') {
      return this.handleLookup(url);
    }

    if (url.pathname === '/traverse' && method === 'GET') {
      return this.handleTraverse(url);
    }

    if (url.pathname === '/filter' && method === 'GET') {
      return this.handleFilter(url);
    }

    // Triple CRUD endpoints
    if (url.pathname === '/triples' && method === 'POST') {
      return this.handleInsertTriples(request);
    }

    // Match /triples/:subject/:predicate
    const triplePredicateMatch = url.pathname.match(/^\/triples\/([^/]+)\/([^/]+)$/);
    if (triplePredicateMatch) {
      const subject = decodeURIComponent(triplePredicateMatch[1]!);
      const predicate = triplePredicateMatch[2]!;

      switch (method) {
        case 'GET':
          return this.handleGetTriple(subject, predicate);
        case 'PUT':
          return this.handleUpdateTriple(request, subject, predicate);
        case 'DELETE':
          return this.handleDeleteTriple(url, subject, predicate);
      }
    }

    // Match /triples/:subject
    const tripleSubjectMatch = url.pathname.match(/^\/triples\/([^/]+)$/);
    if (tripleSubjectMatch && method === 'GET') {
      const subject = decodeURIComponent(tripleSubjectMatch[1]!);
      return this.handleGetTriples(subject);
    }

    // Match /entities/:subject (DELETE)
    const entityMatch = url.pathname.match(/^\/entities\/([^/]+)$/);
    if (entityMatch && method === 'DELETE') {
      const subject = decodeURIComponent(entityMatch[1]!);
      return this.handleDeleteEntity(url, subject);
    }

    // Chunk management endpoints
    if (url.pathname === '/chunks' && method === 'GET') {
      return this.handleListChunks();
    }

    if (url.pathname === '/chunks/compact' && method === 'POST') {
      return this.handleCompact(url);
    }

    if (url.pathname === '/chunks/stats' && method === 'GET') {
      return this.handleChunkStats();
    }

    // Match /chunks/:chunkId
    const chunkMatch = url.pathname.match(/^\/chunks\/([^/]+)$/);
    if (chunkMatch) {
      const chunkId = decodeURIComponent(chunkMatch[1]!);
      switch (method) {
        case 'GET':
          return this.handleGetChunk(chunkId);
        case 'DELETE':
          return this.handleDeleteChunk(chunkId);
      }
    }

    switch (url.pathname) {
      case '/count':
        return this.handleCount(request);

      case '/stats':
        return this.handleStats();

      case '/reset':
        return this.handleReset();

      case '/echo':
        return this.handleEcho(request);

      case '/health':
        return new Response(
          JSON.stringify({
            status: 'healthy',
            uptime: Date.now() - this.startupTimestamp,
            schema: {
              initialized: this.schemaInitialized,
              version: this.schemaInitialized ? getCurrentVersion(this.ctx.storage.sql) : null,
              targetVersion: SCHEMA_VERSION,
            },
          }),
          {
            headers: { 'Content-Type': 'application/json' },
          }
        );

      default:
        // Default to count for any other path
        return this.handleCount(request);
    }
  }

  /**
   * Main count endpoint - increments counter and returns stats
   * Used for testing subrequest quota resets
   */
  private async handleCount(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const messageId = url.searchParams.get('messageId') ?? 'unknown';
    const index = url.searchParams.get('index') ?? '0';

    // Increment counters
    this.totalRequests++;
    this.lastRequestTimestamp = Date.now();

    // Track per-message counts
    const messageKey = `msg_${messageId}`;
    const currentCount = this.requestsPerMessage.get(messageKey) ?? 0;
    this.requestsPerMessage.set(messageKey, currentCount + 1);

    // Persist total count periodically (every 100 requests)
    if (this.totalRequests % 100 === 0) {
      await this.ctx.storage.put('totalRequests', this.totalRequests);
    }

    return new Response(
      JSON.stringify({
        success: true,
        requestNumber: this.totalRequests,
        messageId,
        index: parseInt(index),
        messageRequestCount: currentCount + 1,
        timestamp: this.lastRequestTimestamp,
        uptimeMs: this.lastRequestTimestamp - this.startupTimestamp,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Get statistics
   */
  private handleStats(): Response {
    // Convert Map to object for JSON
    const perMessageStats: Record<string, number> = {};
    for (const [key, value] of this.requestsPerMessage) {
      perMessageStats[key] = value;
    }

    // Get active WebSocket connections count
    const activeConnections = this.ctx.getWebSockets('shard-client').length;

    return new Response(
      JSON.stringify({
        totalRequests: this.totalRequests,
        lastRequestTimestamp: this.lastRequestTimestamp,
        startupTimestamp: this.startupTimestamp,
        perMessageStats,
        uptimeMs: Date.now() - this.startupTimestamp,
        // Hibernation metrics
        totalWakes: this.totalWakes,
        alarmCount: this.alarmCount,
        lastAlarmTimestamp: this.lastAlarmTimestamp,
        staleConnectionsCleanedUp: this.staleConnectionsCleanedUp,
        activeConnections,
        pendingOperationsCount: this.pendingOperations.size,
        maintenanceTasksCount: this.maintenanceTasks.length,
        completedMaintenanceTasks: this.completedMaintenanceTasks,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Reset all counters
   */
  private async handleReset(): Promise<Response> {
    this.totalRequests = 0;
    this.requestsPerMessage.clear();
    this.lastRequestTimestamp = 0;

    await this.ctx.storage.deleteAll();

    return new Response(
      JSON.stringify({
        message: 'Reset complete',
        timestamp: Date.now(),
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Echo endpoint for testing
   */
  private async handleEcho(request: Request): Promise<Response> {
    const body = await request.text();

    return new Response(
      JSON.stringify({
        method: request.method,
        url: request.url,
        headers: Object.fromEntries(request.headers.entries()),
        body: body || null,
        timestamp: Date.now(),
        requestNumber: this.totalRequests,
      }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // ==========================================================================
  // Triple CRUD Handlers
  // ==========================================================================

  /**
   * Convert JSON body to Triple object
   * Uses consolidated type-converters module for TypedObject conversion
   *
   * Uses assertion functions to validate branded types at this API boundary,
   * preventing bypass via direct casting.
   */
  private jsonToTriple(data: Record<string, unknown>): Triple {
    const object = data['object'] as JsonTypedObjectValue;
    const typedObject = jsonToTypedObject(object);

    // Validate branded types at the API boundary
    const subject = assertEntityId(data['subject'], 'subject');
    const predicate = assertPredicate(data['predicate'], 'predicate');
    const txId = assertTransactionId(data['txId'], 'txId');

    return {
      subject,
      predicate,
      object: typedObject,
      timestamp: BigInt(data['timestamp'] as number),
      txId,
    };
  }

  /**
   * Convert Triple to JSON-safe format
   * Uses consolidated type-converters module for TypedObject conversion
   */
  private tripleToJson(triple: Triple): Record<string, unknown> {
    return {
      subject: triple.subject,
      predicate: triple.predicate,
      object: typedObjectToJson(triple.object),
      timestamp: triple.timestamp.toString(),
      txId: triple.txId,
    };
  }

  /**
   * POST /triples - Insert triple(s)
   */
  private async handleInsertTriples(request: Request): Promise<Response> {
    try {
      const body = await request.json();
      const store = this.getTripleStore();
      const indexStore = this.getIndexStore();

      if (Array.isArray(body)) {
        // Batch insert
        const triples = body.map((t: Record<string, unknown>) => this.jsonToTriple(t));
        await store.insertTriples(triples);

        // Index all triples
        await indexStore.indexTriples(triples);

        return new Response(
          JSON.stringify({ success: true, count: triples.length }),
          {
            status: 201,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      } else {
        // Single insert
        const triple = this.jsonToTriple(body as Record<string, unknown>);
        await store.insertTriple(triple);

        // Index the triple
        await indexStore.indexTriple(triple);

        return new Response(
          JSON.stringify({ success: true }),
          {
            status: 201,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }
    } catch (error) {
      return errorResponse(
        ErrorCode.BAD_REQUEST,
        'Failed to insert triples',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /triples/:subject - Get all triples for subject
   */
  private async handleGetTriples(subject: string): Promise<Response> {
    try {
      // Validate branded type at API boundary
      const validatedSubject = assertEntityId(subject, 'subject');
      const store = this.getTripleStore();
      const triples = await store.getTriples(validatedSubject);

      return new Response(
        JSON.stringify({ triples: triples.map((t) => this.tripleToJson(t)) }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to get triples',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /triples/:subject/:predicate - Get specific triple
   */
  private async handleGetTriple(subject: string, predicate: string): Promise<Response> {
    try {
      // Validate branded types at API boundary
      const validatedSubject = assertEntityId(subject, 'subject');
      const validatedPredicate = assertPredicate(predicate, 'predicate');

      const store = this.getTripleStore();
      const triple = await store.getLatestTriple(validatedSubject, validatedPredicate);

      if (!triple) {
        return errorResponse(
          ErrorCode.NOT_FOUND,
          'Triple not found',
          { subject, predicate }
        );
      }

      return new Response(
        JSON.stringify({ triple: this.tripleToJson(triple) }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to get triple',
        { reason: String(error) }
      );
    }
  }

  /**
   * PUT /triples/:subject/:predicate - Update triple
   * Uses consolidated type-converters module for TypedObject conversion
   */
  private async handleUpdateTriple(
    request: Request,
    subject: string,
    predicate: string
  ): Promise<Response> {
    try {
      // Validate branded types at API boundary
      const validatedSubject = assertEntityId(subject, 'subject');
      const validatedPredicate = assertPredicate(predicate, 'predicate');

      const body = (await request.json()) as Record<string, unknown>;
      const validatedTxId = assertTransactionId(body['txId'], 'txId');

      const store = this.getTripleStore();

      const objectData = body['object'] as JsonTypedObjectValue;
      const typedObject = jsonToTypedObject(objectData);

      await store.updateTriple(
        validatedSubject,
        validatedPredicate,
        typedObject,
        validatedTxId
      );

      return new Response(
        JSON.stringify({ success: true }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.BAD_REQUEST,
        'Failed to update triple',
        { reason: String(error) }
      );
    }
  }

  /**
   * DELETE /triples/:subject/:predicate - Delete triple (soft delete)
   */
  private async handleDeleteTriple(url: URL, subject: string, predicate: string): Promise<Response> {
    try {
      // Validate branded types at API boundary
      const validatedSubject = assertEntityId(subject, 'subject');
      const validatedPredicate = assertPredicate(predicate, 'predicate');

      const txId = url.searchParams.get('txId');
      if (!txId) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          'txId query parameter is required',
          { param: 'txId' }
        );
      }

      const validatedTxId = assertTransactionId(txId, 'txId');

      const store = this.getTripleStore();
      await store.deleteTriple(validatedSubject, validatedPredicate, validatedTxId);

      return new Response(
        JSON.stringify({ success: true }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to delete triple',
        { reason: String(error) }
      );
    }
  }

  /**
   * DELETE /entities/:subject - Delete all triples for entity
   */
  private async handleDeleteEntity(url: URL, subject: string): Promise<Response> {
    try {
      // Validate branded type at API boundary
      const validatedSubject = assertEntityId(subject, 'subject');

      const txId = url.searchParams.get('txId');
      if (!txId) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          'txId query parameter is required',
          { param: 'txId' }
        );
      }

      const validatedTxId = assertTransactionId(txId, 'txId');

      const store = this.getTripleStore();
      await store.deleteEntity(validatedSubject, validatedTxId);

      return new Response(
        JSON.stringify({ success: true }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to delete entity',
        { reason: String(error) }
      );
    }
  }

  // ==========================================================================
  // Chunk Management Handlers
  // ==========================================================================

  /**
   * GET /chunks - List all chunks
   */
  private async handleListChunks(): Promise<Response> {
    try {
      const chunkStore = this.getChunkStore();
      const chunks = await chunkStore.listChunks();

      return new Response(
        JSON.stringify({ chunks }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to list chunks',
        { reason: String(error) }
      );
    }
  }

  /**
   * POST /chunks/compact - Trigger compaction
   * Query params:
   * - force=true: Force flush the buffer regardless of size
   */
  private async handleCompact(url: URL): Promise<Response> {
    try {
      const chunkStore = this.getChunkStore();
      const force = url.searchParams.get('force') === 'true';

      if (force) {
        // Force flush the buffer to a chunk
        const chunkId = await chunkStore.forceFlush();
        if (chunkId) {
          return new Response(
            JSON.stringify({ success: true, chunkId, operation: 'forceFlush' }),
            {
              status: 201,
              headers: { 'Content-Type': 'application/json' },
            }
          );
        }
      }

      // Run compaction on small chunks
      const compactedCount = await chunkStore.compact();

      if (compactedCount > 0) {
        return new Response(
          JSON.stringify({ success: true, compactedChunks: compactedCount, operation: 'compact' }),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      } else {
        return new Response(
          JSON.stringify({
            success: false,
            message: 'No compaction needed (not enough small chunks)',
          }),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }
    } catch (error) {
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to compact chunks',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /chunks/stats - Get chunk store statistics
   */
  private async handleChunkStats(): Promise<Response> {
    try {
      const chunkStore = this.getChunkStore();
      const stats = await chunkStore.getStats();

      return new Response(
        JSON.stringify({ stats }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to get chunk stats',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /chunks/:chunkId - Read a specific chunk
   */
  private async handleGetChunk(chunkId: string): Promise<Response> {
    try {
      const chunkStore = this.getChunkStore();
      const triples = await chunkStore.readChunk(chunkId);

      if (triples.length === 0) {
        return errorResponse(
          ErrorCode.NOT_FOUND,
          'Chunk not found',
          { chunkId }
        );
      }

      return new Response(
        JSON.stringify({
          chunkId,
          tripleCount: triples.length,
          triples: triples.map((t) => this.tripleToJson(t)),
        }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to get chunk',
        { reason: String(error) }
      );
    }
  }

  /**
   * DELETE /chunks/:chunkId - Delete a specific chunk
   */
  private async handleDeleteChunk(chunkId: string): Promise<Response> {
    try {
      const chunkStore = this.getChunkStore();
      await chunkStore.deleteChunk(chunkId);

      return new Response(
        JSON.stringify({ success: true }),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to delete chunk',
        { reason: String(error) }
      );
    }
  }

  // ==========================================================================
  // Orchestrator Query Handlers (lookup, traverse, filter)
  // ==========================================================================

  /**
   * Convert triples for a subject to an Entity object
   */
  private triplesToEntity(subject: EntityId, triples: Triple[]): Entity {
    const { namespace, context, localId } = resolveNamespace(subject);

    // Build properties from triples
    const props: Record<string, unknown> = {};
    let entityType: string | string[] = 'Unknown';

    for (const triple of triples) {
      // Skip tombstones (NULL type)
      if (triple.object.type === ObjectType.NULL) {
        continue;
      }

      // Extract type from $type predicate
      if (triple.predicate === '$type') {
        if (triple.object.type === ObjectType.STRING && triple.object.value) {
          entityType = triple.object.value as string;
        }
        continue;
      }

      // Extract value based on type
      let value: unknown;
      switch (triple.object.type) {
        case ObjectType.STRING:
          value = triple.object.value;
          break;
        case ObjectType.INT32:
        case ObjectType.INT64:
          value = triple.object.value;
          break;
        case ObjectType.FLOAT64:
          value = triple.object.value;
          break;
        case ObjectType.BOOL:
          value = triple.object.value;
          break;
        case ObjectType.REF:
          value = triple.object.value;
          break;
        case ObjectType.TIMESTAMP:
          value = triple.object.value;
          break;
        case ObjectType.JSON:
          value = triple.object.value;
          break;
        case ObjectType.GEO_POINT:
          value = triple.object.value;
          break;
        default:
          value = triple.object.value;
      }

      // Handle multi-valued properties
      if (props[triple.predicate] !== undefined) {
        const existing = props[triple.predicate];
        if (Array.isArray(existing)) {
          existing.push(value);
        } else {
          props[triple.predicate] = [existing, value];
        }
      } else {
        props[triple.predicate] = value;
      }
    }

    return {
      $id: subject,
      $type: entityType,
      $context: context,
      _namespace: namespace,
      _localId: localId,
      ...props,
    };
  }

  /**
   * GET /lookup - Lookup entities by ID(s)
   * Query params: ids (comma-separated entity IDs)
   *
   * Uses batch query to avoid N+1 query pattern - all entities are fetched
   * in a single SQL query using IN clause.
   */
  private async handleLookup(url: URL): Promise<Response> {
    try {
      const idsParam = url.searchParams.get('ids') ?? '';
      const ids = idsParam ? idsParam.split(',').filter(id => id.trim() !== '') : [];

      if (ids.length === 0) {
        return new Response(
          JSON.stringify([]),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // Validate all entity IDs at API boundary
      const validatedIds = assertEntityIdArray(ids, 'ids');

      const store = this.getTripleStore();

      // Batch query: fetch all entities in a single SQL query
      const triplesMap = await store.getTriplesForMultipleSubjects(validatedIds);

      // Convert triples to entities, preserving order from input ids
      const entities: Entity[] = [];
      for (const id of validatedIds) {
        const triples = triplesMap.get(id);
        if (triples && triples.length > 0) {
          entities.push(this.triplesToEntity(id, triples));
        }
      }

      return new Response(
        JSON.stringify(entities),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to lookup entities',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /traverse - Traverse from an entity following a predicate
   * Query params: from (entity ID), predicate (relationship name), depth (optional)
   *
   * Uses batch query to avoid N+1 query pattern - all target entities are fetched
   * in a single SQL query using IN clause.
   */
  private async handleTraverse(url: URL): Promise<Response> {
    try {
      const fromId = url.searchParams.get('from') ?? '';
      const predicateParam = url.searchParams.get('predicate') ?? '';
      // TODO: Implement multi-level traversal using depth parameter
      const _depth = parseInt(url.searchParams.get('depth') ?? '1', 10);
      void _depth; // Reserved for future multi-level traversal

      if (!fromId || !predicateParam) {
        return new Response(
          JSON.stringify([]),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // Validate branded types at API boundary
      const validatedFromId = assertEntityId(fromId, 'from');
      const validatedPredicate = assertPredicate(predicateParam, 'predicate');

      const store = this.getTripleStore();

      // Get triples for the source entity with the specified predicate
      const sourceTriples = await store.getTriples(validatedFromId);
      const refTriples = sourceTriples.filter(
        t => t.predicate === validatedPredicate && t.object.type === ObjectType.REF
      );

      // Collect target entity IDs
      const targetIds: EntityId[] = [];
      for (const triple of refTriples) {
        // Narrow the type - we already filtered for ObjectType.REF above
        if (triple.object.type === ObjectType.REF && triple.object.value) {
          targetIds.push(triple.object.value);
        }
      }

      if (targetIds.length === 0) {
        return new Response(
          JSON.stringify([]),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // Batch query: fetch all target entities in a single SQL query
      const triplesMap = await store.getTriplesForMultipleSubjects(targetIds);

      // Convert triples to entities, preserving order from targetIds
      const entities: Entity[] = [];
      for (const targetId of targetIds) {
        const targetTriples = triplesMap.get(targetId);
        if (targetTriples && targetTriples.length > 0) {
          entities.push(this.triplesToEntity(targetId, targetTriples));
        }
      }

      return new Response(
        JSON.stringify(entities),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to traverse',
        { reason: String(error) }
      );
    }
  }

  /**
   * GET /filter - Filter entities by property value
   * Query params: field (property name), op (operator), value
   */
  private async handleFilter(url: URL): Promise<Response> {
    try {
      const field = url.searchParams.get('field') ?? '';
      const op = url.searchParams.get('op') ?? '=';
      const valueStr = url.searchParams.get('value') ?? '';

      if (!field) {
        return new Response(
          JSON.stringify([]),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        );
      }

      // Validate predicate at API boundary
      const validatedField = assertPredicate(field, 'field');

      const store = this.getTripleStore();

      // Get all triples with this predicate
      const triples = await store.getTriplesByPredicate(validatedField);

      // Filter based on operator
      const matchingSubjects = new Set<string>();
      const value = isNaN(Number(valueStr)) ? valueStr : Number(valueStr);

      for (const triple of triples) {
        // Skip null-typed objects which don't have a value property
        if (triple.object.type === ObjectType.NULL) {
          continue;
        }
        const tripleValue = triple.object.value;
        let matches = false;

        switch (op) {
          case '=':
            matches = tripleValue === value;
            break;
          case '!=':
            matches = tripleValue !== value;
            break;
          case '>':
            matches = typeof tripleValue === 'number' && typeof value === 'number' && tripleValue > value;
            break;
          case '<':
            matches = typeof tripleValue === 'number' && typeof value === 'number' && tripleValue < value;
            break;
          case '>=':
            matches = typeof tripleValue === 'number' && typeof value === 'number' && tripleValue >= value;
            break;
          case '<=':
            matches = typeof tripleValue === 'number' && typeof value === 'number' && tripleValue <= value;
            break;
        }

        if (matches) {
          matchingSubjects.add(triple.subject);
        }
      }

      // Get full entities for matching subjects (batch query to avoid N+1)
      const subjectIds = [...matchingSubjects] as EntityId[];
      const triplesMap = await store.getTriplesForMultipleSubjects(subjectIds);

      const entities: Entity[] = [];
      for (const [subject, subjectTriples] of triplesMap) {
        entities.push(this.triplesToEntity(subject, subjectTriples));
      }

      return new Response(
        JSON.stringify(entities),
        {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      if (error instanceof BrandedTypeValidationError) {
        return errorResponse(
          ErrorCode.VALIDATION_ERROR,
          error.message,
          { code: error.code, value: String(error.value).slice(0, 100) }
        );
      }
      return errorResponse(
        ErrorCode.INTERNAL_ERROR,
        'Failed to filter entities',
        { reason: String(error) }
      );
    }
  }

  // ==========================================================================
  // WebSocket Hibernation Handlers
  // ==========================================================================

  /**
   * Handle WebSocket upgrade with hibernation support
   *
   * Uses ctx.acceptWebSocket() for hibernation - enables cost-effective
   * long-lived connections with 95% discount vs active connections.
   */
  private handleWebSocketUpgrade(_request: Request): Response {
    // Create WebSocket pair
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // Create attachment for hibernation state preservation
    const attachment: ShardWebSocketAttachment = {
      clientId: `shard_client_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`,
      connectedAt: Date.now(),
      totalMessagesReceived: 0,
      pendingOperations: 0,
      lastActivityTimestamp: Date.now(),
    };

    // Accept with hibernation - this is key for cost savings
    this.ctx.acceptWebSocket(server, ['shard-client']);

    // Store attachment data that persists across hibernation
    server.serializeAttachment(attachment);

    // Send welcome message before hibernating
    server.send(
      JSON.stringify({
        type: 'connected',
        clientId: attachment.clientId,
        message: 'ShardDO connected. Ready for triple operations.',
      })
    );

    return new Response(null, {
      status: 101,
      webSocket: client,
    } as ResponseInit);
  }

  /**
   * Handle WebSocket message - called on wake from hibernation
   *
   * Each wake gets fresh subrequest quota and processes messages.
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    this.totalWakes++;
    await this.ctx.storage.put('totalWakes', this.totalWakes);

    // Convert ArrayBuffer to string if needed
    const messageStr =
      typeof message === 'string' ? message : new TextDecoder().decode(message as ArrayBuffer);

    let data: Record<string, unknown>;
    try {
      data = JSON.parse(messageStr);
    } catch {
      ws.send(JSON.stringify({ type: 'error', code: 'PARSE_ERROR', message: 'Invalid JSON' }));
      return;
    }

    // Restore attachment from hibernation
    const attachment = ws.deserializeAttachment() as ShardWebSocketAttachment;
    if (!attachment) {
      ws.send(JSON.stringify({ type: 'error', message: 'No attachment found' }));
      return;
    }

    attachment.totalMessagesReceived++;
    attachment.lastActivityTimestamp = Date.now();

    // Handle different message types
    const msgType = data['type'];
    if (msgType === 'ping') {
      ws.send(
        JSON.stringify({
          type: 'pong',
          timestamp: data['timestamp'],
          serverTime: Date.now(),
          hibernationCount: this.totalWakes,
          clientId: attachment.clientId,
          messageCount: attachment.totalMessagesReceived,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'setState') {
      this.stateValue = data['value'] as number;
      await this.ctx.storage.put('stateValue', this.stateValue);
      ws.send(
        JSON.stringify({
          type: 'stateSet',
          value: this.stateValue,
          hibernationCount: this.totalWakes,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'getState') {
      ws.send(
        JSON.stringify({
          type: 'state',
          value: this.stateValue,
          hibernationCount: this.totalWakes,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'getStats') {
      const activeConnections = this.ctx.getWebSockets('shard-client').length;
      ws.send(
        JSON.stringify({
          type: 'stats',
          totalWakes: this.totalWakes,
          alarmCount: this.alarmCount,
          activeConnections,
          stateValue: this.stateValue,
          pendingOperationsCount: this.pendingOperations.size,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'queueOperation') {
      const operation = data['operation'] as { type: string; data: unknown };
      const operationId = `op_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

      const pendingOp: PendingOperation = {
        operationId,
        type: operation.type,
        data: operation.data,
        status: 'pending',
        createdAt: Date.now(),
      };

      this.pendingOperations.set(operationId, pendingOp);
      attachment.pendingOperations++;

      // Persist pending operations
      await this.ctx.storage.put(
        'pendingOperations',
        Array.from(this.pendingOperations.values())
      );

      ws.send(
        JSON.stringify({
          type: 'operationQueued',
          operationId,
          status: 'pending',
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'getOperationStatus') {
      const operationId = data['operationId'] as string;
      const operation = this.pendingOperations.get(operationId);

      if (operation) {
        ws.send(
          JSON.stringify({
            type: operation.status === 'pending' ? 'operationQueued' : 'operationResult',
            operationId: operation.operationId,
            status: operation.status,
            result: operation.result,
          })
        );
      } else {
        ws.send(
          JSON.stringify({
            type: 'error',
            message: 'Operation not found',
            operationId,
          })
        );
      }
      ws.serializeAttachment(attachment);
      return;
    }

    if (msgType === 'triggerCleanup') {
      const result = await this.cleanupStaleConnections();
      ws.send(
        JSON.stringify({
          type: 'cleanupResult',
          closedConnections: result.closedConnections,
          remainingConnections: result.remainingConnections,
        })
      );
      ws.serializeAttachment(attachment);
      return;
    }

    // Default response for unknown message types
    ws.send(
      JSON.stringify({
        type: 'error',
        code: 'UNKNOWN_MESSAGE',
        message: `Unknown message type: ${msgType}`,
      })
    );
    ws.serializeAttachment(attachment);
  }

  /**
   * Handle WebSocket close
   */
  async webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    const attachment = ws.deserializeAttachment() as ShardWebSocketAttachment | null;
    if (attachment) {
      console.log(`ShardDO WebSocket closed: ${attachment.clientId}`);
    }
  }

  /**
   * Handle WebSocket error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    const attachment = ws.deserializeAttachment() as ShardWebSocketAttachment | null;
    if (attachment) {
      console.error(`ShardDO WebSocket error for ${attachment.clientId}:`, error);
    } else {
      console.error('ShardDO WebSocket error:', error);
    }
  }

  /**
   * Handle alarm - called for scheduled maintenance
   */
  async alarm(): Promise<void> {
    this.alarmCount++;
    this.lastAlarmTimestamp = Date.now();
    await this.ctx.storage.put('alarmCount', this.alarmCount);
    await this.ctx.storage.put('lastAlarmTimestamp', this.lastAlarmTimestamp);

    // Process pending maintenance tasks
    await this.processMaintenanceTasks();

    // Process pending operations
    await this.processPendingOperations();

    // Cleanup stale connections
    await this.cleanupStaleConnections();
  }

  /**
   * Process pending maintenance tasks
   */
  private async processMaintenanceTasks(): Promise<void> {
    const now = Date.now();
    const pendingTasks = this.maintenanceTasks.filter((t) => !t.completedAt);

    // Sort by priority
    const priorityOrder = { high: 0, medium: 1, low: 2 };
    pendingTasks.sort((a, b) => priorityOrder[a.priority] - priorityOrder[b.priority]);

    for (const task of pendingTasks) {
      try {
        // Mark as completed (simplified - real impl would process the task)
        task.completedAt = now;
        this.completedMaintenanceTasks++;
      } catch (error) {
        console.error(`Failed to process maintenance task ${task.task}:`, error);
      }
    }

    // Persist changes
    await this.ctx.storage.put('maintenanceTasks', this.maintenanceTasks);
    await this.ctx.storage.put('completedMaintenanceTasks', this.completedMaintenanceTasks);
  }

  /**
   * Process pending operations
   */
  private async processPendingOperations(): Promise<void> {
    const now = Date.now();

    for (const [_id, op] of this.pendingOperations) {
      if (op.status === 'pending') {
        try {
          // Simplified processing - mark as completed
          op.status = 'completed';
          op.completedAt = now;
          op.result = { processed: true };
        } catch (error) {
          op.status = 'failed';
          op.completedAt = now;
          op.error = error instanceof Error ? error.message : String(error);
        }
      }
    }

    // Persist changes
    await this.ctx.storage.put(
      'pendingOperations',
      Array.from(this.pendingOperations.values())
    );
  }

  /**
   * Cleanup stale connections based on timeout
   */
  private async cleanupStaleConnections(): Promise<{ closedConnections: number; remainingConnections: number }> {
    const now = Date.now();
    const sockets = this.ctx.getWebSockets('shard-client');
    let closedConnections = 0;

    for (const ws of sockets) {
      const attachment = ws.deserializeAttachment() as ShardWebSocketAttachment | null;
      if (attachment) {
        const timeSinceActivity = now - attachment.lastActivityTimestamp;
        if (timeSinceActivity > this.connectionTimeoutMs) {
          ws.close(1000, 'Connection timeout');
          closedConnections++;
          this.staleConnectionsCleanedUp++;
        }
      }
    }

    if (closedConnections > 0) {
      await this.ctx.storage.put('staleConnectionsCleanedUp', this.staleConnectionsCleanedUp);
    }

    const remainingConnections = this.ctx.getWebSockets('shard-client').length;
    return { closedConnections, remainingConnections };
  }

  // ==========================================================================
  // HTTP Lifecycle Handlers
  // ==========================================================================

  /**
   * Get active connection count
   */
  private handleConnectionCount(): Response {
    const activeConnections = this.ctx.getWebSockets('shard-client').length;
    return new Response(
      JSON.stringify({ activeConnections }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Schedule a maintenance task
   */
  private async handleScheduleMaintenance(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { task: string; priority?: 'high' | 'medium' | 'low' };

      const task: MaintenanceTask = {
        task: body.task,
        priority: body.priority ?? 'medium',
        scheduledAt: Date.now(),
      };

      this.maintenanceTasks.push(task);
      await this.ctx.storage.put('maintenanceTasks', this.maintenanceTasks);

      return new Response(
        JSON.stringify({ success: true, task }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.BAD_REQUEST,
        'Failed to schedule maintenance task',
        { reason: String(error) }
      );
    }
  }

  /**
   * Get maintenance status
   */
  private handleMaintenanceStatus(): Response {
    const pendingTasks = this.maintenanceTasks.filter((t) => !t.completedAt).length;
    const completedTasks = this.completedMaintenanceTasks;

    return new Response(
      JSON.stringify({ pendingTasks, completedTasks }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  /**
   * Queue multiple operations
   */
  private async handleQueueOperations(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { operations: Array<{ type: string; data: unknown }> };

      const operationIds: string[] = [];

      for (const op of body.operations) {
        const operationId = `op_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;

        const pendingOp: PendingOperation = {
          operationId,
          type: op.type,
          data: op.data,
          status: 'pending',
          createdAt: Date.now(),
        };

        this.pendingOperations.set(operationId, pendingOp);
        operationIds.push(operationId);
      }

      // Persist pending operations
      await this.ctx.storage.put(
        'pendingOperations',
        Array.from(this.pendingOperations.values())
      );

      return new Response(
        JSON.stringify({ queuedCount: operationIds.length, operationIds }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.BAD_REQUEST,
        'Failed to queue operations',
        { reason: String(error) }
      );
    }
  }

  /**
   * Get pending operations count
   */
  private handleGetPendingOperations(): Response {
    const pending = Array.from(this.pendingOperations.values()).filter(
      (op) => op.status === 'pending'
    );

    return new Response(
      JSON.stringify({ count: pending.length }),
      {
        headers: { 'Content-Type': 'application/json' },
      }
    );
  }

  // Configuration validation constants
  private static readonly MIN_CONNECTION_TIMEOUT_MS = 1000; // 1 second
  private static readonly MAX_CONNECTION_TIMEOUT_MS = 300000; // 5 minutes

  /**
   * Update configuration
   */
  private async handleConfig(request: Request): Promise<Response> {
    try {
      const body = (await request.json()) as { connectionTimeoutMs?: unknown };

      if (body.connectionTimeoutMs !== undefined) {
        // Validate connectionTimeoutMs is a number
        if (typeof body.connectionTimeoutMs !== 'number') {
          return errorResponse(
            ErrorCode.VALIDATION_ERROR,
            'connectionTimeoutMs must be a number',
            { param: 'connectionTimeoutMs', value: body.connectionTimeoutMs }
          );
        }

        // Validate connectionTimeoutMs is not NaN or Infinity
        if (!Number.isFinite(body.connectionTimeoutMs)) {
          return errorResponse(
            ErrorCode.VALIDATION_ERROR,
            'connectionTimeoutMs must be a finite number',
            { param: 'connectionTimeoutMs', value: body.connectionTimeoutMs }
          );
        }

        // Validate connectionTimeoutMs is positive
        if (body.connectionTimeoutMs <= 0) {
          return errorResponse(
            ErrorCode.VALIDATION_ERROR,
            'connectionTimeoutMs must be a positive number',
            { param: 'connectionTimeoutMs', value: body.connectionTimeoutMs }
          );
        }

        // Validate connectionTimeoutMs is within bounds
        if (body.connectionTimeoutMs < ShardDO.MIN_CONNECTION_TIMEOUT_MS) {
          return errorResponse(
            ErrorCode.VALIDATION_ERROR,
            `connectionTimeoutMs must be at least ${ShardDO.MIN_CONNECTION_TIMEOUT_MS}ms (1 second)`,
            {
              param: 'connectionTimeoutMs',
              value: body.connectionTimeoutMs,
              min: ShardDO.MIN_CONNECTION_TIMEOUT_MS,
            }
          );
        }

        if (body.connectionTimeoutMs > ShardDO.MAX_CONNECTION_TIMEOUT_MS) {
          return errorResponse(
            ErrorCode.VALIDATION_ERROR,
            `connectionTimeoutMs must be at most ${ShardDO.MAX_CONNECTION_TIMEOUT_MS}ms (5 minutes)`,
            {
              param: 'connectionTimeoutMs',
              value: body.connectionTimeoutMs,
              max: ShardDO.MAX_CONNECTION_TIMEOUT_MS,
            }
          );
        }

        this.connectionTimeoutMs = body.connectionTimeoutMs;
      }

      return new Response(
        JSON.stringify({ success: true, config: { connectionTimeoutMs: this.connectionTimeoutMs } }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    } catch (error) {
      return errorResponse(
        ErrorCode.BAD_REQUEST,
        'Failed to update configuration',
        { reason: String(error) }
      );
    }
  }
}
