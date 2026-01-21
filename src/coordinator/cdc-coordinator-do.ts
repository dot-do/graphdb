/**
 * CDCCoordinatorDO - CDC Pipeline Coordinator Durable Object
 *
 * Orchestrates CDC (Change Data Capture) from child Shard DOs:
 * - Accepts WebSocket connections from Shard DOs
 * - Buffers CDC events until batch threshold (100ms or 1000 events)
 * - Flushes to R2 in GraphCol format
 * - Tracks per-shard sequence numbers
 * - Supports hibernation between batches (95% cost discount)
 *
 * Architecture:
 * - Shard DOs connect via WebSocket and register with namespace
 * - CDC events are buffered by namespace
 * - Alarms trigger periodic flushes for low-volume periods
 * - Sequence numbers ensure exactly-once delivery
 *
 * @see CLAUDE.md for architecture details
 */

import type { Env } from '../core/index.js';
import type { CDCEvent } from '../storage/cdc-types.js';
import type { Namespace, EntityId, Predicate, TransactionId } from '../core/types.js';
import type { Triple } from '../core/triple.js';
import { createNamespace } from '../core/types.js';
import { jsonToTypedObject, type JsonTypedObjectValue } from '../core/type-converters.js';
import { getCDCPath } from '../storage/r2-writer.js';
import { encodeGraphCol } from '../storage/graphcol.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Shard registration information
 */
export interface ShardRegistration {
  /** Unique shard identifier */
  shardId: string;
  /** Namespace URL for this shard's data */
  namespace: Namespace;
  /** Last acknowledged sequence number */
  lastSequence: bigint;
  /** Associated WebSocket */
  webSocket?: WebSocket;
  /** Registration timestamp */
  registeredAt: number;
}

/**
 * CDC batch for flushing to R2
 */
export interface CDCBatch {
  /** Namespace for this batch */
  namespace: Namespace;
  /** CDC events in this batch */
  events: CDCEvent[];
  /** Shard sequences included in this batch */
  shardSequences: Map<string, bigint>;
}

/**
 * Coordinator CDC statistics
 */
export interface CoordinatorCDCStats {
  /** Total events currently buffered */
  eventsBuffered: number;
  /** Total events flushed to R2 */
  eventsFlushed: number;
  /** Number of flush operations */
  flushCount: number;
  /** Bytes written to R2 */
  bytesWritten: number;
  /** Number of registered shards */
  registeredShards: number;
  /** Startup timestamp */
  startupTimestamp: number;
  /** Uptime in milliseconds */
  uptimeMs: number;
}

/**
 * WebSocket attachment for hibernation state
 */
interface WebSocketAttachment {
  shardId: string;
  namespace: string;
}

/**
 * Message types from shards
 */
interface RegisterMessage {
  type: 'register';
  shardId: string;
  namespace: string;
  lastSequence: string; // BigInt as string for JSON
}

interface DeregisterMessage {
  type: 'deregister';
  shardId: string;
}

interface CDCMessage {
  type: 'cdc';
  shardId: string;
  events: SerializedCDCEvent[];
  sequence: string; // BigInt as string for JSON
}

interface SerializedCDCEvent {
  type: 'insert' | 'update' | 'delete';
  triple: SerializedTriple;
  previousValue?: SerializedTriple;
  timestamp: string; // BigInt as string for JSON
}

interface SerializedTriple {
  subject: string;
  predicate: string;
  object: {
    type: number;
    value?: unknown; // Serialized value - type depends on object.type
  };
  timestamp: string; // BigInt as string
  txId: string;
}

type ShardMessage = RegisterMessage | DeregisterMessage | CDCMessage;

// ============================================================================
// Constants
// ============================================================================

/** Flush timeout in milliseconds */
const FLUSH_TIMEOUT_MS = 100;

/** Maximum batch size before auto-flush */
const MAX_BATCH_SIZE = 1000;

// ============================================================================
// Implementation
// ============================================================================

export class CDCCoordinatorDO implements DurableObject {
  private readonly ctx: DurableObjectState;
  private readonly env: Env;

  // In-memory state
  private shardRegistrations: Map<string, ShardRegistration> = new Map();
  private eventBuffers: Map<string, CDCEvent[]> = new Map(); // namespace -> events
  private shardWebSockets: Map<string, WebSocket> = new Map(); // shardId -> WebSocket

  // Statistics
  private eventsBuffered: number = 0;
  private eventsFlushed: number = 0;
  private flushCount: number = 0;
  private bytesWritten: number = 0;
  private readonly startupTimestamp: number;

  constructor(ctx: DurableObjectState, env: Env) {
    this.ctx = ctx;
    this.env = env;
    this.startupTimestamp = Date.now();

    // Restore state from storage on startup
    ctx.blockConcurrencyWhile(async () => {
      await this.restoreState();
    });
  }

  /**
   * Restore state from durable storage after hibernation
   */
  private async restoreState(): Promise<void> {
    // Restore shard registrations
    const stored = await this.ctx.storage.list<{
      shardId: string;
      namespace: string;
      lastSequence: string;
      registeredAt: number;
    }>({
      prefix: 'shard:',
    });

    for (const [key, storedReg] of stored) {
      const shardId = key.replace('shard:', '');
      // Build registration without webSocket - will be re-associated when shard reconnects
      const registration: ShardRegistration = {
        shardId: storedReg.shardId,
        namespace: createNamespace(storedReg.namespace),
        lastSequence: BigInt(storedReg.lastSequence),
        registeredAt: storedReg.registeredAt,
      };
      this.shardRegistrations.set(shardId, registration);
    }

    // Restore WebSocket associations from hibernated connections
    const websockets = this.ctx.getWebSockets();
    for (const ws of websockets) {
      const attachment = ws.deserializeAttachment() as WebSocketAttachment | null;
      if (attachment?.shardId) {
        this.shardWebSockets.set(attachment.shardId, ws);
        const registration = this.shardRegistrations.get(attachment.shardId);
        if (registration) {
          registration.webSocket = ws;
        }
      }
    }
  }

  /**
   * Handle HTTP requests
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // WebSocket upgrade for shard connections
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocketUpgrade();
    }

    // HTTP endpoints
    switch (url.pathname) {
      case '/health':
        return new Response(
          JSON.stringify({
            status: 'healthy',
            uptime: Date.now() - this.startupTimestamp,
          }),
          { headers: { 'Content-Type': 'application/json' } }
        );

      case '/stats':
        const stats = await this.getStats();
        return new Response(JSON.stringify(stats), {
          headers: { 'Content-Type': 'application/json' },
        });

      case '/shards':
        const registrations = await this.getRegisteredShards();
        return new Response(
          JSON.stringify(
            registrations.map((r) => ({
              ...r,
              lastSequence: r.lastSequence.toString(),
              webSocket: undefined,
            }))
          ),
          { headers: { 'Content-Type': 'application/json' } }
        );

      default:
        return new Response('Not Found', { status: 404 });
    }
  }

  /**
   * Handle WebSocket upgrade request
   */
  private handleWebSocketUpgrade(): Response {
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    // Use acceptWebSocket for hibernation support
    this.ctx.acceptWebSocket(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  /**
   * Handle incoming WebSocket messages (called after hibernation wake)
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    try {
      const msgStr = typeof message === 'string' ? message : new TextDecoder().decode(message);
      const parsed = JSON.parse(msgStr) as ShardMessage;

      switch (parsed.type) {
        case 'register':
          await this.handleRegister(ws, parsed);
          break;

        case 'deregister':
          await this.handleDeregister(parsed);
          break;

        case 'cdc':
          await this.handleCDCEvents(ws, parsed);
          break;

        default:
          this.sendError(ws, `Unknown message type: ${(parsed as any).type}`);
      }
    } catch (error) {
      this.sendError(ws, `Invalid message: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }

  /**
   * Handle shard registration
   */
  private async handleRegister(ws: WebSocket, msg: RegisterMessage): Promise<void> {
    const { shardId, namespace, lastSequence } = msg;

    const registration: ShardRegistration = {
      shardId,
      namespace: createNamespace(namespace),
      lastSequence: BigInt(lastSequence),
      webSocket: ws,
      registeredAt: Date.now(),
    };

    // Store registration
    this.shardRegistrations.set(shardId, registration);
    this.shardWebSockets.set(shardId, ws);

    // Persist to durable storage
    await this.ctx.storage.put(`shard:${shardId}`, {
      shardId,
      namespace,
      lastSequence: lastSequence,
      registeredAt: registration.registeredAt,
    });

    // Set WebSocket attachment for hibernation
    (ws as any).serializeAttachment({ shardId, namespace } as WebSocketAttachment);

    // Send acknowledgment
    ws.send(
      JSON.stringify({
        type: 'registered',
        shardId,
        message: 'Registration successful',
      })
    );
  }

  /**
   * Handle shard deregistration
   */
  private async handleDeregister(msg: DeregisterMessage): Promise<void> {
    const { shardId } = msg;

    this.shardRegistrations.delete(shardId);
    this.shardWebSockets.delete(shardId);

    // Remove from durable storage
    await this.ctx.storage.delete(`shard:${shardId}`);
    await this.ctx.storage.delete(`sequence:${shardId}`);
  }

  /**
   * Handle incoming CDC events
   */
  private async handleCDCEvents(ws: WebSocket, msg: CDCMessage): Promise<void> {
    const { shardId, events, sequence } = msg;
    const sequenceBigInt = BigInt(sequence);

    // Check if shard is registered
    const registration = this.shardRegistrations.get(shardId);
    if (!registration) {
      this.sendError(ws, `Shard ${shardId} is not registered`);
      return;
    }

    // Check sequence ordering
    if (sequenceBigInt <= registration.lastSequence) {
      this.sendError(
        ws,
        `Out of order sequence: received ${sequence}, expected > ${registration.lastSequence}`
      );
      return;
    }

    // Deserialize CDC events
    const deserializedEvents = events.map((e) => this.deserializeCDCEvent(e));

    // Buffer events by namespace
    const namespace = registration.namespace;
    let buffer = this.eventBuffers.get(namespace);
    if (!buffer) {
      buffer = [];
      this.eventBuffers.set(namespace, buffer);
    }
    buffer.push(...deserializedEvents);

    // Update statistics
    this.eventsBuffered += deserializedEvents.length;

    // Update registration sequence
    registration.lastSequence = sequenceBigInt;

    // Check if we should auto-flush (batch size threshold)
    const totalBuffered = this.getTotalBufferedEvents();
    if (totalBuffered >= MAX_BATCH_SIZE) {
      await this.flushAllBuffers();
    } else {
      // Set alarm for timeout-based flush if not already set
      await this.ensureFlushAlarm();
    }
  }

  /**
   * Deserialize a CDC event from JSON
   */
  private deserializeCDCEvent(e: SerializedCDCEvent): CDCEvent {
    const event: CDCEvent = {
      type: e.type,
      triple: this.deserializeTriple(e.triple),
      timestamp: BigInt(e.timestamp),
    };
    if (e.previousValue) {
      event.previousValue = this.deserializeTriple(e.previousValue);
    }
    return event;
  }

  /**
   * Deserialize a triple from JSON
   * Uses consolidated type-converters module for TypedObject conversion
   */
  private deserializeTriple(t: SerializedTriple): Triple {
    const typedObject = jsonToTypedObject(t.object as JsonTypedObjectValue);

    return {
      subject: t.subject as EntityId,
      predicate: t.predicate as Predicate,
      object: typedObject,
      timestamp: BigInt(t.timestamp),
      txId: t.txId as TransactionId,
    };
  }

  /**
   * Get total number of buffered events across all namespaces
   */
  private getTotalBufferedEvents(): number {
    let total = 0;
    for (const buffer of this.eventBuffers.values()) {
      total += buffer.length;
    }
    return total;
  }

  /**
   * Ensure a flush alarm is set
   */
  private async ensureFlushAlarm(): Promise<void> {
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (currentAlarm === null) {
      await this.ctx.storage.setAlarm(Date.now() + FLUSH_TIMEOUT_MS);
    }
  }

  /**
   * Handle alarm (triggered for timeout-based flush)
   */
  async alarm(): Promise<void> {
    await this.flushAllBuffers();
  }

  /**
   * Flush all buffered events to R2
   */
  private async flushAllBuffers(): Promise<void> {
    // Clear alarm
    await this.ctx.storage.deleteAlarm();

    // Process each namespace buffer
    for (const [namespaceStr, events] of this.eventBuffers) {
      if (events.length === 0) continue;

      const namespace = namespaceStr as Namespace;

      try {
        // Encode events as GraphCol
        const triples = events.map((e) => e.triple);
        const encoded = encodeGraphCol(triples, namespace);

        // Generate R2 path
        const firstEvent = events[0]!;
        const maxTimestamp = events.reduce(
          (max, e) => (e.timestamp > max ? e.timestamp : max),
          firstEvent.timestamp
        );
        const path = getCDCPath(namespace, maxTimestamp);

        // Write to R2
        await this.env.LAKEHOUSE.put(path, encoded);

        // Update statistics
        this.eventsFlushed += events.length;
        this.bytesWritten += encoded.length;
        this.flushCount++;

        // Find shards for this namespace and send acknowledgments
        await this.acknowledgeShards(namespace, events);

        // Clear buffer
        events.length = 0;
      } catch (error) {
        console.error(`Failed to flush namespace ${namespace}:`, error);
        // Keep events in buffer for retry
      }
    }

    // Update buffered count
    this.eventsBuffered = this.getTotalBufferedEvents();

    // Persist sequence numbers
    await this.persistSequenceNumbers();
  }

  /**
   * Send acknowledgments to shards after successful flush
   */
  private async acknowledgeShards(namespace: Namespace, events: CDCEvent[]): Promise<void> {
    // Find all shards for this namespace
    for (const [shardId, registration] of this.shardRegistrations) {
      if (registration.namespace === namespace && registration.webSocket) {
        try {
          registration.webSocket.send(
            JSON.stringify({
              type: 'ack',
              shardId,
              sequence: registration.lastSequence.toString(),
              eventsAcked: events.length,
            })
          );
        } catch (error) {
          // WebSocket may be closed
          console.error(`Failed to send ack to shard ${shardId}:`, error);
        }
      }
    }
  }

  /**
   * Persist sequence numbers to durable storage
   */
  private async persistSequenceNumbers(): Promise<void> {
    const puts: Promise<void>[] = [];
    for (const [shardId, registration] of this.shardRegistrations) {
      puts.push(
        this.ctx.storage.put(`sequence:${shardId}`, registration.lastSequence)
      );
    }
    await Promise.all(puts);
  }

  /**
   * Handle WebSocket close
   */
  async webSocketClose(ws: WebSocket, _code: number, _reason: string): Promise<void> {
    // Find and remove the shard associated with this WebSocket
    const attachment = (ws as any).deserializeAttachment?.() as WebSocketAttachment | null;
    if (attachment?.shardId) {
      await this.handleDeregister({ type: 'deregister', shardId: attachment.shardId });
    }
  }

  /**
   * Handle WebSocket error
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    console.error('WebSocket error:', error);
    // Treat as close
    await this.webSocketClose(ws, 1006, 'Error');
  }

  /**
   * Send error message to WebSocket
   */
  private sendError(ws: WebSocket, message: string): void {
    try {
      ws.send(JSON.stringify({ type: 'error', message }));
    } catch {
      // WebSocket may be closed
    }
  }

  /**
   * Get all registered shards
   */
  async getRegisteredShards(): Promise<ShardRegistration[]> {
    return Array.from(this.shardRegistrations.values());
  }

  /**
   * Get coordinator statistics
   */
  async getStats(): Promise<CoordinatorCDCStats> {
    return {
      eventsBuffered: this.eventsBuffered,
      eventsFlushed: this.eventsFlushed,
      flushCount: this.flushCount,
      bytesWritten: this.bytesWritten,
      registeredShards: this.shardRegistrations.size,
      startupTimestamp: this.startupTimestamp,
      uptimeMs: Date.now() - this.startupTimestamp,
    };
  }
}
