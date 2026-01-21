/**
 * OpenTelemetry integration for GraphDB
 *
 * Provides opt-in observability with:
 * - Trace spans for key operations (query, traversal, write)
 * - Metric counters (query count, latency histogram)
 * - Workers-compatible approach with minimal dependencies
 *
 * Design principles:
 * - Zero runtime cost when disabled (no-op implementations)
 * - No external dependencies (uses built-in Workers APIs)
 * - W3C Trace Context compatible format
 * - Exportable to any OTLP-compatible backend
 *
 * @example
 * ```typescript
 * import { initOtel, tracer, metrics } from './observability/otel';
 *
 * // Initialize once at worker startup
 * initOtel({
 *   enabled: true,
 *   serviceName: 'graphdb',
 *   exporter: async (spans) => {
 *     await fetch('https://otel-collector.example.com/v1/traces', {
 *       method: 'POST',
 *       body: JSON.stringify(spans),
 *     });
 *   },
 * });
 *
 * // Create spans
 * const span = tracer.startSpan('query', { 'query.type': 'traverse' });
 * try {
 *   const result = await performQuery();
 *   span.setAttributes({ 'result.count': result.length });
 * } finally {
 *   span.end();
 * }
 *
 * // Record metrics
 * metrics.recordQuery('traverse', 150);
 * ```
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Span status codes following OpenTelemetry specification
 */
export enum SpanStatusCode {
  /** The default status - unset */
  UNSET = 0,
  /** The operation completed successfully */
  OK = 1,
  /** The operation contained an error */
  ERROR = 2,
}

/**
 * Span kind following OpenTelemetry specification
 */
export enum SpanKind {
  /** Default, unspecified kind */
  INTERNAL = 0,
  /** A server handling a synchronous request */
  SERVER = 1,
  /** A client making a synchronous request */
  CLIENT = 2,
  /** A producer sending a message */
  PRODUCER = 3,
  /** A consumer receiving a message */
  CONSUMER = 4,
}

/**
 * Attribute values supported in spans
 */
export type AttributeValue = string | number | boolean | string[] | number[] | boolean[];

/**
 * Span attributes map
 */
export type SpanAttributes = Record<string, AttributeValue>;

/**
 * Span event representing a point-in-time occurrence
 */
export interface SpanEvent {
  /** Event name */
  name: string;
  /** Timestamp in milliseconds since epoch */
  timestamp: number;
  /** Event attributes */
  attributes?: SpanAttributes | undefined;
}

/**
 * Span link to another span
 */
export interface SpanLink {
  /** Trace ID of the linked span */
  traceId: string;
  /** Span ID of the linked span */
  spanId: string;
  /** Link attributes */
  attributes?: SpanAttributes | undefined;
}

/**
 * Span context for trace propagation
 */
export interface SpanContext {
  /** 32-character lowercase hex trace ID */
  traceId: string;
  /** 16-character lowercase hex span ID */
  spanId: string;
  /** Trace flags (sampled = 1) */
  traceFlags: number;
}

/**
 * Completed span data for export
 */
export interface SpanData {
  /** Span name/operation */
  name: string;
  /** Span context */
  context: SpanContext;
  /** Parent span context (if any) */
  parentContext?: SpanContext | undefined;
  /** Span kind */
  kind: SpanKind;
  /** Start time in milliseconds since epoch */
  startTime: number;
  /** End time in milliseconds since epoch */
  endTime: number;
  /** Duration in milliseconds */
  duration: number;
  /** Span attributes */
  attributes: SpanAttributes;
  /** Span events */
  events: SpanEvent[];
  /** Span links */
  links: SpanLink[];
  /** Status code */
  statusCode: SpanStatusCode;
  /** Status message (for errors) */
  statusMessage?: string | undefined;
  /** Service name */
  serviceName: string;
}

/**
 * Span interface for recording trace data
 */
export interface Span {
  /** Get the span context for propagation */
  getContext(): SpanContext;

  /** Set a single attribute */
  setAttribute(key: string, value: AttributeValue): Span;

  /** Set multiple attributes */
  setAttributes(attributes: SpanAttributes): Span;

  /** Add an event to the span */
  addEvent(name: string, attributes?: SpanAttributes): Span;

  /** Add a link to another span */
  addLink(context: SpanContext, attributes?: SpanAttributes): Span;

  /** Set the span status */
  setStatus(code: SpanStatusCode, message?: string): Span;

  /** Record an exception */
  recordException(error: Error): Span;

  /** End the span (must be called to export) */
  end(): void;

  /** Check if the span is recording */
  isRecording(): boolean;
}

/**
 * Options for starting a span
 */
export interface SpanOptions {
  /** Span kind */
  kind?: SpanKind;
  /** Initial attributes */
  attributes?: SpanAttributes;
  /** Links to other spans */
  links?: SpanLink[];
  /** Parent span context (for distributed tracing) */
  parent?: SpanContext;
  /** Start time override (milliseconds since epoch) */
  startTime?: number;
}

/**
 * Tracer interface for creating spans
 */
export interface Tracer {
  /** Start a new span */
  startSpan(name: string, options?: SpanOptions): Span;

  /** Start a span that is a child of the given parent */
  startSpanWithParent(name: string, parent: SpanContext, options?: SpanOptions): Span;
}

/**
 * Span exporter function
 */
export type SpanExporter = (spans: SpanData[]) => Promise<void>;

/**
 * Metric exporter function
 */
export type MetricExporter = (metrics: MetricData[]) => Promise<void>;

/**
 * Configuration for OpenTelemetry
 */
export interface OtelConfig {
  /** Enable or disable tracing/metrics collection */
  enabled: boolean;
  /** Service name for attribution */
  serviceName: string;
  /** Span exporter function (called when spans end) */
  spanExporter?: SpanExporter;
  /** Metric exporter function (called periodically or on flush) */
  metricExporter?: MetricExporter;
  /** Sampling rate (0.0 to 1.0, default 1.0) */
  samplingRate?: number;
  /** Maximum spans to buffer before export (default 100) */
  maxSpanBuffer?: number;
  /** Maximum metrics to buffer before export (default 1000) */
  maxMetricBuffer?: number;
}

// ============================================================================
// Metrics Types
// ============================================================================

/**
 * Metric data point
 */
export interface MetricData {
  /** Metric name */
  name: string;
  /** Metric type */
  type: 'counter' | 'histogram' | 'gauge';
  /** Metric value */
  value: number;
  /** Timestamp in milliseconds since epoch */
  timestamp: number;
  /** Metric labels */
  labels: Record<string, string>;
  /** Service name */
  serviceName: string;
  /** For histograms: bucket boundaries and counts */
  histogram?: {
    buckets: number[];
    counts: number[];
    sum: number;
    count: number;
  };
}

/**
 * Histogram bucket boundaries (in milliseconds for latency)
 */
const DEFAULT_LATENCY_BUCKETS = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/**
 * Metrics collector interface
 */
export interface MetricsCollector {
  /** Increment a counter */
  incrementCounter(name: string, value?: number, labels?: Record<string, string>): void;

  /** Record a histogram value */
  recordHistogram(name: string, value: number, labels?: Record<string, string>): void;

  /** Set a gauge value */
  setGauge(name: string, value: number, labels?: Record<string, string>): void;

  /** Record a query metric (convenience method) */
  recordQuery(queryType: string, durationMs: number, labels?: Record<string, string>): void;

  /** Record a traversal metric (convenience method) */
  recordTraversal(depth: number, durationMs: number, labels?: Record<string, string>): void;

  /** Record a write metric (convenience method) */
  recordWrite(entityCount: number, durationMs: number, labels?: Record<string, string>): void;

  /** Flush buffered metrics to exporter */
  flush(): Promise<void>;

  /** Get current metrics snapshot */
  getSnapshot(): MetricData[];
}

// ============================================================================
// Global State
// ============================================================================

let globalConfig: OtelConfig = {
  enabled: false,
  serviceName: 'graphdb',
  samplingRate: 1.0,
  maxSpanBuffer: 100,
  maxMetricBuffer: 1000,
};

const spanBuffer: SpanData[] = [];
const metricBuffer: MetricData[] = [];

// Histogram state for aggregation
const histogramState: Map<
  string,
  {
    buckets: number[];
    counts: number[];
    sum: number;
    count: number;
    labels: Record<string, string>;
  }
> = new Map();

// ============================================================================
// ID Generation (Workers-compatible)
// ============================================================================

/**
 * Generate a random hex string of specified length
 * Uses crypto.getRandomValues which is available in Workers
 */
function generateHexId(length: number): string {
  const bytes = new Uint8Array(length / 2);
  crypto.getRandomValues(bytes);
  return Array.from(bytes)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Generate a 32-character trace ID
 */
function generateTraceId(): string {
  return generateHexId(32);
}

/**
 * Generate a 16-character span ID
 */
function generateSpanId(): string {
  return generateHexId(16);
}

// ============================================================================
// No-op Implementations
// ============================================================================

/**
 * No-op span that does nothing (used when tracing is disabled)
 */
class NoopSpan implements Span {
  private static readonly NOOP_CONTEXT: SpanContext = {
    traceId: '00000000000000000000000000000000',
    spanId: '0000000000000000',
    traceFlags: 0,
  };

  getContext(): SpanContext {
    return NoopSpan.NOOP_CONTEXT;
  }

  setAttribute(_key: string, _value: AttributeValue): Span {
    return this;
  }

  setAttributes(_attributes: SpanAttributes): Span {
    return this;
  }

  addEvent(_name: string, _attributes?: SpanAttributes): Span {
    return this;
  }

  addLink(_context: SpanContext, _attributes?: SpanAttributes): Span {
    return this;
  }

  setStatus(_code: SpanStatusCode, _message?: string): Span {
    return this;
  }

  recordException(_error: Error): Span {
    return this;
  }

  end(): void {
    // No-op
  }

  isRecording(): boolean {
    return false;
  }
}

const NOOP_SPAN = new NoopSpan();

/**
 * No-op tracer that returns no-op spans
 */
class NoopTracer implements Tracer {
  startSpan(_name: string, _options?: SpanOptions): Span {
    return NOOP_SPAN;
  }

  startSpanWithParent(_name: string, _parent: SpanContext, _options?: SpanOptions): Span {
    return NOOP_SPAN;
  }
}

const NOOP_TRACER = new NoopTracer();

/**
 * No-op metrics collector
 */
class NoopMetricsCollector implements MetricsCollector {
  incrementCounter(_name: string, _value?: number, _labels?: Record<string, string>): void {
    // No-op
  }

  recordHistogram(_name: string, _value: number, _labels?: Record<string, string>): void {
    // No-op
  }

  setGauge(_name: string, _value: number, _labels?: Record<string, string>): void {
    // No-op
  }

  recordQuery(_queryType: string, _durationMs: number, _labels?: Record<string, string>): void {
    // No-op
  }

  recordTraversal(_depth: number, _durationMs: number, _labels?: Record<string, string>): void {
    // No-op
  }

  recordWrite(_entityCount: number, _durationMs: number, _labels?: Record<string, string>): void {
    // No-op
  }

  async flush(): Promise<void> {
    // No-op
  }

  getSnapshot(): MetricData[] {
    return [];
  }
}

const NOOP_METRICS = new NoopMetricsCollector();

// ============================================================================
// Real Implementations
// ============================================================================

/**
 * Real span implementation that records trace data
 */
class RealSpan implements Span {
  private context: SpanContext;
  private parentContext: SpanContext | undefined;
  private name: string;
  private kind: SpanKind;
  private startTime: number;
  private endTime: number | undefined;
  private attributes: SpanAttributes = {};
  private events: SpanEvent[] = [];
  private links: SpanLink[] = [];
  private statusCode: SpanStatusCode = SpanStatusCode.UNSET;
  private statusMessage: string | undefined;
  private ended = false;

  constructor(name: string, options: SpanOptions = {}) {
    this.name = name;
    this.kind = options.kind ?? SpanKind.INTERNAL;
    this.startTime = options.startTime ?? Date.now();
    this.parentContext = options.parent;
    this.links = options.links ?? [];

    if (options.attributes) {
      this.attributes = { ...options.attributes };
    }

    // Generate span context
    this.context = {
      traceId: options.parent?.traceId ?? generateTraceId(),
      spanId: generateSpanId(),
      traceFlags: 1, // Sampled
    };
  }

  getContext(): SpanContext {
    return this.context;
  }

  setAttribute(key: string, value: AttributeValue): Span {
    if (!this.ended) {
      this.attributes[key] = value;
    }
    return this;
  }

  setAttributes(attributes: SpanAttributes): Span {
    if (!this.ended) {
      Object.assign(this.attributes, attributes);
    }
    return this;
  }

  addEvent(name: string, attributes?: SpanAttributes): Span {
    if (!this.ended) {
      this.events.push({
        name,
        timestamp: Date.now(),
        attributes,
      });
    }
    return this;
  }

  addLink(context: SpanContext, attributes?: SpanAttributes): Span {
    if (!this.ended) {
      this.links.push({
        traceId: context.traceId,
        spanId: context.spanId,
        attributes,
      });
    }
    return this;
  }

  setStatus(code: SpanStatusCode, message?: string): Span {
    if (!this.ended) {
      this.statusCode = code;
      this.statusMessage = message;
    }
    return this;
  }

  recordException(error: Error): Span {
    this.addEvent('exception', {
      'exception.type': error.name,
      'exception.message': error.message,
      'exception.stacktrace': error.stack ?? '',
    });
    this.setStatus(SpanStatusCode.ERROR, error.message);
    return this;
  }

  end(): void {
    if (this.ended) return;
    this.ended = true;
    this.endTime = Date.now();

    // Add to buffer for export
    const spanData: SpanData = {
      name: this.name,
      context: this.context,
      parentContext: this.parentContext,
      kind: this.kind,
      startTime: this.startTime,
      endTime: this.endTime,
      duration: this.endTime - this.startTime,
      attributes: this.attributes,
      events: this.events,
      links: this.links,
      statusCode: this.statusCode,
      statusMessage: this.statusMessage,
      serviceName: globalConfig.serviceName,
    };

    spanBuffer.push(spanData);

    // Auto-flush if buffer is full
    if (spanBuffer.length >= (globalConfig.maxSpanBuffer ?? 100)) {
      void flushSpans();
    }
  }

  isRecording(): boolean {
    return !this.ended;
  }
}

/**
 * Real tracer implementation
 */
class RealTracer implements Tracer {
  private samplingRate: number;

  constructor(samplingRate: number = 1.0) {
    this.samplingRate = samplingRate;
  }

  private shouldSample(): boolean {
    return Math.random() < this.samplingRate;
  }

  startSpan(name: string, options?: SpanOptions): Span {
    if (!this.shouldSample()) {
      return NOOP_SPAN;
    }
    return new RealSpan(name, options);
  }

  startSpanWithParent(name: string, parent: SpanContext, options?: SpanOptions): Span {
    if (!this.shouldSample()) {
      return NOOP_SPAN;
    }
    return new RealSpan(name, { ...options, parent });
  }
}

/**
 * Real metrics collector implementation
 */
class RealMetricsCollector implements MetricsCollector {
  incrementCounter(name: string, value: number = 1, labels: Record<string, string> = {}): void {
    metricBuffer.push({
      name,
      type: 'counter',
      value,
      timestamp: Date.now(),
      labels,
      serviceName: globalConfig.serviceName,
    });

    this.checkBufferFlush();
  }

  recordHistogram(name: string, value: number, labels: Record<string, string> = {}): void {
    const labelKey = `${name}:${JSON.stringify(labels)}`;
    let state = histogramState.get(labelKey);

    if (!state) {
      state = {
        buckets: [...DEFAULT_LATENCY_BUCKETS],
        counts: new Array(DEFAULT_LATENCY_BUCKETS.length + 1).fill(0),
        sum: 0,
        count: 0,
        labels,
      };
      histogramState.set(labelKey, state);
    }

    // Find bucket and increment
    let bucketIndex = state.buckets.length;
    for (let i = 0; i < state.buckets.length; i++) {
      const bucket = state.buckets[i];
      if (bucket !== undefined && value <= bucket) {
        bucketIndex = i;
        break;
      }
    }
    // Increment the count at the bucket index
    const currentCount = state.counts[bucketIndex] ?? 0;
    state.counts[bucketIndex] = currentCount + 1;
    state.sum += value;
    state.count++;

    // Record raw value for export
    metricBuffer.push({
      name,
      type: 'histogram',
      value,
      timestamp: Date.now(),
      labels,
      serviceName: globalConfig.serviceName,
      histogram: {
        buckets: state.buckets,
        counts: [...state.counts],
        sum: state.sum,
        count: state.count,
      },
    });

    this.checkBufferFlush();
  }

  setGauge(name: string, value: number, labels: Record<string, string> = {}): void {
    metricBuffer.push({
      name,
      type: 'gauge',
      value,
      timestamp: Date.now(),
      labels,
      serviceName: globalConfig.serviceName,
    });

    this.checkBufferFlush();
  }

  recordQuery(queryType: string, durationMs: number, labels: Record<string, string> = {}): void {
    this.incrementCounter('graphdb.query.count', 1, { ...labels, query_type: queryType });
    this.recordHistogram('graphdb.query.duration_ms', durationMs, { ...labels, query_type: queryType });
  }

  recordTraversal(depth: number, durationMs: number, labels: Record<string, string> = {}): void {
    this.incrementCounter('graphdb.traversal.count', 1, { ...labels, depth: String(depth) });
    this.recordHistogram('graphdb.traversal.duration_ms', durationMs, { ...labels, depth: String(depth) });
  }

  recordWrite(entityCount: number, durationMs: number, labels: Record<string, string> = {}): void {
    this.incrementCounter('graphdb.write.count', 1, labels);
    this.incrementCounter('graphdb.write.entities', entityCount, labels);
    this.recordHistogram('graphdb.write.duration_ms', durationMs, labels);
  }

  private checkBufferFlush(): void {
    if (metricBuffer.length >= (globalConfig.maxMetricBuffer ?? 1000)) {
      void this.flush();
    }
  }

  async flush(): Promise<void> {
    if (metricBuffer.length === 0) return;

    const toExport = [...metricBuffer];
    metricBuffer.length = 0;

    if (globalConfig.metricExporter) {
      try {
        await globalConfig.metricExporter(toExport);
      } catch (error) {
        // Re-add metrics on export failure (best effort)
        console.error('Failed to export metrics:', error);
      }
    }
  }

  getSnapshot(): MetricData[] {
    return [...metricBuffer];
  }
}

// ============================================================================
// Module State
// ============================================================================

let activeTracer: Tracer = NOOP_TRACER;
let activeMetrics: MetricsCollector = NOOP_METRICS;

// ============================================================================
// Public API
// ============================================================================

/**
 * Initialize OpenTelemetry with the given configuration.
 *
 * This should be called once at worker startup. If called multiple times,
 * the new configuration will replace the previous one.
 *
 * @param config - OpenTelemetry configuration
 *
 * @example
 * ```typescript
 * initOtel({
 *   enabled: env.OTEL_ENABLED === 'true',
 *   serviceName: 'graphdb',
 *   samplingRate: 0.1, // Sample 10% of traces
 *   spanExporter: async (spans) => {
 *     await fetch(env.OTEL_ENDPOINT, {
 *       method: 'POST',
 *       headers: { 'Content-Type': 'application/json' },
 *       body: JSON.stringify({ resourceSpans: spans }),
 *     });
 *   },
 * });
 * ```
 */
export function initOtel(config: OtelConfig): void {
  globalConfig = {
    ...config,
    samplingRate: config.samplingRate ?? 1.0,
    maxSpanBuffer: config.maxSpanBuffer ?? 100,
    maxMetricBuffer: config.maxMetricBuffer ?? 1000,
  };

  if (config.enabled) {
    activeTracer = new RealTracer(globalConfig.samplingRate);
    activeMetrics = new RealMetricsCollector();
  } else {
    activeTracer = NOOP_TRACER;
    activeMetrics = NOOP_METRICS;
  }
}

/**
 * Get the current OpenTelemetry configuration.
 *
 * @returns Current configuration (copy)
 */
export function getOtelConfig(): OtelConfig {
  return { ...globalConfig };
}

/**
 * Check if OpenTelemetry is currently enabled.
 *
 * @returns true if tracing/metrics collection is active
 */
export function isOtelEnabled(): boolean {
  return globalConfig.enabled;
}

/**
 * Get the active tracer for creating spans.
 *
 * Returns a no-op tracer if OpenTelemetry is disabled.
 *
 * @returns Tracer instance
 */
export function getTracer(): Tracer {
  return activeTracer;
}

/**
 * Get the active metrics collector.
 *
 * Returns a no-op collector if OpenTelemetry is disabled.
 *
 * @returns MetricsCollector instance
 */
export function getMetrics(): MetricsCollector {
  return activeMetrics;
}

/**
 * Convenience export for the tracer (can be imported directly).
 *
 * @example
 * ```typescript
 * import { tracer } from './observability/otel';
 *
 * const span = tracer.startSpan('myOperation');
 * // ...
 * span.end();
 * ```
 */
export const tracer: Tracer = {
  startSpan: (name: string, options?: SpanOptions) => activeTracer.startSpan(name, options),
  startSpanWithParent: (name: string, parent: SpanContext, options?: SpanOptions) =>
    activeTracer.startSpanWithParent(name, parent, options),
};

/**
 * Convenience export for metrics (can be imported directly).
 *
 * @example
 * ```typescript
 * import { metrics } from './observability/otel';
 *
 * metrics.recordQuery('traverse', 150);
 * ```
 */
export const metrics: MetricsCollector = {
  incrementCounter: (name: string, value?: number, labels?: Record<string, string>) =>
    activeMetrics.incrementCounter(name, value, labels),
  recordHistogram: (name: string, value: number, labels?: Record<string, string>) =>
    activeMetrics.recordHistogram(name, value, labels),
  setGauge: (name: string, value: number, labels?: Record<string, string>) =>
    activeMetrics.setGauge(name, value, labels),
  recordQuery: (queryType: string, durationMs: number, labels?: Record<string, string>) =>
    activeMetrics.recordQuery(queryType, durationMs, labels),
  recordTraversal: (depth: number, durationMs: number, labels?: Record<string, string>) =>
    activeMetrics.recordTraversal(depth, durationMs, labels),
  recordWrite: (entityCount: number, durationMs: number, labels?: Record<string, string>) =>
    activeMetrics.recordWrite(entityCount, durationMs, labels),
  flush: () => activeMetrics.flush(),
  getSnapshot: () => activeMetrics.getSnapshot(),
};

/**
 * Flush all buffered spans to the exporter.
 *
 * Call this before the Worker request ends to ensure spans are exported.
 *
 * @returns Promise that resolves when export is complete
 */
export async function flushSpans(): Promise<void> {
  if (spanBuffer.length === 0) return;

  const toExport = [...spanBuffer];
  spanBuffer.length = 0;

  if (globalConfig.spanExporter) {
    try {
      await globalConfig.spanExporter(toExport);
    } catch (error) {
      // Log but don't throw - observability shouldn't break the app
      console.error('Failed to export spans:', error);
    }
  }
}

/**
 * Flush all buffered data (spans and metrics).
 *
 * Call this at the end of a request to ensure all telemetry is exported.
 *
 * @returns Promise that resolves when all exports are complete
 */
export async function flushAll(): Promise<void> {
  await Promise.all([flushSpans(), activeMetrics.flush()]);
}

/**
 * Get the current span buffer for inspection/testing.
 *
 * @returns Copy of current span buffer
 */
export function getSpanBuffer(): SpanData[] {
  return [...spanBuffer];
}

/**
 * Clear all buffered data without exporting.
 *
 * Useful for testing or error recovery.
 */
export function clearBuffers(): void {
  spanBuffer.length = 0;
  metricBuffer.length = 0;
  histogramState.clear();
}

// ============================================================================
// Trace Context Propagation (W3C format)
// ============================================================================

/**
 * Parse a W3C traceparent header into a SpanContext.
 *
 * Format: {version}-{trace-id}-{parent-id}-{trace-flags}
 * Example: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
 *
 * @param traceparent - The traceparent header value
 * @returns SpanContext or null if invalid
 */
export function parseTraceparent(traceparent: string): SpanContext | null {
  const parts = traceparent.split('-');
  if (parts.length !== 4) return null;

  const version = parts[0];
  const traceId = parts[1];
  const spanId = parts[2];
  const flags = parts[3];

  // Ensure all parts are defined (TypeScript type narrowing)
  if (!version || !traceId || !spanId || !flags) return null;

  // Version must be 00
  if (version !== '00') return null;

  // Trace ID must be 32 hex chars and not all zeros
  if (!/^[0-9a-f]{32}$/i.test(traceId) || /^0+$/.test(traceId)) return null;

  // Span ID must be 16 hex chars and not all zeros
  if (!/^[0-9a-f]{16}$/i.test(spanId) || /^0+$/.test(spanId)) return null;

  // Flags must be 2 hex chars
  if (!/^[0-9a-f]{2}$/i.test(flags)) return null;

  return {
    traceId: traceId.toLowerCase(),
    spanId: spanId.toLowerCase(),
    traceFlags: parseInt(flags, 16),
  };
}

/**
 * Format a SpanContext as a W3C traceparent header.
 *
 * @param context - The span context to format
 * @returns traceparent header value
 */
export function formatTraceparent(context: SpanContext): string {
  const flags = context.traceFlags.toString(16).padStart(2, '0');
  return `00-${context.traceId}-${context.spanId}-${flags}`;
}

/**
 * Extract trace context from incoming request headers.
 *
 * @param headers - Request headers
 * @returns SpanContext or null if not present/invalid
 */
export function extractTraceContext(headers: Headers): SpanContext | null {
  const traceparent = headers.get('traceparent');
  if (!traceparent) return null;
  return parseTraceparent(traceparent);
}

/**
 * Inject trace context into outgoing request headers.
 *
 * @param headers - Headers object to modify
 * @param context - Span context to inject
 */
export function injectTraceContext(headers: Headers, context: SpanContext): void {
  headers.set('traceparent', formatTraceparent(context));
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Wrap an async function with a span.
 *
 * Automatically records duration and exceptions.
 *
 * @param name - Span name
 * @param fn - Function to wrap
 * @param options - Span options
 * @returns Result of the wrapped function
 *
 * @example
 * ```typescript
 * const result = await withSpan('fetchData', async () => {
 *   return await fetch(url);
 * });
 * ```
 */
export async function withSpan<T>(
  name: string,
  fn: (span: Span) => Promise<T>,
  options?: SpanOptions
): Promise<T> {
  const span = tracer.startSpan(name, options);
  try {
    const result = await fn(span);
    span.setStatus(SpanStatusCode.OK);
    return result;
  } catch (error) {
    if (error instanceof Error) {
      span.recordException(error);
    } else {
      span.setStatus(SpanStatusCode.ERROR, String(error));
    }
    throw error;
  } finally {
    span.end();
  }
}

/**
 * Wrap a synchronous function with a span.
 *
 * @param name - Span name
 * @param fn - Function to wrap
 * @param options - Span options
 * @returns Result of the wrapped function
 */
export function withSpanSync<T>(name: string, fn: (span: Span) => T, options?: SpanOptions): T {
  const span = tracer.startSpan(name, options);
  try {
    const result = fn(span);
    span.setStatus(SpanStatusCode.OK);
    return result;
  } catch (error) {
    if (error instanceof Error) {
      span.recordException(error);
    } else {
      span.setStatus(SpanStatusCode.ERROR, String(error));
    }
    throw error;
  } finally {
    span.end();
  }
}
