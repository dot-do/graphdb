/**
 * Observability module exports
 *
 * Provides unified access to logging and OpenTelemetry tracing/metrics.
 */

// Logger exports
export {
  createLogger,
  configureLogging,
  getLogConfig,
  type Logger,
  type LogLevel,
  type LogConfig,
} from './logger.js';

// OpenTelemetry exports
export {
  // Configuration
  initOtel,
  getOtelConfig,
  isOtelEnabled,
  type OtelConfig,

  // Tracer and span types
  getTracer,
  tracer,
  type Tracer,
  type Span,
  type SpanOptions,
  type SpanContext,
  type SpanData,
  type SpanEvent,
  type SpanLink,
  type SpanAttributes,
  type AttributeValue,
  SpanStatusCode,
  SpanKind,

  // Metrics
  getMetrics,
  metrics,
  type MetricsCollector,
  type MetricData,
  type SpanExporter,
  type MetricExporter,

  // Flush and buffer management
  flushSpans,
  flushAll,
  getSpanBuffer,
  clearBuffers,

  // Trace context propagation (W3C format)
  parseTraceparent,
  formatTraceparent,
  extractTraceContext,
  injectTraceContext,

  // Helper functions
  withSpan,
  withSpanSync,
} from './otel.js';
