import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  initOtel,
  getOtelConfig,
  isOtelEnabled,
  getTracer,
  getMetrics,
  tracer,
  metrics,
  flushSpans,
  flushAll,
  getSpanBuffer,
  clearBuffers,
  parseTraceparent,
  formatTraceparent,
  extractTraceContext,
  injectTraceContext,
  withSpan,
  withSpanSync,
  SpanStatusCode,
  SpanKind,
  type SpanData,
  type MetricData,
  type SpanContext,
} from '../../src/observability/otel';

describe('OpenTelemetry Integration', () => {
  beforeEach(() => {
    // Reset to disabled state before each test
    clearBuffers();
    initOtel({
      enabled: false,
      serviceName: 'test-service',
    });
  });

  afterEach(() => {
    clearBuffers();
  });

  describe('initOtel', () => {
    it('should initialize with disabled state by default', () => {
      initOtel({
        enabled: false,
        serviceName: 'graphdb',
      });

      expect(isOtelEnabled()).toBe(false);
    });

    it('should enable tracing when configured', () => {
      initOtel({
        enabled: true,
        serviceName: 'graphdb',
      });

      expect(isOtelEnabled()).toBe(true);
    });

    it('should store configuration values', () => {
      initOtel({
        enabled: true,
        serviceName: 'my-service',
        samplingRate: 0.5,
        maxSpanBuffer: 50,
        maxMetricBuffer: 500,
      });

      const config = getOtelConfig();
      expect(config.enabled).toBe(true);
      expect(config.serviceName).toBe('my-service');
      expect(config.samplingRate).toBe(0.5);
      expect(config.maxSpanBuffer).toBe(50);
      expect(config.maxMetricBuffer).toBe(500);
    });

    it('should use default values for optional config', () => {
      initOtel({
        enabled: true,
        serviceName: 'graphdb',
      });

      const config = getOtelConfig();
      expect(config.samplingRate).toBe(1.0);
      expect(config.maxSpanBuffer).toBe(100);
      expect(config.maxMetricBuffer).toBe(1000);
    });
  });

  describe('Tracer - Disabled', () => {
    beforeEach(() => {
      initOtel({
        enabled: false,
        serviceName: 'test-service',
      });
    });

    it('should return no-op span when disabled', () => {
      const span = tracer.startSpan('test-operation');

      expect(span).toBeDefined();
      expect(span.isRecording()).toBe(false);

      // Operations should not throw
      span.setAttribute('key', 'value');
      span.setAttributes({ key1: 'value1', key2: 42 });
      span.addEvent('event');
      span.setStatus(SpanStatusCode.OK);
      span.end();
    });

    it('should return zero context for no-op spans', () => {
      const span = tracer.startSpan('test');
      const context = span.getContext();

      expect(context.traceId).toBe('00000000000000000000000000000000');
      expect(context.spanId).toBe('0000000000000000');
      expect(context.traceFlags).toBe(0);
    });

    it('should not add spans to buffer when disabled', () => {
      const span = tracer.startSpan('test');
      span.end();

      expect(getSpanBuffer()).toHaveLength(0);
    });
  });

  describe('Tracer - Enabled', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'test-service',
      });
    });

    it('should create real span when enabled', () => {
      const span = tracer.startSpan('test-operation');

      expect(span).toBeDefined();
      expect(span.isRecording()).toBe(true);
    });

    it('should generate valid trace and span IDs', () => {
      const span = tracer.startSpan('test');
      const context = span.getContext();

      // Trace ID should be 32 hex characters
      expect(context.traceId).toMatch(/^[0-9a-f]{32}$/);
      // Span ID should be 16 hex characters
      expect(context.spanId).toMatch(/^[0-9a-f]{16}$/);
      // Trace flags should indicate sampled
      expect(context.traceFlags).toBe(1);

      span.end();
    });

    it('should add span to buffer when ended', () => {
      const span = tracer.startSpan('test-operation');
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer).toHaveLength(1);
      expect(buffer[0].name).toBe('test-operation');
    });

    it('should record span attributes', () => {
      const span = tracer.startSpan('query');
      span.setAttribute('query.type', 'traverse');
      span.setAttributes({ 'result.count': 42, 'cached': true });
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].attributes).toEqual({
        'query.type': 'traverse',
        'result.count': 42,
        'cached': true,
      });
    });

    it('should record span events', () => {
      const span = tracer.startSpan('operation');
      span.addEvent('cache-hit', { key: 'users:123' });
      span.addEvent('db-query');
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].events).toHaveLength(2);
      expect(buffer[0].events[0].name).toBe('cache-hit');
      expect(buffer[0].events[0].attributes).toEqual({ key: 'users:123' });
      expect(buffer[0].events[1].name).toBe('db-query');
    });

    it('should record span status', () => {
      const span = tracer.startSpan('operation');
      span.setStatus(SpanStatusCode.ERROR, 'Something went wrong');
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].statusCode).toBe(SpanStatusCode.ERROR);
      expect(buffer[0].statusMessage).toBe('Something went wrong');
    });

    it('should record exceptions', () => {
      const span = tracer.startSpan('operation');
      const error = new Error('Test error');
      span.recordException(error);
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].statusCode).toBe(SpanStatusCode.ERROR);
      expect(buffer[0].events).toHaveLength(1);
      expect(buffer[0].events[0].name).toBe('exception');
      expect(buffer[0].events[0].attributes?.['exception.message']).toBe('Test error');
    });

    it('should calculate duration correctly', async () => {
      const span = tracer.startSpan('operation');

      // Small delay to ensure measurable duration
      await new Promise((resolve) => setTimeout(resolve, 10));

      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].duration).toBeGreaterThanOrEqual(10);
      expect(buffer[0].endTime).toBeGreaterThan(buffer[0].startTime);
    });

    it('should not record attributes after span ends', () => {
      const span = tracer.startSpan('operation');
      span.setAttribute('before', true);
      span.end();
      span.setAttribute('after', true);

      const buffer = getSpanBuffer();
      expect(buffer[0].attributes).toEqual({ before: true });
    });

    it('should support span options', () => {
      const span = tracer.startSpan('operation', {
        kind: SpanKind.CLIENT,
        attributes: { 'initial.attr': 'value' },
      });
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].kind).toBe(SpanKind.CLIENT);
      expect(buffer[0].attributes).toEqual({ 'initial.attr': 'value' });
    });

    it('should support parent span context', () => {
      const parentContext: SpanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
      };

      const span = tracer.startSpanWithParent('child-operation', parentContext);
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].context.traceId).toBe(parentContext.traceId);
      expect(buffer[0].parentContext).toEqual(parentContext);
    });

    it('should include service name in span data', () => {
      const span = tracer.startSpan('operation');
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].serviceName).toBe('test-service');
    });
  });

  describe('Span Sampling', () => {
    it('should sample all spans at 1.0 rate', () => {
      initOtel({
        enabled: true,
        serviceName: 'test',
        samplingRate: 1.0,
      });

      // Create multiple spans
      for (let i = 0; i < 10; i++) {
        const span = tracer.startSpan(`span-${i}`);
        span.end();
      }

      expect(getSpanBuffer()).toHaveLength(10);
    });

    it('should sample no spans at 0.0 rate', () => {
      initOtel({
        enabled: true,
        serviceName: 'test',
        samplingRate: 0.0,
      });

      // Create multiple spans
      for (let i = 0; i < 10; i++) {
        const span = tracer.startSpan(`span-${i}`);
        span.end();
      }

      expect(getSpanBuffer()).toHaveLength(0);
    });
  });

  describe('Metrics - Disabled', () => {
    beforeEach(() => {
      initOtel({
        enabled: false,
        serviceName: 'test-service',
      });
    });

    it('should not throw when recording metrics while disabled', () => {
      expect(() => {
        metrics.incrementCounter('test.counter');
        metrics.recordHistogram('test.histogram', 100);
        metrics.setGauge('test.gauge', 50);
        metrics.recordQuery('traverse', 150);
        metrics.recordTraversal(3, 200);
        metrics.recordWrite(10, 50);
      }).not.toThrow();
    });

    it('should return empty snapshot when disabled', () => {
      metrics.incrementCounter('test.counter');
      expect(metrics.getSnapshot()).toEqual([]);
    });
  });

  describe('Metrics - Enabled', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'test-service',
      });
    });

    it('should record counter metrics', () => {
      metrics.incrementCounter('test.counter', 5, { label: 'value' });

      const snapshot = metrics.getSnapshot();
      expect(snapshot).toHaveLength(1);
      expect(snapshot[0].name).toBe('test.counter');
      expect(snapshot[0].type).toBe('counter');
      expect(snapshot[0].value).toBe(5);
      expect(snapshot[0].labels).toEqual({ label: 'value' });
    });

    it('should record histogram metrics', () => {
      metrics.recordHistogram('test.latency', 150, { endpoint: '/api' });

      const snapshot = metrics.getSnapshot();
      expect(snapshot).toHaveLength(1);
      expect(snapshot[0].name).toBe('test.latency');
      expect(snapshot[0].type).toBe('histogram');
      expect(snapshot[0].value).toBe(150);
      expect(snapshot[0].histogram).toBeDefined();
      expect(snapshot[0].histogram?.sum).toBe(150);
      expect(snapshot[0].histogram?.count).toBe(1);
    });

    it('should record gauge metrics', () => {
      metrics.setGauge('active.connections', 42, { region: 'us-east' });

      const snapshot = metrics.getSnapshot();
      expect(snapshot).toHaveLength(1);
      expect(snapshot[0].name).toBe('active.connections');
      expect(snapshot[0].type).toBe('gauge');
      expect(snapshot[0].value).toBe(42);
    });

    it('should record query convenience metrics', () => {
      metrics.recordQuery('traverse', 150, { db: 'graph' });

      const snapshot = metrics.getSnapshot();
      // Should have counter and histogram
      expect(snapshot.length).toBeGreaterThanOrEqual(2);

      const counter = snapshot.find((m) => m.name === 'graphdb.query.count');
      const histogram = snapshot.find((m) => m.name === 'graphdb.query.duration_ms');

      expect(counter).toBeDefined();
      expect(counter?.labels?.query_type).toBe('traverse');
      expect(histogram).toBeDefined();
      expect(histogram?.value).toBe(150);
    });

    it('should record traversal convenience metrics', () => {
      metrics.recordTraversal(3, 200);

      const snapshot = metrics.getSnapshot();
      const counter = snapshot.find((m) => m.name === 'graphdb.traversal.count');
      const histogram = snapshot.find((m) => m.name === 'graphdb.traversal.duration_ms');

      expect(counter).toBeDefined();
      expect(counter?.labels?.depth).toBe('3');
      expect(histogram).toBeDefined();
    });

    it('should record write convenience metrics', () => {
      metrics.recordWrite(100, 50);

      const snapshot = metrics.getSnapshot();
      const countMetric = snapshot.find((m) => m.name === 'graphdb.write.count');
      const entityMetric = snapshot.find((m) => m.name === 'graphdb.write.entities');
      const durationMetric = snapshot.find((m) => m.name === 'graphdb.write.duration_ms');

      expect(countMetric).toBeDefined();
      expect(entityMetric?.value).toBe(100);
      expect(durationMetric?.value).toBe(50);
    });

    it('should include service name in metric data', () => {
      metrics.incrementCounter('test');

      const snapshot = metrics.getSnapshot();
      expect(snapshot[0].serviceName).toBe('test-service');
    });

    it('should include timestamp in metric data', () => {
      const before = Date.now();
      metrics.incrementCounter('test');
      const after = Date.now();

      const snapshot = metrics.getSnapshot();
      expect(snapshot[0].timestamp).toBeGreaterThanOrEqual(before);
      expect(snapshot[0].timestamp).toBeLessThanOrEqual(after);
    });
  });

  describe('Span Export', () => {
    it('should call span exporter when flushing', async () => {
      const exportedSpans: SpanData[] = [];
      const exporter = vi.fn(async (spans: SpanData[]) => {
        exportedSpans.push(...spans);
      });

      initOtel({
        enabled: true,
        serviceName: 'test',
        spanExporter: exporter,
      });

      const span = tracer.startSpan('test');
      span.end();

      await flushSpans();

      expect(exporter).toHaveBeenCalledTimes(1);
      expect(exportedSpans).toHaveLength(1);
      expect(exportedSpans[0].name).toBe('test');
    });

    it('should clear buffer after successful export', async () => {
      initOtel({
        enabled: true,
        serviceName: 'test',
        spanExporter: async () => {},
      });

      const span = tracer.startSpan('test');
      span.end();
      expect(getSpanBuffer()).toHaveLength(1);

      await flushSpans();

      expect(getSpanBuffer()).toHaveLength(0);
    });

    it('should handle export errors gracefully', async () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      initOtel({
        enabled: true,
        serviceName: 'test',
        spanExporter: async () => {
          throw new Error('Export failed');
        },
      });

      const span = tracer.startSpan('test');
      span.end();

      // Should not throw
      await expect(flushSpans()).resolves.not.toThrow();

      consoleSpy.mockRestore();
    });
  });

  describe('Metric Export', () => {
    it('should call metric exporter when flushing', async () => {
      const exportedMetrics: MetricData[] = [];
      const exporter = vi.fn(async (data: MetricData[]) => {
        exportedMetrics.push(...data);
      });

      initOtel({
        enabled: true,
        serviceName: 'test',
        metricExporter: exporter,
      });

      metrics.incrementCounter('test.counter', 1);

      await metrics.flush();

      expect(exporter).toHaveBeenCalled();
      expect(exportedMetrics).toHaveLength(1);
    });

    it('should flush all data with flushAll', async () => {
      const spanExporter = vi.fn(async () => {});
      const metricExporter = vi.fn(async () => {});

      initOtel({
        enabled: true,
        serviceName: 'test',
        spanExporter,
        metricExporter,
      });

      tracer.startSpan('test').end();
      metrics.incrementCounter('test');

      await flushAll();

      expect(spanExporter).toHaveBeenCalled();
      expect(metricExporter).toHaveBeenCalled();
    });
  });

  describe('W3C Trace Context Propagation', () => {
    it('should parse valid traceparent header', () => {
      const traceparent = '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01';
      const context = parseTraceparent(traceparent);

      expect(context).not.toBeNull();
      expect(context?.traceId).toBe('0af7651916cd43dd8448eb211c80319c');
      expect(context?.spanId).toBe('b7ad6b7169203331');
      expect(context?.traceFlags).toBe(1);
    });

    it('should return null for invalid traceparent', () => {
      expect(parseTraceparent('invalid')).toBeNull();
      expect(parseTraceparent('00-short-short-01')).toBeNull();
      expect(parseTraceparent('01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')).toBeNull(); // wrong version
      expect(parseTraceparent('00-00000000000000000000000000000000-b7ad6b7169203331-01')).toBeNull(); // all zero trace
      expect(parseTraceparent('00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01')).toBeNull(); // all zero span
    });

    it('should format span context as traceparent', () => {
      const context: SpanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
      };

      const traceparent = formatTraceparent(context);

      expect(traceparent).toBe('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
    });

    it('should extract trace context from headers', () => {
      const headers = new Headers();
      headers.set('traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');

      const context = extractTraceContext(headers);

      expect(context).not.toBeNull();
      expect(context?.traceId).toBe('0af7651916cd43dd8448eb211c80319c');
    });

    it('should return null when traceparent header missing', () => {
      const headers = new Headers();
      const context = extractTraceContext(headers);

      expect(context).toBeNull();
    });

    it('should inject trace context into headers', () => {
      const headers = new Headers();
      const context: SpanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
      };

      injectTraceContext(headers, context);

      expect(headers.get('traceparent')).toBe(
        '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01'
      );
    });
  });

  describe('withSpan helper', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'test',
      });
    });

    it('should wrap async function with span', async () => {
      const result = await withSpan('async-op', async (span) => {
        span.setAttribute('custom', 'value');
        return 'success';
      });

      expect(result).toBe('success');

      const buffer = getSpanBuffer();
      expect(buffer).toHaveLength(1);
      expect(buffer[0].name).toBe('async-op');
      expect(buffer[0].attributes.custom).toBe('value');
      expect(buffer[0].statusCode).toBe(SpanStatusCode.OK);
    });

    it('should record exception when async function throws', async () => {
      const error = new Error('Async error');

      await expect(
        withSpan('failing-op', async () => {
          throw error;
        })
      ).rejects.toThrow('Async error');

      const buffer = getSpanBuffer();
      expect(buffer[0].statusCode).toBe(SpanStatusCode.ERROR);
      expect(buffer[0].events[0].name).toBe('exception');
    });
  });

  describe('withSpanSync helper', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'test',
      });
    });

    it('should wrap sync function with span', () => {
      const result = withSpanSync('sync-op', (span) => {
        span.setAttribute('custom', 'value');
        return 42;
      });

      expect(result).toBe(42);

      const buffer = getSpanBuffer();
      expect(buffer).toHaveLength(1);
      expect(buffer[0].name).toBe('sync-op');
      expect(buffer[0].statusCode).toBe(SpanStatusCode.OK);
    });

    it('should record exception when sync function throws', () => {
      const error = new Error('Sync error');

      expect(() =>
        withSpanSync('failing-op', () => {
          throw error;
        })
      ).toThrow('Sync error');

      const buffer = getSpanBuffer();
      expect(buffer[0].statusCode).toBe(SpanStatusCode.ERROR);
    });
  });

  describe('Buffer Management', () => {
    it('should clear all buffers', () => {
      initOtel({
        enabled: true,
        serviceName: 'test',
      });

      tracer.startSpan('span1').end();
      tracer.startSpan('span2').end();
      metrics.incrementCounter('counter');

      expect(getSpanBuffer().length).toBeGreaterThan(0);
      expect(metrics.getSnapshot().length).toBeGreaterThan(0);

      clearBuffers();

      expect(getSpanBuffer()).toHaveLength(0);
      expect(metrics.getSnapshot()).toHaveLength(0);
    });
  });

  describe('Span Links', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'test',
      });
    });

    it('should support adding span links', () => {
      const linkedContext: SpanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
      };

      const span = tracer.startSpan('operation');
      span.addLink(linkedContext, { 'link.reason': 'batch' });
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].links).toHaveLength(1);
      expect(buffer[0].links[0].traceId).toBe(linkedContext.traceId);
      expect(buffer[0].links[0].spanId).toBe(linkedContext.spanId);
      expect(buffer[0].links[0].attributes).toEqual({ 'link.reason': 'batch' });
    });

    it('should support links in span options', () => {
      const linkedContext: SpanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
      };

      const span = tracer.startSpan('operation', {
        links: [{ traceId: linkedContext.traceId, spanId: linkedContext.spanId }],
      });
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].links).toHaveLength(1);
    });
  });

  describe('Key Operations Spans', () => {
    beforeEach(() => {
      initOtel({
        enabled: true,
        serviceName: 'graphdb',
      });
    });

    it('should create span for query operation', () => {
      const span = tracer.startSpan('graphdb.query', {
        kind: SpanKind.INTERNAL,
        attributes: {
          'graphdb.query.type': 'traverse',
          'graphdb.query.shard': 'shard-001',
        },
      });
      span.setAttribute('graphdb.query.result_count', 42);
      span.setStatus(SpanStatusCode.OK);
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer).toHaveLength(1);
      expect(buffer[0].name).toBe('graphdb.query');
      expect(buffer[0].attributes['graphdb.query.type']).toBe('traverse');
      expect(buffer[0].attributes['graphdb.query.result_count']).toBe(42);
    });

    it('should create span for traversal operation', () => {
      const span = tracer.startSpan('graphdb.traversal', {
        kind: SpanKind.INTERNAL,
        attributes: {
          'graphdb.traversal.depth': 3,
          'graphdb.traversal.start_node': 'user:123',
        },
      });
      span.addEvent('traversal.level', { level: 1, nodes: 5 });
      span.addEvent('traversal.level', { level: 2, nodes: 12 });
      span.addEvent('traversal.level', { level: 3, nodes: 28 });
      span.setAttribute('graphdb.traversal.total_nodes', 45);
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].name).toBe('graphdb.traversal');
      expect(buffer[0].events).toHaveLength(3);
      expect(buffer[0].attributes['graphdb.traversal.total_nodes']).toBe(45);
    });

    it('should create span for write operation', () => {
      const span = tracer.startSpan('graphdb.write', {
        kind: SpanKind.INTERNAL,
        attributes: {
          'graphdb.write.batch_size': 100,
          'graphdb.write.shard': 'shard-002',
        },
      });
      span.setAttribute('graphdb.write.entities_written', 100);
      span.setStatus(SpanStatusCode.OK);
      span.end();

      const buffer = getSpanBuffer();
      expect(buffer[0].name).toBe('graphdb.write');
      expect(buffer[0].attributes['graphdb.write.batch_size']).toBe(100);
      expect(buffer[0].statusCode).toBe(SpanStatusCode.OK);
    });
  });
});
