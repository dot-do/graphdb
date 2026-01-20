/**
 * Tests for RPC retry logic with exponential backoff
 *
 * Tests cover:
 * - Idempotent method detection
 * - Transient error detection
 * - Exponential backoff delay calculation
 * - Retry wrapper behavior
 * - Integration with RPC client
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  isIdempotentMethod,
  isTransientError,
  shouldRetry,
  calculateRetryDelay,
  withRetry,
  withRpcRetry,
  createRetryConfig,
  createLowLatencyRetryConfig,
  createHighReliabilityRetryConfig,
  DEFAULT_RETRY_CONFIG,
  type RetryConfig,
  type RetryResult,
} from '../../src/rpc/retry.js';
import type { RpcMethodName } from '../../src/rpc/types.js';

// ============================================================================
// Idempotent Method Detection
// ============================================================================

describe('isIdempotentMethod', () => {
  describe('read operations (idempotent)', () => {
    const idempotentMethods: RpcMethodName[] = [
      'getEntity',
      'traverse',
      'reverseTraverse',
      'pathTraverse',
      'query',
      'batchGet',
    ];

    it.each(idempotentMethods)('should return true for %s', (method) => {
      expect(isIdempotentMethod(method)).toBe(true);
    });
  });

  describe('write operations (non-idempotent)', () => {
    const nonIdempotentMethods: RpcMethodName[] = [
      'createEntity',
      'updateEntity',
      'deleteEntity',
      'batchCreate',
      'batchExecute',
    ];

    it.each(nonIdempotentMethods)('should return false for %s', (method) => {
      expect(isIdempotentMethod(method)).toBe(false);
    });
  });
});

// ============================================================================
// Transient Error Detection
// ============================================================================

describe('isTransientError', () => {
  describe('network errors', () => {
    const networkErrors = [
      'Network error',
      'ECONNRESET',
      'ECONNREFUSED',
      'ETIMEDOUT',
      'ENETUNREACH',
      'EAI_AGAIN',
      'fetch failed',
      'Failed to fetch',
      'connection refused',
      'connection reset',
      'socket hang up',
    ];

    it.each(networkErrors)('should detect "%s" as transient', (message) => {
      expect(isTransientError(new Error(message))).toBe(true);
    });
  });

  describe('timeout errors', () => {
    const timeoutErrors = [
      'Request timeout',
      'timed out',
      'Timeout exceeded',
      'deadline exceeded',
    ];

    it.each(timeoutErrors)('should detect "%s" as transient', (message) => {
      expect(isTransientError(new Error(message))).toBe(true);
    });
  });

  describe('server overload errors', () => {
    const overloadErrors = [
      'Service unavailable',
      'HTTP 503',
      'temporarily unavailable',
      'Server overloaded',
      'Too many requests',
      'Rate limit exceeded',
    ];

    it.each(overloadErrors)('should detect "%s" as transient', (message) => {
      expect(isTransientError(new Error(message))).toBe(true);
    });
  });

  describe('WebSocket errors', () => {
    const wsErrors = [
      'WebSocket not connected',
      'Connection closed',
      'Not connected',
    ];

    it.each(wsErrors)('should detect "%s" as transient', (message) => {
      expect(isTransientError(new Error(message))).toBe(true);
    });
  });

  describe('HTTP status codes in error message', () => {
    const transientStatusCodes = [408, 429, 502, 503, 504];

    it.each(transientStatusCodes)('should detect status %d as transient', (status) => {
      expect(isTransientError(new Error(`HTTP error ${status}`))).toBe(true);
    });

    it('should not detect 400 as transient', () => {
      expect(isTransientError(new Error('HTTP error 400'))).toBe(false);
    });

    it('should not detect 404 as transient', () => {
      expect(isTransientError(new Error('HTTP error 404'))).toBe(false);
    });

    it('should not detect 500 as transient', () => {
      expect(isTransientError(new Error('HTTP error 500'))).toBe(false);
    });
  });

  describe('error with code property', () => {
    it('should detect ECONNRESET code as transient', () => {
      const error = new Error('Connection reset') as Error & { code: string };
      error.code = 'ECONNRESET';
      expect(isTransientError(error)).toBe(true);
    });

    it('should detect ETIMEDOUT code as transient', () => {
      const error = new Error('Timeout') as Error & { code: string };
      error.code = 'ETIMEDOUT';
      expect(isTransientError(error)).toBe(true);
    });
  });

  describe('error with status property', () => {
    it('should detect status 503 as transient', () => {
      const error = new Error('Service unavailable') as Error & { status: number };
      error.status = 503;
      expect(isTransientError(error)).toBe(true);
    });

    it('should detect status 429 as transient', () => {
      const error = new Error('Rate limited') as Error & { status: number };
      error.status = 429;
      expect(isTransientError(error)).toBe(true);
    });
  });

  describe('non-transient errors', () => {
    const permanentErrors = [
      'Entity not found',
      'Invalid entity',
      'Permission denied',
      'Authentication failed',
      'Validation error',
      'Invalid request',
    ];

    it.each(permanentErrors)('should not detect "%s" as transient', (message) => {
      expect(isTransientError(new Error(message))).toBe(false);
    });
  });
});

// ============================================================================
// shouldRetry Combined Check
// ============================================================================

describe('shouldRetry', () => {
  it('should return true for idempotent method with transient error', () => {
    expect(shouldRetry('getEntity', new Error('Connection timeout'))).toBe(true);
    expect(shouldRetry('query', new Error('Service unavailable'))).toBe(true);
    expect(shouldRetry('batchGet', new Error('Network error'))).toBe(true);
  });

  it('should return false for non-idempotent method with transient error', () => {
    expect(shouldRetry('createEntity', new Error('Connection timeout'))).toBe(false);
    expect(shouldRetry('updateEntity', new Error('Service unavailable'))).toBe(false);
    expect(shouldRetry('deleteEntity', new Error('Network error'))).toBe(false);
  });

  it('should return false for idempotent method with non-transient error', () => {
    expect(shouldRetry('getEntity', new Error('Entity not found'))).toBe(false);
    expect(shouldRetry('query', new Error('Invalid query syntax'))).toBe(false);
  });

  it('should return false for non-idempotent method with non-transient error', () => {
    expect(shouldRetry('createEntity', new Error('Entity already exists'))).toBe(false);
  });
});

// ============================================================================
// Delay Calculation
// ============================================================================

describe('calculateRetryDelay', () => {
  const config: RetryConfig = {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
    jitterFactor: 0,
  };

  it('should return base delay for first attempt', () => {
    const delay = calculateRetryDelay(0, config);
    expect(delay).toBe(100);
  });

  it('should double delay for each attempt (exponential backoff)', () => {
    expect(calculateRetryDelay(1, config)).toBe(200);
    expect(calculateRetryDelay(2, config)).toBe(400);
    expect(calculateRetryDelay(3, config)).toBe(800);
  });

  it('should cap delay at maxDelayMs', () => {
    const configWithLowMax = { ...config, maxDelayMs: 300 };
    expect(calculateRetryDelay(0, configWithLowMax)).toBe(100);
    expect(calculateRetryDelay(1, configWithLowMax)).toBe(200);
    expect(calculateRetryDelay(2, configWithLowMax)).toBe(300); // Capped
    expect(calculateRetryDelay(3, configWithLowMax)).toBe(300); // Still capped
  });

  it('should add jitter when jitterFactor > 0', () => {
    const configWithJitter = { ...config, jitterFactor: 0.2 };

    // With 20% jitter, delay should be between baseDelay and baseDelay * 1.2
    const delays = Array.from({ length: 100 }, () => calculateRetryDelay(0, configWithJitter));

    const allWithinRange = delays.every((d) => d >= 100 && d <= 120);
    expect(allWithinRange).toBe(true);

    // With jitter, not all delays should be exactly the same
    const uniqueDelays = new Set(delays);
    expect(uniqueDelays.size).toBeGreaterThan(1);
  });
});

// ============================================================================
// withRetry Wrapper
// ============================================================================

describe('withRetry', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should return success on first attempt if no error', async () => {
    const operation = vi.fn().mockResolvedValue('success');

    const resultPromise = withRetry(operation, { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 1000, jitterFactor: 0 });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(true);
    expect(result.value).toBe('success');
    expect(result.attempts).toBe(1);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it('should retry on transient error and succeed', async () => {
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Connection timeout'))
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValue('success');

    const resultPromise = withRetry(operation, { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 1000, jitterFactor: 0 });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(true);
    expect(result.value).toBe('success');
    expect(result.attempts).toBe(3);
    expect(operation).toHaveBeenCalledTimes(3);
  });

  it('should fail after max retries exhausted', async () => {
    const operation = vi.fn().mockRejectedValue(new Error('Connection timeout'));

    const resultPromise = withRetry(operation, { maxRetries: 2, baseDelayMs: 100, maxDelayMs: 1000, jitterFactor: 0 });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(false);
    expect(result.error?.message).toBe('Connection timeout');
    expect(result.attempts).toBe(3); // Initial + 2 retries
    expect(operation).toHaveBeenCalledTimes(3);
  });

  it('should not retry non-transient errors', async () => {
    const operation = vi.fn().mockRejectedValue(new Error('Entity not found'));

    const resultPromise = withRetry(operation, { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 1000, jitterFactor: 0 });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(false);
    expect(result.error?.message).toBe('Entity not found');
    expect(result.attempts).toBe(1);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it('should call onRetry callback before each retry', async () => {
    const onRetry = vi.fn();
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Connection timeout'))
      .mockRejectedValueOnce(new Error('Network error'))
      .mockResolvedValue('success');

    const resultPromise = withRetry(operation, {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
      jitterFactor: 0,
      onRetry,
    });
    await vi.runAllTimersAsync();
    await resultPromise;

    expect(onRetry).toHaveBeenCalledTimes(2);
    expect(onRetry).toHaveBeenNthCalledWith(1, 1, expect.any(Error), 100);
    expect(onRetry).toHaveBeenNthCalledWith(2, 2, expect.any(Error), 200);
  });

  it('should respect custom isRetryable function', async () => {
    const customIsRetryable = vi.fn().mockReturnValue(true);
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Custom error'))
      .mockResolvedValue('success');

    const resultPromise = withRetry(operation, {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
      jitterFactor: 0,
      isRetryable: customIsRetryable,
    });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(true);
    expect(customIsRetryable).toHaveBeenCalled();
  });

  it('should track total time spent', async () => {
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Connection timeout'))
      .mockResolvedValue('success');

    const resultPromise = withRetry(operation, { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 1000, jitterFactor: 0 });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.totalTimeMs).toBeGreaterThanOrEqual(0);
  });
});

// ============================================================================
// withRpcRetry Wrapper
// ============================================================================

describe('withRpcRetry', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should retry idempotent methods on transient errors', async () => {
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Connection timeout'))
      .mockResolvedValue({ $id: 'test' });

    const resultPromise = withRpcRetry('getEntity', operation, {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
      jitterFactor: 0,
    });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(true);
    expect(result.attempts).toBe(2);
  });

  it('should not retry non-idempotent methods', async () => {
    const operation = vi.fn()
      .mockRejectedValueOnce(new Error('Connection timeout'))
      .mockResolvedValue(undefined);

    const resultPromise = withRpcRetry('createEntity', operation, {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
      jitterFactor: 0,
    });
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(false);
    expect(result.attempts).toBe(1);
    expect(operation).toHaveBeenCalledTimes(1);
  });

  it('should succeed on first attempt for non-idempotent methods', async () => {
    const operation = vi.fn().mockResolvedValue(undefined);

    const resultPromise = withRpcRetry('createEntity', operation);
    await vi.runAllTimersAsync();
    const result = await resultPromise;

    expect(result.success).toBe(true);
    expect(result.attempts).toBe(1);
  });

  describe('all idempotent methods', () => {
    const idempotentMethods: RpcMethodName[] = [
      'getEntity',
      'traverse',
      'reverseTraverse',
      'pathTraverse',
      'query',
      'batchGet',
    ];

    it.each(idempotentMethods)('should retry %s on transient error', async (method) => {
      const operation = vi.fn()
        .mockRejectedValueOnce(new Error('Service unavailable'))
        .mockResolvedValue([]);

      const resultPromise = withRpcRetry(method, operation, {
        maxRetries: 2,
        baseDelayMs: 50,
        maxDelayMs: 500,
        jitterFactor: 0,
      });
      await vi.runAllTimersAsync();
      const result = await resultPromise;

      expect(result.success).toBe(true);
      expect(result.attempts).toBe(2);
    });
  });
});

// ============================================================================
// Config Helpers
// ============================================================================

describe('createRetryConfig', () => {
  it('should return default config when no overrides provided', () => {
    const config = createRetryConfig();
    expect(config).toEqual(DEFAULT_RETRY_CONFIG);
  });

  it('should merge overrides with defaults', () => {
    const config = createRetryConfig({ maxRetries: 5, baseDelayMs: 200 });
    expect(config.maxRetries).toBe(5);
    expect(config.baseDelayMs).toBe(200);
    expect(config.maxDelayMs).toBe(DEFAULT_RETRY_CONFIG.maxDelayMs);
    expect(config.jitterFactor).toBe(DEFAULT_RETRY_CONFIG.jitterFactor);
  });
});

describe('createLowLatencyRetryConfig', () => {
  it('should use fewer retries and shorter delays', () => {
    const config = createLowLatencyRetryConfig();
    expect(config.maxRetries).toBe(2);
    expect(config.baseDelayMs).toBe(50);
    expect(config.maxDelayMs).toBe(500);
    expect(config.jitterFactor).toBe(0.1);
  });

  it('should allow overrides', () => {
    const config = createLowLatencyRetryConfig({ maxRetries: 1 });
    expect(config.maxRetries).toBe(1);
    expect(config.baseDelayMs).toBe(50);
  });
});

describe('createHighReliabilityRetryConfig', () => {
  it('should use more retries and longer delays', () => {
    const config = createHighReliabilityRetryConfig();
    expect(config.maxRetries).toBe(5);
    expect(config.baseDelayMs).toBe(200);
    expect(config.maxDelayMs).toBe(10000);
    expect(config.jitterFactor).toBe(0.3);
  });

  it('should allow overrides', () => {
    const config = createHighReliabilityRetryConfig({ maxRetries: 10 });
    expect(config.maxRetries).toBe(10);
    expect(config.baseDelayMs).toBe(200);
  });
});

// ============================================================================
// Default Config
// ============================================================================

describe('DEFAULT_RETRY_CONFIG', () => {
  it('should have reasonable defaults', () => {
    expect(DEFAULT_RETRY_CONFIG.maxRetries).toBe(3);
    expect(DEFAULT_RETRY_CONFIG.baseDelayMs).toBe(100);
    expect(DEFAULT_RETRY_CONFIG.maxDelayMs).toBe(5000);
    expect(DEFAULT_RETRY_CONFIG.jitterFactor).toBe(0.2);
  });
});
