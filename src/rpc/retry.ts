/**
 * RPC Retry Logic with Exponential Backoff
 *
 * Provides retry functionality for transient failures in RPC calls.
 * Only retries idempotent operations and transient errors (network, timeout, 503).
 *
 * @module rpc/retry
 */

import type { RpcMethodName } from './types.js';

// ============================================================================
// Retry Configuration Types
// ============================================================================

/**
 * Configuration for retry behavior.
 */
export interface RetryConfig {
  /**
   * Maximum number of retry attempts (not including initial attempt).
   * @default 3
   */
  maxRetries: number;

  /**
   * Base delay in milliseconds for exponential backoff.
   * @default 100
   */
  baseDelayMs: number;

  /**
   * Maximum delay in milliseconds between retries.
   * @default 5000
   */
  maxDelayMs: number;

  /**
   * Jitter factor (0-1) to add randomness to delays.
   * Helps prevent thundering herd issues.
   * @default 0.2
   */
  jitterFactor: number;

  /**
   * Request timeout in milliseconds.
   * If not specified, uses the default timeout from the client.
   */
  timeoutMs?: number;

  /**
   * Custom function to determine if an error is retryable.
   * If not provided, uses the default isTransientError check.
   */
  isRetryable?: (error: Error) => boolean;

  /**
   * Callback for retry events (useful for logging/monitoring).
   */
  onRetry?: (attempt: number, error: Error, delayMs: number) => void;
}

/**
 * Default retry configuration.
 */
export const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
  jitterFactor: 0.2,
};

/**
 * Result of a retried operation.
 */
export interface RetryResult<T> {
  /** The result value if successful */
  value?: T;
  /** The final error if all retries failed */
  error?: Error;
  /** Number of attempts made (1 = no retries) */
  attempts: number;
  /** Whether the operation succeeded */
  success: boolean;
  /** Total time spent including retries (ms) */
  totalTimeMs: number;
}

// ============================================================================
// Idempotent Method Detection
// ============================================================================

/**
 * Set of RPC methods that are safe to retry (idempotent).
 *
 * Read operations are always safe to retry.
 * Write operations (create, update, delete) are NOT safe to retry
 * as they could cause duplicate effects.
 */
const IDEMPOTENT_METHODS = new Set<RpcMethodName>([
  'getEntity',
  'traverse',
  'reverseTraverse',
  'pathTraverse',
  'query',
  'batchGet',
  // Note: createEntity, updateEntity, deleteEntity, batchCreate, batchExecute
  // are NOT included as they may not be idempotent
]);

/**
 * Check if an RPC method is idempotent and safe to retry.
 *
 * @param method - The RPC method name
 * @returns true if the method is safe to retry
 */
export function isIdempotentMethod(method: RpcMethodName): boolean {
  return IDEMPOTENT_METHODS.has(method);
}

// ============================================================================
// Transient Error Detection
// ============================================================================

/**
 * Error messages that indicate transient failures.
 * These are typically recoverable by retrying.
 */
const TRANSIENT_ERROR_PATTERNS = [
  // Network errors
  /network/i,
  /connection refused/i,
  /connection reset/i,
  /socket hang up/i,
  /ECONNRESET/i,
  /ECONNREFUSED/i,
  /ETIMEDOUT/i,
  /ENETUNREACH/i,
  /EAI_AGAIN/i,
  /fetch failed/i,
  /failed to fetch/i,

  // Timeout errors
  /timeout/i,
  /timed out/i,
  /request timeout/i,
  /deadline exceeded/i,

  // Server overload (503)
  /service unavailable/i,
  /503/i,
  /temporarily unavailable/i,
  /overloaded/i,
  /too many requests/i,
  /rate limit/i,

  // WebSocket errors
  /websocket not connected/i,
  /connection closed/i,
  /not connected/i,

  // R2/Storage transient errors
  /r2 internal error/i,
  /storage temporarily unavailable/i,
  /internal storage error/i,
];

/**
 * Check if an error is a transient failure that may succeed on retry.
 *
 * @param error - The error to check
 * @returns true if the error appears to be transient
 */
export function isTransientError(error: Error): boolean {
  const message = error.message.toLowerCase();

  // Check against known transient patterns
  for (const pattern of TRANSIENT_ERROR_PATTERNS) {
    if (pattern.test(error.message)) {
      return true;
    }
  }

  // Check for HTTP status codes in error message
  // 408 Request Timeout, 429 Too Many Requests, 502 Bad Gateway,
  // 503 Service Unavailable, 504 Gateway Timeout
  const statusMatch = message.match(/\b(408|429|502|503|504)\b/);
  if (statusMatch) {
    return true;
  }

  // Check if error has a status or code property
  const errorWithCode = error as Error & { code?: string | number; status?: number };
  if (errorWithCode.code) {
    const code = String(errorWithCode.code);
    if (
      code === 'ECONNRESET' ||
      code === 'ECONNREFUSED' ||
      code === 'ETIMEDOUT' ||
      code === 'ENETUNREACH' ||
      code === 'EAI_AGAIN' ||
      code === 'EPIPE'
    ) {
      return true;
    }
  }

  if (errorWithCode.status) {
    const status = errorWithCode.status;
    if (status === 408 || status === 429 || status === 502 || status === 503 || status === 504) {
      return true;
    }
  }

  return false;
}

/**
 * Combine idempotency and transient error checks.
 *
 * @param method - The RPC method
 * @param error - The error that occurred
 * @returns true if the operation should be retried
 */
export function shouldRetry(method: RpcMethodName, error: Error): boolean {
  return isIdempotentMethod(method) && isTransientError(error);
}

// ============================================================================
// Delay Calculation
// ============================================================================

/**
 * Calculate the delay before the next retry attempt.
 *
 * Uses exponential backoff with jitter:
 * delay = min(maxDelay, baseDelay * 2^attempt) * (1 + random * jitter)
 *
 * @param attempt - The retry attempt number (0-indexed)
 * @param config - Retry configuration
 * @returns Delay in milliseconds
 */
export function calculateRetryDelay(attempt: number, config: RetryConfig): number {
  // Exponential backoff: baseDelay * 2^attempt
  const exponentialDelay = config.baseDelayMs * Math.pow(2, attempt);

  // Cap at maxDelay
  const cappedDelay = Math.min(exponentialDelay, config.maxDelayMs);

  // Add jitter: delay * (1 + random * jitterFactor)
  const jitter = 1 + Math.random() * config.jitterFactor;

  return Math.round(cappedDelay * jitter);
}

// ============================================================================
// Retry Wrapper
// ============================================================================

/**
 * Execute an operation with retry logic.
 *
 * @param operation - The async operation to execute
 * @param config - Retry configuration (uses defaults if not provided)
 * @returns Promise resolving to the retry result
 *
 * @example
 * ```typescript
 * const result = await withRetry(
 *   () => rpcClient.call('getEntity', entityId),
 *   { maxRetries: 3, baseDelayMs: 100 }
 * );
 *
 * if (result.success) {
 *   console.log('Got entity:', result.value);
 * } else {
 *   console.error('Failed after', result.attempts, 'attempts:', result.error);
 * }
 * ```
 */
export async function withRetry<T>(
  operation: () => Promise<T>,
  config: Partial<RetryConfig> = {}
): Promise<RetryResult<T>> {
  const fullConfig: RetryConfig = { ...DEFAULT_RETRY_CONFIG, ...config };
  const isRetryable = fullConfig.isRetryable ?? isTransientError;

  const startTime = Date.now();
  let lastError: Error | undefined;
  let attempts = 0;

  while (attempts <= fullConfig.maxRetries) {
    attempts++;

    try {
      const value = await operation();
      return {
        value,
        attempts,
        success: true,
        totalTimeMs: Date.now() - startTime,
      };
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));

      // Check if we should retry
      const hasMoreRetries = attempts <= fullConfig.maxRetries;
      const canRetry = hasMoreRetries && isRetryable(lastError);

      if (!canRetry) {
        break;
      }

      // Calculate delay and wait
      const delayMs = calculateRetryDelay(attempts - 1, fullConfig);
      fullConfig.onRetry?.(attempts, lastError, delayMs);

      await sleep(delayMs);
    }
  }

  return {
    error: lastError ?? new Error('Unknown error'),
    attempts,
    success: false,
    totalTimeMs: Date.now() - startTime,
  };
}

/**
 * Execute an RPC call with retry logic for idempotent methods.
 *
 * This is a specialized version of withRetry that checks if the method
 * is idempotent before allowing retries.
 *
 * @param method - The RPC method name
 * @param operation - The async RPC operation to execute
 * @param config - Retry configuration
 * @returns Promise resolving to the retry result
 *
 * @example
 * ```typescript
 * const result = await withRpcRetry(
 *   'getEntity',
 *   () => client.call('getEntity', entityId),
 *   { maxRetries: 3 }
 * );
 * ```
 */
export async function withRpcRetry<T>(
  method: RpcMethodName,
  operation: () => Promise<T>,
  config: Partial<RetryConfig> = {}
): Promise<RetryResult<T>> {
  // For non-idempotent methods, don't retry
  if (!isIdempotentMethod(method)) {
    const startTime = Date.now();
    try {
      const value = await operation();
      return {
        value,
        attempts: 1,
        success: true,
        totalTimeMs: Date.now() - startTime,
      };
    } catch (err) {
      return {
        error: err instanceof Error ? err : new Error(String(err)),
        attempts: 1,
        success: false,
        totalTimeMs: Date.now() - startTime,
      };
    }
  }

  // For idempotent methods, use retry logic
  return withRetry(operation, {
    ...config,
    isRetryable: config.isRetryable ?? ((error) => shouldRetry(method, error)),
  });
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Sleep for a specified duration.
 * @param ms - Duration in milliseconds
 */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Create a retry config with custom settings.
 *
 * @param overrides - Partial config to override defaults
 * @returns Complete retry config
 */
export function createRetryConfig(overrides: Partial<RetryConfig> = {}): RetryConfig {
  return { ...DEFAULT_RETRY_CONFIG, ...overrides };
}

/**
 * Create a retry config optimized for low-latency scenarios.
 * Uses shorter delays and fewer retries.
 */
export function createLowLatencyRetryConfig(overrides: Partial<RetryConfig> = {}): RetryConfig {
  return {
    maxRetries: 2,
    baseDelayMs: 50,
    maxDelayMs: 500,
    jitterFactor: 0.1,
    ...overrides,
  };
}

/**
 * Create a retry config optimized for high-reliability scenarios.
 * Uses more retries and longer delays.
 */
export function createHighReliabilityRetryConfig(
  overrides: Partial<RetryConfig> = {}
): RetryConfig {
  return {
    maxRetries: 5,
    baseDelayMs: 200,
    maxDelayMs: 10000,
    jitterFactor: 0.3,
    ...overrides,
  };
}
