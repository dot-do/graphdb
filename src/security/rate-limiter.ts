/**
 * Rate Limiter for Durable Object Protection
 *
 * Implements windowed rate limiting to protect DOs from abuse:
 * - Configurable time windows (e.g., 60 requests per minute)
 * - Per-client tracking (by IP or other identifier)
 * - Sliding window counter implementation
 *
 * Usage:
 * ```typescript
 * const limiter = createRateLimiter({
 *   windowMs: 60000,    // 1 minute window
 *   maxRequests: 100,   // 100 requests per window
 * });
 *
 * // In request handler:
 * const clientIp = request.headers.get('CF-Connecting-IP') || 'unknown';
 * const result = limiter.check(clientIp);
 *
 * if (!result.allowed) {
 *   return new Response('Too Many Requests', {
 *     status: 429,
 *     headers: {
 *       'Retry-After': String(Math.ceil((result.resetAt - Date.now()) / 1000)),
 *       'X-RateLimit-Remaining': '0',
 *     },
 *   });
 * }
 *
 * limiter.consume(clientIp);
 * // Process request...
 * ```
 */

/**
 * Configuration for the rate limiter
 */
export interface RateLimiterConfig {
  /** Time window in milliseconds (e.g., 60000 for 1 minute) */
  windowMs: number;
  /** Maximum number of requests allowed per window */
  maxRequests: number;
}

/**
 * Result of a rate limit check
 */
export interface RateLimitResult {
  /** Whether the request is allowed */
  allowed: boolean;
  /** Number of requests remaining in the current window */
  remaining: number;
  /** Unix timestamp (ms) when the window resets */
  resetAt: number;
}

/**
 * Rate limiter interface
 */
export interface RateLimiter {
  /**
   * Check if a client is allowed to make a request without consuming a token
   * @param clientId - Unique identifier for the client (typically IP address)
   * @returns Rate limit check result
   */
  check(clientId: string): RateLimitResult;

  /**
   * Consume a request token for the client
   * @param clientId - Unique identifier for the client
   * @returns true if the request was allowed and token consumed, false if rate limited
   */
  consume(clientId: string): boolean;
}

/**
 * Internal tracking data for a client
 */
interface ClientData {
  /** Number of requests made in current window */
  count: number;
  /** Start time of the current window */
  windowStart: number;
}

/**
 * Creates a new rate limiter with the specified configuration
 *
 * @param config - Rate limiter configuration
 * @returns A new RateLimiter instance
 * @throws Error if configuration is invalid
 */
export function createRateLimiter(config: RateLimiterConfig): RateLimiter {
  // Validate configuration
  if (config.windowMs <= 0) {
    throw new Error('windowMs must be a positive number');
  }
  if (config.maxRequests <= 0) {
    throw new Error('maxRequests must be a positive number');
  }

  const { windowMs, maxRequests } = config;

  // Map to track request counts per client
  const clients = new Map<string, ClientData>();

  /**
   * Get or create client data, resetting if window has expired
   */
  function getClientData(clientId: string): ClientData {
    const now = Date.now();
    let data = clients.get(clientId);

    if (!data) {
      // New client - create fresh window
      data = {
        count: 0,
        windowStart: now,
      };
      clients.set(clientId, data);
    } else if (now - data.windowStart >= windowMs) {
      // Window has expired - reset
      data.count = 0;
      data.windowStart = now;
    }

    return data;
  }

  return {
    check(clientId: string): RateLimitResult {
      const data = getClientData(clientId);
      const remaining = Math.max(0, maxRequests - data.count);
      const resetAt = data.windowStart + windowMs;

      return {
        allowed: data.count < maxRequests,
        remaining,
        resetAt,
      };
    },

    consume(clientId: string): boolean {
      const data = getClientData(clientId);

      if (data.count >= maxRequests) {
        return false;
      }

      data.count++;
      return true;
    },
  };
}
