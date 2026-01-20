/**
 * Rate Limiting Security Tests
 *
 * Tests for protection against abuse and DoS attacks:
 * - Request rate limiting per client IP
 * - Configurable request limits per time window
 * - Windowed rate limiting (requests per minute)
 * - Graceful handling when limits exceeded
 *
 * These tests validate the rate limiter used to protect Durable Objects.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  createRateLimiter,
  RateLimiterConfig,
  RateLimiter,
  RateLimitResult,
} from '../../src/security/rate-limiter.js';

describe('Rate Limiting Security', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('Basic Rate Limiting', () => {
    it('should allow requests within rate limit', () => {
      const limiter = createRateLimiter({
        windowMs: 60000, // 1 minute
        maxRequests: 10,
      });

      const clientId = '192.168.1.1';

      // First request should be allowed
      const result = limiter.check(clientId);

      expect(result.allowed).toBe(true);
      expect(result.remaining).toBe(10); // Haven't consumed yet
    });

    it('should reject requests exceeding rate limit', () => {
      const limiter = createRateLimiter({
        windowMs: 60000, // 1 minute
        maxRequests: 5,
      });

      const clientId = '192.168.1.1';

      // Consume all 5 allowed requests
      for (let i = 0; i < 5; i++) {
        const consumed = limiter.consume(clientId);
        expect(consumed).toBe(true);
      }

      // 6th request should be rejected
      const result = limiter.check(clientId);
      expect(result.allowed).toBe(false);
      expect(result.remaining).toBe(0);

      // Consume should also fail
      const consumed = limiter.consume(clientId);
      expect(consumed).toBe(false);
    });
  });

  describe('Client IP Tracking', () => {
    it('should track requests per client IP', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 3,
      });

      const client1 = '192.168.1.1';
      const client2 = '192.168.1.2';

      // Client 1 uses all their requests
      limiter.consume(client1);
      limiter.consume(client1);
      limiter.consume(client1);

      // Client 1 should be blocked
      const result1 = limiter.check(client1);
      expect(result1.allowed).toBe(false);
      expect(result1.remaining).toBe(0);

      // Client 2 should still be allowed (independent limit)
      const result2 = limiter.check(client2);
      expect(result2.allowed).toBe(true);
      expect(result2.remaining).toBe(3);
    });

    it('should handle multiple concurrent clients', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 10,
      });

      const clients = [
        '10.0.0.1',
        '10.0.0.2',
        '10.0.0.3',
        '10.0.0.4',
        '10.0.0.5',
      ];

      // Each client makes different number of requests
      clients.forEach((client, index) => {
        for (let i = 0; i <= index; i++) {
          limiter.consume(client);
        }
      });

      // Verify each client has correct remaining count
      clients.forEach((client, index) => {
        const result = limiter.check(client);
        expect(result.remaining).toBe(10 - (index + 1));
      });
    });
  });

  describe('Configurable Limits', () => {
    it('should support configurable limits', () => {
      // Low limit configuration
      const strictLimiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 2,
      });

      // High limit configuration
      const relaxedLimiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 100,
      });

      const clientId = 'test-client';

      // Strict limiter blocks after 2 requests
      strictLimiter.consume(clientId);
      strictLimiter.consume(clientId);
      expect(strictLimiter.check(clientId).allowed).toBe(false);

      // Relaxed limiter still allows after 2 requests
      relaxedLimiter.consume(clientId);
      relaxedLimiter.consume(clientId);
      expect(relaxedLimiter.check(clientId).allowed).toBe(true);
      expect(relaxedLimiter.check(clientId).remaining).toBe(98);
    });

    it('should support different window durations', () => {
      // 1 second window
      const shortWindow = createRateLimiter({
        windowMs: 1000,
        maxRequests: 5,
      });

      // 1 minute window
      const longWindow = createRateLimiter({
        windowMs: 60000,
        maxRequests: 5,
      });

      const clientId = 'test-client';

      // Use up all requests in both
      for (let i = 0; i < 5; i++) {
        shortWindow.consume(clientId);
        longWindow.consume(clientId);
      }

      // Both blocked initially
      expect(shortWindow.check(clientId).allowed).toBe(false);
      expect(longWindow.check(clientId).allowed).toBe(false);

      // Advance time by 1.1 seconds
      vi.advanceTimersByTime(1100);

      // Short window should reset, long window still blocked
      expect(shortWindow.check(clientId).allowed).toBe(true);
      expect(longWindow.check(clientId).allowed).toBe(false);
    });
  });

  describe('Windowed Rate Limiting', () => {
    it('should support windowed rate limiting (requests per minute)', () => {
      const limiter = createRateLimiter({
        windowMs: 60000, // 1 minute
        maxRequests: 60, // 60 requests per minute
      });

      const clientId = 'api-client';

      // Consume 60 requests
      for (let i = 0; i < 60; i++) {
        expect(limiter.consume(clientId)).toBe(true);
      }

      // 61st should fail
      expect(limiter.consume(clientId)).toBe(false);
      expect(limiter.check(clientId).allowed).toBe(false);

      // Wait for window to expire
      vi.advanceTimersByTime(60001);

      // Should be allowed again
      expect(limiter.check(clientId).allowed).toBe(true);
      expect(limiter.check(clientId).remaining).toBe(60);
    });

    it('should reset counter after window expires', () => {
      const limiter = createRateLimiter({
        windowMs: 5000, // 5 seconds
        maxRequests: 3,
      });

      const clientId = 'test-client';

      // Use all requests
      limiter.consume(clientId);
      limiter.consume(clientId);
      limiter.consume(clientId);

      expect(limiter.check(clientId).allowed).toBe(false);

      // Advance past window
      vi.advanceTimersByTime(5001);

      // Counter should reset
      const result = limiter.check(clientId);
      expect(result.allowed).toBe(true);
      expect(result.remaining).toBe(3);
    });

    it('should provide accurate resetAt timestamp', () => {
      const windowMs = 10000;
      const limiter = createRateLimiter({
        windowMs,
        maxRequests: 5,
      });

      const clientId = 'test-client';
      const startTime = Date.now();

      // First request establishes window
      limiter.consume(clientId);

      const result = limiter.check(clientId);

      // resetAt should be approximately startTime + windowMs
      expect(result.resetAt).toBeGreaterThanOrEqual(startTime + windowMs);
      expect(result.resetAt).toBeLessThanOrEqual(startTime + windowMs + 100);
    });
  });

  describe('Edge Cases', () => {
    it('should handle zero remaining correctly', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 1,
      });

      const clientId = 'single-request';

      limiter.consume(clientId);

      const result = limiter.check(clientId);
      expect(result.allowed).toBe(false);
      expect(result.remaining).toBe(0);
    });

    it('should handle first-time clients', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 10,
      });

      // New client that has never been seen
      const newClient = 'brand-new-client-' + Date.now();

      const result = limiter.check(newClient);
      expect(result.allowed).toBe(true);
      expect(result.remaining).toBe(10);
    });

    it('should handle empty client ID', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 5,
      });

      // Empty string should still work (tracks anonymous requests)
      const result = limiter.check('');
      expect(result.allowed).toBe(true);

      limiter.consume('');
      const afterConsume = limiter.check('');
      expect(afterConsume.remaining).toBe(4);
    });

    it('should handle very long client IDs', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 5,
      });

      const longId = 'client-'.repeat(1000);

      const result = limiter.check(longId);
      expect(result.allowed).toBe(true);

      limiter.consume(longId);
      expect(limiter.check(longId).remaining).toBe(4);
    });
  });

  describe('Remaining Count Accuracy', () => {
    it('should accurately track remaining requests', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 5,
      });

      const clientId = 'counter-test';

      expect(limiter.check(clientId).remaining).toBe(5);

      limiter.consume(clientId);
      expect(limiter.check(clientId).remaining).toBe(4);

      limiter.consume(clientId);
      expect(limiter.check(clientId).remaining).toBe(3);

      limiter.consume(clientId);
      expect(limiter.check(clientId).remaining).toBe(2);

      limiter.consume(clientId);
      expect(limiter.check(clientId).remaining).toBe(1);

      limiter.consume(clientId);
      expect(limiter.check(clientId).remaining).toBe(0);
    });

    it('should not go negative on remaining count', () => {
      const limiter = createRateLimiter({
        windowMs: 60000,
        maxRequests: 2,
      });

      const clientId = 'negative-test';

      // Consume all
      limiter.consume(clientId);
      limiter.consume(clientId);

      // Try to consume more (should fail)
      limiter.consume(clientId);
      limiter.consume(clientId);

      // Remaining should still be 0, not negative
      expect(limiter.check(clientId).remaining).toBe(0);
    });
  });

  describe('Configuration Validation', () => {
    it('should require positive windowMs', () => {
      expect(() => createRateLimiter({
        windowMs: 0,
        maxRequests: 10,
      })).toThrow();

      expect(() => createRateLimiter({
        windowMs: -1000,
        maxRequests: 10,
      })).toThrow();
    });

    it('should require positive maxRequests', () => {
      expect(() => createRateLimiter({
        windowMs: 60000,
        maxRequests: 0,
      })).toThrow();

      expect(() => createRateLimiter({
        windowMs: 60000,
        maxRequests: -5,
      })).toThrow();
    });
  });
});
