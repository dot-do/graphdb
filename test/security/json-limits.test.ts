/**
 * JSON Validation Security Tests
 *
 * Tests for protection against DoS attacks via unbounded JSON parsing:
 * - Message size limits (prevent memory exhaustion)
 * - Nesting depth limits (prevent stack overflow)
 * - Key count limits (prevent hash collision attacks)
 * - Graceful handling of malformed JSON
 *
 * These tests validate the safeJsonParse function used in WebSocket handlers.
 */

import { describe, it, expect } from 'vitest';
import {
  safeJsonParse,
  JsonParseError,
  JsonParseErrorCode,
  DEFAULT_MAX_SIZE,
  DEFAULT_MAX_DEPTH,
  DEFAULT_MAX_KEYS,
} from '../../src/security/json-validator.js';

describe('JSON Validation Security', () => {
  describe('Size Limits', () => {
    it('should reject messages larger than MAX_MESSAGE_SIZE (64KB)', () => {
      // Create a string larger than 64KB
      const largePayload = JSON.stringify({ data: 'x'.repeat(70000) });
      expect(largePayload.length).toBeGreaterThan(DEFAULT_MAX_SIZE);

      const result = safeJsonParse(largePayload);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.SIZE_EXCEEDED);
        expect(result.message).toContain('exceeds maximum allowed size');
      }
    });

    it('should accept messages within size limit', () => {
      const normalPayload = JSON.stringify({ type: 'ping', timestamp: Date.now() });
      expect(normalPayload.length).toBeLessThan(DEFAULT_MAX_SIZE);

      const result = safeJsonParse<{ type: string; timestamp: number }>(normalPayload);

      expect(result).not.toBeInstanceOf(Error);
      if (!(result instanceof Error)) {
        expect(result.type).toBe('ping');
      }
    });

    it('should allow custom size limits', () => {
      const payload = JSON.stringify({ data: 'x'.repeat(1000) });

      // Should fail with small limit
      const resultSmall = safeJsonParse(payload, { maxSize: 500 });
      expect(resultSmall).toBeInstanceOf(JsonParseError);

      // Should succeed with large limit
      const resultLarge = safeJsonParse(payload, { maxSize: 10000 });
      expect(resultLarge).not.toBeInstanceOf(Error);
    });

    it('should check size BEFORE attempting to parse', () => {
      // This tests that we don't even try to parse large messages
      // Create something that would crash JSON.parse if attempted
      const hugeSize = 100 * 1024; // 100KB
      const hugePayload = 'x'.repeat(hugeSize);

      const startTime = performance.now();
      const result = safeJsonParse(hugePayload);
      const elapsed = performance.now() - startTime;

      expect(result).toBeInstanceOf(JsonParseError);
      // Size check should be near-instant (< 1ms)
      expect(elapsed).toBeLessThan(10);
    });
  });

  describe('Nesting Depth Limits', () => {
    it('should reject deeply nested JSON (depth > 10)', () => {
      // Create JSON with depth 15
      let deepJson = '{"value": 42}';
      for (let i = 0; i < 14; i++) {
        deepJson = `{"nested": ${deepJson}}`;
      }

      const result = safeJsonParse(deepJson);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.DEPTH_EXCEEDED);
        expect(result.message).toContain('exceeds maximum allowed depth');
      }
    });

    it('should accept JSON within depth limit', () => {
      // Create JSON with depth 5 (within limit)
      let shallowJson = '{"value": 42}';
      for (let i = 0; i < 4; i++) {
        shallowJson = `{"nested": ${shallowJson}}`;
      }

      const result = safeJsonParse<{ nested: unknown }>(shallowJson);

      expect(result).not.toBeInstanceOf(Error);
    });

    it('should allow custom depth limits', () => {
      // Create JSON with depth 6
      let json = '{"value": 42}';
      for (let i = 0; i < 5; i++) {
        json = `{"nested": ${json}}`;
      }

      // Should fail with shallow limit
      const resultShallow = safeJsonParse(json, { maxDepth: 5 });
      expect(resultShallow).toBeInstanceOf(JsonParseError);

      // Should succeed with deeper limit
      const resultDeep = safeJsonParse(json, { maxDepth: 10 });
      expect(resultDeep).not.toBeInstanceOf(Error);
    });

    it('should handle deeply nested arrays', () => {
      // Create array with depth 15
      let deepArray = '[1, 2, 3]';
      for (let i = 0; i < 14; i++) {
        deepArray = `[${deepArray}]`;
      }

      const result = safeJsonParse(deepArray);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.DEPTH_EXCEEDED);
      }
    });

    it('should handle mixed nested objects and arrays', () => {
      // Create mixed nesting depth 15
      let mixed = '"value"';
      for (let i = 0; i < 7; i++) {
        mixed = `{"a": [${mixed}]}`;
      }
      // This creates depth > 14 (object + array alternating)

      const result = safeJsonParse(mixed);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.DEPTH_EXCEEDED);
      }
    });
  });

  describe('Key Count Limits', () => {
    it('should reject JSON with too many keys (> 1000)', () => {
      // Create object with 1500 keys
      const manyKeys: Record<string, number> = {};
      for (let i = 0; i < 1500; i++) {
        manyKeys[`key${i}`] = i;
      }
      const payload = JSON.stringify(manyKeys);

      const result = safeJsonParse(payload);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.KEYS_EXCEEDED);
        expect(result.message).toContain('exceeds maximum allowed key count');
      }
    });

    it('should accept JSON within key count limit', () => {
      // Create object with 100 keys (within limit)
      const normalKeys: Record<string, number> = {};
      for (let i = 0; i < 100; i++) {
        normalKeys[`key${i}`] = i;
      }
      const payload = JSON.stringify(normalKeys);

      const result = safeJsonParse<Record<string, number>>(payload);

      expect(result).not.toBeInstanceOf(Error);
    });

    it('should allow custom key count limits', () => {
      // Create object with 50 keys
      const keys: Record<string, number> = {};
      for (let i = 0; i < 50; i++) {
        keys[`key${i}`] = i;
      }
      const payload = JSON.stringify(keys);

      // Should fail with small limit
      const resultSmall = safeJsonParse(payload, { maxKeys: 30 });
      expect(resultSmall).toBeInstanceOf(JsonParseError);

      // Should succeed with larger limit
      const resultLarge = safeJsonParse(payload, { maxKeys: 100 });
      expect(resultLarge).not.toBeInstanceOf(Error);
    });

    it('should count keys across nested objects', () => {
      // Create nested structure with total 1100 keys spread across nested objects
      const nested: Record<string, Record<string, number>> = {};
      for (let i = 0; i < 22; i++) {
        nested[`obj${i}`] = {};
        for (let j = 0; j < 50; j++) {
          nested[`obj${i}`][`key${j}`] = j;
        }
      }
      // 22 top-level keys + 22 * 50 = 1122 total keys

      const payload = JSON.stringify(nested);

      const result = safeJsonParse(payload);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.KEYS_EXCEEDED);
      }
    });
  });

  describe('Malformed JSON Handling', () => {
    it('should handle malformed JSON gracefully', () => {
      const malformed = '{"type": "ping", invalid}';

      const result = safeJsonParse(malformed);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
        expect(result.message).toContain('Invalid JSON');
      }
    });

    it('should handle empty string', () => {
      const result = safeJsonParse('');

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
      }
    });

    it('should handle null input', () => {
      const result = safeJsonParse(null as unknown as string);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
      }
    });

    it('should handle undefined input', () => {
      const result = safeJsonParse(undefined as unknown as string);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
      }
    });

    it('should handle truncated JSON', () => {
      const truncated = '{"type": "ping", "data": ';

      const result = safeJsonParse(truncated);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
      }
    });

    it('should handle invalid escape sequences', () => {
      const invalidEscape = '{"data": "test\\qinvalid"}';

      const result = safeJsonParse(invalidEscape);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBe(JsonParseErrorCode.PARSE_ERROR);
      }
    });

    it('should handle non-string input types', () => {
      const numberInput = 12345 as unknown as string;
      const objectInput = { foo: 'bar' } as unknown as string;

      expect(safeJsonParse(numberInput)).toBeInstanceOf(JsonParseError);
      expect(safeJsonParse(objectInput)).toBeInstanceOf(JsonParseError);
    });
  });

  describe('Error Response Format', () => {
    it('should return proper error response on rejection', () => {
      const largePayload = JSON.stringify({ data: 'x'.repeat(70000) });

      const result = safeJsonParse(largePayload);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.code).toBeDefined();
        expect(result.message).toBeDefined();
        expect(typeof result.message).toBe('string');

        // Error should be serializable to JSON for WebSocket responses
        const errorResponse = result.toResponse();
        expect(errorResponse).toHaveProperty('type', 'error');
        expect(errorResponse).toHaveProperty('code');
        expect(errorResponse).toHaveProperty('message');
      }
    });

    it('should include limit details in error message for size exceeded', () => {
      const result = safeJsonParse('x'.repeat(100000));

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.message).toMatch(/\d+.*bytes/i);
      }
    });

    it('should include limit details in error message for depth exceeded', () => {
      let deep = '{"value": 42}';
      for (let i = 0; i < 14; i++) {
        deep = `{"nested": ${deep}}`;
      }

      const result = safeJsonParse(deep);

      expect(result).toBeInstanceOf(JsonParseError);
      if (result instanceof JsonParseError) {
        expect(result.message).toMatch(/depth/i);
      }
    });
  });

  describe('Valid JSON Parsing', () => {
    it('should parse valid JSON correctly', () => {
      const validJson = JSON.stringify({
        type: 'rpc',
        method: 'getEntity',
        args: ['https://example.com/user/1'],
        id: 'req-123',
      });

      const result = safeJsonParse<{
        type: string;
        method: string;
        args: string[];
        id: string;
      }>(validJson);

      expect(result).not.toBeInstanceOf(Error);
      if (!(result instanceof Error)) {
        expect(result.type).toBe('rpc');
        expect(result.method).toBe('getEntity');
        expect(result.args).toEqual(['https://example.com/user/1']);
        expect(result.id).toBe('req-123');
      }
    });

    it('should handle arrays at root level', () => {
      const arrayJson = JSON.stringify([1, 2, 3, { nested: true }]);

      const result = safeJsonParse<unknown[]>(arrayJson);

      expect(result).not.toBeInstanceOf(Error);
      if (!(result instanceof Error)) {
        expect(result.length).toBe(4);
      }
    });

    it('should handle primitive values', () => {
      expect(safeJsonParse('"string"')).toBe('string');
      expect(safeJsonParse('123')).toBe(123);
      expect(safeJsonParse('true')).toBe(true);
      expect(safeJsonParse('false')).toBe(false);
      expect(safeJsonParse('null')).toBe(null);
    });

    it('should preserve special characters in strings', () => {
      const special = JSON.stringify({ emoji: '\u{1F600}', newline: 'line1\nline2' });

      const result = safeJsonParse<{ emoji: string; newline: string }>(special);

      expect(result).not.toBeInstanceOf(Error);
      if (!(result instanceof Error)) {
        expect(result.emoji).toBe('\u{1F600}');
        expect(result.newline).toBe('line1\nline2');
      }
    });
  });

  describe('Edge Cases', () => {
    it('should handle exactly at size limit', () => {
      // Create payload exactly at limit
      const targetSize = DEFAULT_MAX_SIZE;
      const baseJson = '{"d":"';
      const suffix = '"}';
      const padding = 'x'.repeat(targetSize - baseJson.length - suffix.length);
      const exactPayload = baseJson + padding + suffix;

      expect(exactPayload.length).toBe(targetSize);

      const result = safeJsonParse(exactPayload);
      expect(result).not.toBeInstanceOf(Error);
    });

    it('should handle exactly at depth limit', () => {
      // Create JSON at exactly depth 10
      let exactDepth = '{"value": 42}';
      for (let i = 0; i < DEFAULT_MAX_DEPTH - 1; i++) {
        exactDepth = `{"n": ${exactDepth}}`;
      }

      const result = safeJsonParse(exactDepth);
      expect(result).not.toBeInstanceOf(Error);
    });

    it('should handle exactly at key count limit', () => {
      // Create object with exactly max keys
      const keys: Record<string, number> = {};
      for (let i = 0; i < DEFAULT_MAX_KEYS; i++) {
        keys[`k${i}`] = i;
      }
      const exactPayload = JSON.stringify(keys);

      const result = safeJsonParse(exactPayload);
      expect(result).not.toBeInstanceOf(Error);
    });

    it('should handle ArrayBuffer input', () => {
      const json = JSON.stringify({ type: 'ping' });
      const encoder = new TextEncoder();
      const buffer = encoder.encode(json);

      // Note: safeJsonParse expects string, but should handle buffer conversion upstream
      const decoded = new TextDecoder().decode(buffer);
      const result = safeJsonParse<{ type: string }>(decoded);

      expect(result).not.toBeInstanceOf(Error);
      if (!(result instanceof Error)) {
        expect(result.type).toBe('ping');
      }
    });
  });

  describe('Default Constants', () => {
    it('should export correct default limits', () => {
      expect(DEFAULT_MAX_SIZE).toBe(65536); // 64KB
      expect(DEFAULT_MAX_DEPTH).toBe(10);
      expect(DEFAULT_MAX_KEYS).toBe(1000);
    });
  });
});
