/**
 * Shard Response Error Tests - RED Phase
 *
 * Tests for error type checking in shard responses.
 * Validates that:
 * - Error responses from shards are detected
 * - Shard errors are propagated to clients
 * - Malformed responses are handled gracefully
 * - Response structures are validated
 */

import { describe, it, expect } from 'vitest';
import {
  validateShardResponse,
  isShardError,
  type ShardResponse,
  type ShardError,
  type ShardSuccess,
} from '../../src/broker/response-validator';

describe('Shard Response Validation', () => {
  describe('validateShardResponse', () => {
    it('should detect error response from shard', () => {
      const errorResponse = {
        success: false,
        error: {
          code: 'SHARD_UNAVAILABLE',
          message: 'Shard is temporarily unavailable',
        },
      };

      const result = validateShardResponse(errorResponse);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('SHARD_UNAVAILABLE');
        expect(result.error.message).toBe('Shard is temporarily unavailable');
      }
    });

    it('should validate successful response from shard', () => {
      const successResponse = {
        success: true,
        data: [
          { $id: 'https://example.com/entity/1', $type: 'Person', name: 'Alice' },
        ],
      };

      const result = validateShardResponse<typeof successResponse.data>(successResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(1);
        expect(result.data[0].$id).toBe('https://example.com/entity/1');
      }
    });

    it('should handle malformed shard response - missing success field', () => {
      const malformed = {
        data: [{ $id: 'https://example.com/entity/1' }],
      };

      const result = validateShardResponse(malformed);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
        expect(result.error.message).toContain('missing success field');
      }
    });

    it('should handle malformed shard response - missing error object', () => {
      const malformed = {
        success: false,
        // error object missing
      };

      const result = validateShardResponse(malformed);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
        expect(result.error.message).toContain('missing error object');
      }
    });

    it('should handle malformed shard response - missing data on success', () => {
      const malformed = {
        success: true,
        // data missing
      };

      const result = validateShardResponse(malformed);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
        expect(result.error.message).toContain('missing data field');
      }
    });

    it('should handle null response', () => {
      const result = validateShardResponse(null);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
      }
    });

    it('should handle undefined response', () => {
      const result = validateShardResponse(undefined);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
      }
    });

    it('should handle non-object response', () => {
      const result = validateShardResponse('string response');

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
      }
    });

    it('should handle legacy array format as success (backward compatibility)', () => {
      // Legacy shards return arrays directly without the success/data wrapper
      const legacyResponse = [
        { $id: 'https://example.com/entity/1', $type: 'Person', name: 'Alice' },
        { $id: 'https://example.com/entity/2', $type: 'Person', name: 'Bob' },
      ];

      const result = validateShardResponse(legacyResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toHaveLength(2);
        expect(result.data[0].$id).toBe('https://example.com/entity/1');
      }
    });

    it('should handle empty legacy array format', () => {
      const legacyEmptyResponse: unknown[] = [];

      const result = validateShardResponse(legacyEmptyResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual([]);
      }
    });

    it('should validate response structure with error code and message', () => {
      const errorResponse = {
        success: false,
        error: {
          code: 'ENTITY_NOT_FOUND',
          message: 'Entity with ID xyz not found',
        },
      };

      const result = validateShardResponse(errorResponse);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(typeof result.error.code).toBe('string');
        expect(typeof result.error.message).toBe('string');
      }
    });

    it('should handle error response with missing code', () => {
      const malformed = {
        success: false,
        error: {
          message: 'Something went wrong',
          // code missing
        },
      };

      const result = validateShardResponse(malformed);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
      }
    });

    it('should handle error response with missing message', () => {
      const malformed = {
        success: false,
        error: {
          code: 'SOME_ERROR',
          // message missing
        },
      };

      const result = validateShardResponse(malformed);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('MALFORMED_RESPONSE');
      }
    });
  });

  describe('isShardError', () => {
    it('should return true for error responses', () => {
      const errorResponse: ShardResponse<unknown> = {
        success: false,
        error: {
          code: 'TEST_ERROR',
          message: 'Test error message',
        },
      };

      expect(isShardError(errorResponse)).toBe(true);
    });

    it('should return false for success responses', () => {
      const successResponse: ShardResponse<string[]> = {
        success: true,
        data: ['item1', 'item2'],
      };

      expect(isShardError(successResponse)).toBe(false);
    });

    it('should provide type narrowing for error responses', () => {
      const response: ShardResponse<{ id: string }[]> = {
        success: false,
        error: {
          code: 'NETWORK_ERROR',
          message: 'Connection failed',
        },
      };

      if (isShardError(response)) {
        // TypeScript should know this is an error response
        expect(response.error.code).toBe('NETWORK_ERROR');
        expect(response.error.message).toBe('Connection failed');
      }
    });

    it('should provide type narrowing for success responses', () => {
      const response: ShardResponse<{ id: string }[]> = {
        success: true,
        data: [{ id: '123' }],
      };

      if (!isShardError(response)) {
        // TypeScript should know this is a success response
        expect(response.data).toHaveLength(1);
        expect(response.data[0].id).toBe('123');
      }
    });
  });

  describe('Error propagation', () => {
    it('should propagate shard error to client with original code', () => {
      const shardErrorResponse = {
        success: false,
        error: {
          code: 'SHARD_OVERLOADED',
          message: 'Too many concurrent requests',
        },
      };

      const validated = validateShardResponse(shardErrorResponse);

      expect(validated.success).toBe(false);
      if (!validated.success) {
        // Error should be propagated with original details
        expect(validated.error.code).toBe('SHARD_OVERLOADED');
        expect(validated.error.message).toBe('Too many concurrent requests');
      }
    });

    it('should include shard ID in error details when provided', () => {
      const shardErrorResponse = {
        success: false,
        error: {
          code: 'SHARD_TIMEOUT',
          message: 'Request timed out',
          shardId: 'shard-7',
        },
      };

      const validated = validateShardResponse(shardErrorResponse);

      expect(validated.success).toBe(false);
      if (!validated.success) {
        expect(validated.error.code).toBe('SHARD_TIMEOUT');
        // The shardId should be preserved if present
        expect((validated.error as ShardError & { shardId?: string }).shardId).toBe('shard-7');
      }
    });
  });

  describe('Response structure validation', () => {
    it('should accept response with additional metadata', () => {
      const responseWithMeta = {
        success: true,
        data: [{ $id: 'https://example.com/1' }],
        metadata: {
          executionTime: 42,
          rowsScanned: 100,
        },
      };

      const result = validateShardResponse(responseWithMeta);

      expect(result.success).toBe(true);
    });

    it('should preserve data type through validation', () => {
      interface CustomData {
        $id: string;
        name: string;
        age: number;
      }

      const typedResponse = {
        success: true,
        data: [
          { $id: 'https://example.com/person/1', name: 'Alice', age: 30 },
        ] as CustomData[],
      };

      const result = validateShardResponse<CustomData[]>(typedResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data[0].name).toBe('Alice');
        expect(result.data[0].age).toBe(30);
      }
    });

    it('should handle empty data array as valid success response', () => {
      const emptyResponse = {
        success: true,
        data: [],
      };

      const result = validateShardResponse(emptyResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual([]);
      }
    });

    it('should handle non-array data types', () => {
      const singleEntityResponse = {
        success: true,
        data: { $id: 'https://example.com/1', count: 42 },
      };

      const result = validateShardResponse(singleEntityResponse);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).toEqual({ $id: 'https://example.com/1', count: 42 });
      }
    });
  });
});
