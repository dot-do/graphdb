/**
 * WebSocket Error Format Tests
 *
 * Tests for consistent WebSocket error response format.
 * All WebSocket error messages should follow the standard structure:
 * {
 *   type: 'error';
 *   code: string;       // Machine-readable error code (e.g., 'INVALID_REQUEST', 'NOT_FOUND')
 *   message: string;    // Human-readable error message
 *   id?: string;        // Request ID if available (for correlation)
 *   details?: Record<string, unknown>; // Optional additional context
 * }
 */

import { describe, it, expect } from 'vitest';
import {
  createWsError,
  wsErrorJson,
  WsErrorCode,
  type WsErrorResponse,
} from '../../src/errors/api-error.js';

describe('WsErrorResponse', () => {
  describe('createWsError', () => {
    it('should create error with type, code and message', () => {
      const error = createWsError(WsErrorCode.INVALID_REQUEST, 'Invalid message format');

      expect(error).toEqual({
        type: 'error',
        code: 'INVALID_REQUEST',
        message: 'Invalid message format',
      });
    });

    it('should create error with request ID for correlation', () => {
      const error = createWsError(WsErrorCode.NOT_FOUND, 'Entity not found', 'req-123');

      expect(error).toEqual({
        type: 'error',
        code: 'NOT_FOUND',
        message: 'Entity not found',
        id: 'req-123',
      });
    });

    it('should create error with optional details', () => {
      const error = createWsError(
        WsErrorCode.MISSING_PARAMETER,
        'Parameter required',
        undefined,
        { parameter: 'queryId' }
      );

      expect(error).toEqual({
        type: 'error',
        code: 'MISSING_PARAMETER',
        message: 'Parameter required',
        details: {
          parameter: 'queryId',
        },
      });
    });

    it('should create error with both id and details', () => {
      const error = createWsError(
        WsErrorCode.VALIDATION_ERROR,
        'Invalid input',
        'req-456',
        { field: 'subject', reason: 'must be a valid URL' }
      );

      expect(error).toEqual({
        type: 'error',
        code: 'VALIDATION_ERROR',
        message: 'Invalid input',
        id: 'req-456',
        details: {
          field: 'subject',
          reason: 'must be a valid URL',
        },
      });
    });

    it('should handle empty details as undefined', () => {
      const error = createWsError(WsErrorCode.INTERNAL_ERROR, 'Something went wrong', undefined, {});

      // Empty details should not be included
      expect(error.details).toBeUndefined();
    });

    it('should not include id when undefined', () => {
      const error = createWsError(WsErrorCode.BAD_REQUEST, 'Bad request');

      expect(error.id).toBeUndefined();
      expect('id' in error).toBe(false);
    });
  });

  describe('wsErrorJson', () => {
    it('should return valid JSON string', () => {
      const jsonStr = wsErrorJson(WsErrorCode.INVALID_REQUEST, 'Test error');
      const parsed = JSON.parse(jsonStr);

      expect(parsed.type).toBe('error');
      expect(parsed.code).toBe('INVALID_REQUEST');
      expect(parsed.message).toBe('Test error');
    });

    it('should include id in JSON when provided', () => {
      const jsonStr = wsErrorJson(WsErrorCode.RPC_ERROR, 'Method failed', 'call-123');
      const parsed = JSON.parse(jsonStr);

      expect(parsed.id).toBe('call-123');
    });

    it('should include details in JSON when provided', () => {
      const jsonStr = wsErrorJson(
        WsErrorCode.MISSING_PARAMETER,
        'Missing queryId',
        undefined,
        { parameter: 'queryId' }
      );
      const parsed = JSON.parse(jsonStr);

      expect(parsed.details).toEqual({ parameter: 'queryId' });
    });
  });

  describe('WsErrorCode', () => {
    it('should include all standard HTTP error codes', () => {
      expect(WsErrorCode.BAD_REQUEST).toBe('BAD_REQUEST');
      expect(WsErrorCode.NOT_FOUND).toBe('NOT_FOUND');
      expect(WsErrorCode.VALIDATION_ERROR).toBe('VALIDATION_ERROR');
      expect(WsErrorCode.INTERNAL_ERROR).toBe('INTERNAL_ERROR');
      expect(WsErrorCode.METHOD_NOT_ALLOWED).toBe('METHOD_NOT_ALLOWED');
      expect(WsErrorCode.NOT_IMPLEMENTED).toBe('NOT_IMPLEMENTED');
      expect(WsErrorCode.PARSE_ERROR).toBe('PARSE_ERROR');
      expect(WsErrorCode.UNAUTHORIZED).toBe('UNAUTHORIZED');
      expect(WsErrorCode.FORBIDDEN).toBe('FORBIDDEN');
      expect(WsErrorCode.CONFLICT).toBe('CONFLICT');
      expect(WsErrorCode.TIMEOUT).toBe('TIMEOUT');
    });

    it('should include WebSocket-specific error codes', () => {
      expect(WsErrorCode.INVALID_REQUEST).toBe('INVALID_REQUEST');
      expect(WsErrorCode.MISSING_ATTACHMENT).toBe('MISSING_ATTACHMENT');
      expect(WsErrorCode.MISSING_PARAMETER).toBe('MISSING_PARAMETER');
      expect(WsErrorCode.QUERY_FAILED).toBe('QUERY_FAILED');
      expect(WsErrorCode.RPC_ERROR).toBe('RPC_ERROR');
      expect(WsErrorCode.UNKNOWN_METHOD).toBe('UNKNOWN_METHOD');
    });
  });

  describe('WsErrorResponse type structure', () => {
    it('should always have type set to "error"', () => {
      const error = createWsError(WsErrorCode.BAD_REQUEST, 'Test');
      expect(error.type).toBe('error');
    });

    it('should have required code property', () => {
      const error = createWsError(WsErrorCode.BAD_REQUEST, 'Test');
      expect(typeof error.code).toBe('string');
      expect(error.code.length).toBeGreaterThan(0);
    });

    it('should have required message property', () => {
      const error = createWsError(WsErrorCode.BAD_REQUEST, 'Test message');
      expect(typeof error.message).toBe('string');
      expect(error.message).toBe('Test message');
    });

    it('should have optional id property', () => {
      const errorWithId = createWsError(WsErrorCode.BAD_REQUEST, 'Test', 'req-1');
      const errorWithoutId = createWsError(WsErrorCode.BAD_REQUEST, 'Test');

      expect(errorWithId.id).toBe('req-1');
      expect(errorWithoutId.id).toBeUndefined();
    });

    it('should have optional details property', () => {
      const errorWithDetails = createWsError(WsErrorCode.BAD_REQUEST, 'Test', undefined, { key: 'value' });
      const errorWithoutDetails = createWsError(WsErrorCode.BAD_REQUEST, 'Test');

      expect(errorWithDetails.details).toBeDefined();
      expect(errorWithoutDetails.details).toBeUndefined();
    });
  });

  describe('Error format consistency across scenarios', () => {
    it('should produce consistent format for missing attachment error', () => {
      const error = createWsError(WsErrorCode.MISSING_ATTACHMENT, 'No attachment found');

      expect(error.type).toBe('error');
      expect(error.code).toBe('MISSING_ATTACHMENT');
      expect(error.message).toBe('No attachment found');
    });

    it('should produce consistent format for missing parameter error', () => {
      const error = createWsError(
        WsErrorCode.MISSING_PARAMETER,
        'queryId is required for storeCursor',
        undefined,
        { parameter: 'queryId' }
      );

      expect(error.type).toBe('error');
      expect(error.code).toBe('MISSING_PARAMETER');
      expect(error.message).toBe('queryId is required for storeCursor');
      expect(error.details).toEqual({ parameter: 'queryId' });
    });

    it('should produce consistent format for query failed error', () => {
      const error = createWsError(WsErrorCode.QUERY_FAILED, 'Syntax error in query');

      expect(error.type).toBe('error');
      expect(error.code).toBe('QUERY_FAILED');
      expect(error.message).toBe('Syntax error in query');
    });

    it('should produce consistent format for RPC error with id', () => {
      const error = createWsError(WsErrorCode.RPC_ERROR, 'Method not found', 'rpc-call-456');

      expect(error.type).toBe('error');
      expect(error.code).toBe('RPC_ERROR');
      expect(error.message).toBe('Method not found');
      expect(error.id).toBe('rpc-call-456');
    });
  });

  describe('JSON serialization round-trip', () => {
    it('should preserve all fields through serialization', () => {
      const original = createWsError(
        WsErrorCode.VALIDATION_ERROR,
        'Field validation failed',
        'req-789',
        { field: 'email', error: 'invalid format' }
      );

      const jsonStr = JSON.stringify(original);
      const parsed = JSON.parse(jsonStr) as WsErrorResponse;

      expect(parsed.type).toBe(original.type);
      expect(parsed.code).toBe(original.code);
      expect(parsed.message).toBe(original.message);
      expect(parsed.id).toBe(original.id);
      expect(parsed.details).toEqual(original.details);
    });

    it('should handle special characters in message', () => {
      const error = createWsError(
        WsErrorCode.PARSE_ERROR,
        'Unexpected token "<" at position 0'
      );

      const jsonStr = JSON.stringify(error);
      const parsed = JSON.parse(jsonStr) as WsErrorResponse;

      expect(parsed.message).toBe('Unexpected token "<" at position 0');
    });

    it('should handle unicode in message', () => {
      const error = createWsError(
        WsErrorCode.VALIDATION_ERROR,
        'Invalid character: \u00e9\u00e8\u00ea'
      );

      const jsonStr = JSON.stringify(error);
      const parsed = JSON.parse(jsonStr) as WsErrorResponse;

      expect(parsed.message).toContain('\u00e9');
    });
  });
});
