/**
 * Error Format Tests
 *
 * Tests for consistent error response format across all handlers.
 * All handlers should return errors in the same ApiError structure:
 * {
 *   error: {
 *     code: string;       // Machine-readable error code (e.g., "NOT_FOUND", "VALIDATION_ERROR")
 *     message: string;    // Human-readable error message
 *     details?: Record<string, unknown>; // Optional additional context
 *   }
 * }
 */

import { describe, it, expect } from 'vitest';
import {
  createApiError,
  toHttpResponse,
  type ApiError,
  ErrorCode,
} from '../../src/errors/api-error.js';

describe('ApiError', () => {
  describe('createApiError', () => {
    it('should create error with code and message', () => {
      const error = createApiError(ErrorCode.NOT_FOUND, 'Resource not found');

      expect(error).toEqual({
        error: {
          code: 'NOT_FOUND',
          message: 'Resource not found',
        },
      });
    });

    it('should create error with optional details', () => {
      const error = createApiError(
        ErrorCode.VALIDATION_ERROR,
        'Invalid input',
        { field: 'subject', reason: 'must be a valid URL' }
      );

      expect(error).toEqual({
        error: {
          code: 'VALIDATION_ERROR',
          message: 'Invalid input',
          details: {
            field: 'subject',
            reason: 'must be a valid URL',
          },
        },
      });
    });

    it('should handle empty details as undefined', () => {
      const error = createApiError(ErrorCode.INTERNAL_ERROR, 'Something went wrong', {});

      // Empty details should not be included
      expect(error.error.details).toBeUndefined();
    });
  });

  describe('toHttpResponse', () => {
    it('should return Response with correct status code', async () => {
      const error = createApiError(ErrorCode.NOT_FOUND, 'Triple not found');
      const response = toHttpResponse(error, 404);

      expect(response.status).toBe(404);
    });

    it('should return Response with JSON content type', async () => {
      const error = createApiError(ErrorCode.BAD_REQUEST, 'Invalid JSON');
      const response = toHttpResponse(error, 400);

      expect(response.headers.get('Content-Type')).toBe('application/json');
    });

    it('should return Response with correct body', async () => {
      const error = createApiError(ErrorCode.NOT_FOUND, 'Triple not found');
      const response = toHttpResponse(error, 404);

      const body = await response.json();
      expect(body).toEqual({
        error: {
          code: 'NOT_FOUND',
          message: 'Triple not found',
        },
      });
    });

    it('should include details in response body', async () => {
      const error = createApiError(
        ErrorCode.VALIDATION_ERROR,
        'Missing required parameter',
        { param: 'txId' }
      );
      const response = toHttpResponse(error, 400);

      const body = await response.json();
      expect(body.error.details).toEqual({ param: 'txId' });
    });
  });

  describe('ErrorCode', () => {
    it('should define standard error codes', () => {
      // Core error codes
      expect(ErrorCode.BAD_REQUEST).toBe('BAD_REQUEST');
      expect(ErrorCode.NOT_FOUND).toBe('NOT_FOUND');
      expect(ErrorCode.VALIDATION_ERROR).toBe('VALIDATION_ERROR');
      expect(ErrorCode.INTERNAL_ERROR).toBe('INTERNAL_ERROR');
      expect(ErrorCode.METHOD_NOT_ALLOWED).toBe('METHOD_NOT_ALLOWED');
      expect(ErrorCode.NOT_IMPLEMENTED).toBe('NOT_IMPLEMENTED');
      expect(ErrorCode.PARSE_ERROR).toBe('PARSE_ERROR');
    });
  });

  describe('HTTP status code consistency', () => {
    it('should use 400 for BAD_REQUEST', async () => {
      const error = createApiError(ErrorCode.BAD_REQUEST, 'Bad request');
      const response = toHttpResponse(error, 400);
      expect(response.status).toBe(400);
    });

    it('should use 404 for NOT_FOUND', async () => {
      const error = createApiError(ErrorCode.NOT_FOUND, 'Not found');
      const response = toHttpResponse(error, 404);
      expect(response.status).toBe(404);
    });

    it('should use 400 for VALIDATION_ERROR', async () => {
      const error = createApiError(ErrorCode.VALIDATION_ERROR, 'Validation failed');
      const response = toHttpResponse(error, 400);
      expect(response.status).toBe(400);
    });

    it('should use 500 for INTERNAL_ERROR', async () => {
      const error = createApiError(ErrorCode.INTERNAL_ERROR, 'Internal error');
      const response = toHttpResponse(error, 500);
      expect(response.status).toBe(500);
    });

    it('should use 405 for METHOD_NOT_ALLOWED', async () => {
      const error = createApiError(ErrorCode.METHOD_NOT_ALLOWED, 'Method not allowed');
      const response = toHttpResponse(error, 405);
      expect(response.status).toBe(405);
    });

    it('should use 501 for NOT_IMPLEMENTED', async () => {
      const error = createApiError(ErrorCode.NOT_IMPLEMENTED, 'Not implemented');
      const response = toHttpResponse(error, 501);
      expect(response.status).toBe(501);
    });
  });

  describe('ApiError type structure', () => {
    it('should have required error.code property', () => {
      const error = createApiError(ErrorCode.BAD_REQUEST, 'Test');
      expect(typeof error.error.code).toBe('string');
      expect(error.error.code.length).toBeGreaterThan(0);
    });

    it('should have required error.message property', () => {
      const error = createApiError(ErrorCode.BAD_REQUEST, 'Test message');
      expect(typeof error.error.message).toBe('string');
      expect(error.error.message).toBe('Test message');
    });

    it('should have optional error.details property', () => {
      const errorWithDetails = createApiError(ErrorCode.BAD_REQUEST, 'Test', { key: 'value' });
      const errorWithoutDetails = createApiError(ErrorCode.BAD_REQUEST, 'Test');

      expect(errorWithDetails.error.details).toBeDefined();
      expect(errorWithoutDetails.error.details).toBeUndefined();
    });
  });
});
