/**
 * API Error Module
 *
 * Provides a consistent error response format across all handlers.
 * All API errors follow the same structure for predictable client handling.
 */

/**
 * Standard error codes for API responses
 */
export const ErrorCode = {
  BAD_REQUEST: 'BAD_REQUEST',
  NOT_FOUND: 'NOT_FOUND',
  VALIDATION_ERROR: 'VALIDATION_ERROR',
  INTERNAL_ERROR: 'INTERNAL_ERROR',
  METHOD_NOT_ALLOWED: 'METHOD_NOT_ALLOWED',
  NOT_IMPLEMENTED: 'NOT_IMPLEMENTED',
  PARSE_ERROR: 'PARSE_ERROR',
  UNAUTHORIZED: 'UNAUTHORIZED',
  FORBIDDEN: 'FORBIDDEN',
  CONFLICT: 'CONFLICT',
  TIMEOUT: 'TIMEOUT',
} as const;

export type ErrorCodeType = (typeof ErrorCode)[keyof typeof ErrorCode];

/**
 * Consistent API error structure
 *
 * All error responses follow this format:
 * {
 *   error: {
 *     code: string;       // Machine-readable error code
 *     message: string;    // Human-readable error message
 *     details?: Record<string, unknown>; // Optional additional context
 *   }
 * }
 */
export interface ApiError {
  error: {
    code: string;
    message: string;
    details?: Record<string, unknown>;
  };
}

/**
 * Create a standardized API error object
 *
 * @param code - Machine-readable error code (e.g., "NOT_FOUND", "VALIDATION_ERROR")
 * @param message - Human-readable error message
 * @param details - Optional additional context for the error
 * @returns ApiError object with consistent structure
 *
 * @example
 * // Simple error
 * createApiError(ErrorCode.NOT_FOUND, 'Triple not found')
 *
 * @example
 * // Error with details
 * createApiError(ErrorCode.VALIDATION_ERROR, 'Invalid input', {
 *   field: 'subject',
 *   reason: 'must be a valid URL'
 * })
 */
export function createApiError(
  code: string,
  message: string,
  details?: Record<string, unknown>
): ApiError {
  const error: ApiError = {
    error: {
      code,
      message,
    },
  };

  // Only include details if provided and non-empty
  if (details && Object.keys(details).length > 0) {
    error.error.details = details;
  }

  return error;
}

/**
 * Convert an ApiError to an HTTP Response
 *
 * @param error - ApiError object to convert
 * @param status - HTTP status code for the response
 * @returns Response object with JSON body and appropriate headers
 *
 * @example
 * // Return a 404 response
 * const error = createApiError(ErrorCode.NOT_FOUND, 'Resource not found');
 * return toHttpResponse(error, 404);
 *
 * @example
 * // Return a 400 response with details
 * const error = createApiError(ErrorCode.VALIDATION_ERROR, 'Invalid input', { field: 'name' });
 * return toHttpResponse(error, 400);
 */
export function toHttpResponse(error: ApiError, status: number): Response {
  return new Response(JSON.stringify(error), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}

/**
 * Mapping of error codes to HTTP status codes
 * Useful for consistent status code selection based on error type
 */
export const ErrorCodeToStatus: Record<ErrorCodeType, number> = {
  [ErrorCode.BAD_REQUEST]: 400,
  [ErrorCode.NOT_FOUND]: 404,
  [ErrorCode.VALIDATION_ERROR]: 400,
  [ErrorCode.INTERNAL_ERROR]: 500,
  [ErrorCode.METHOD_NOT_ALLOWED]: 405,
  [ErrorCode.NOT_IMPLEMENTED]: 501,
  [ErrorCode.PARSE_ERROR]: 400,
  [ErrorCode.UNAUTHORIZED]: 401,
  [ErrorCode.FORBIDDEN]: 403,
  [ErrorCode.CONFLICT]: 409,
  [ErrorCode.TIMEOUT]: 408,
};

/**
 * Create and return an HTTP error response in one step
 *
 * @param code - Error code (determines HTTP status)
 * @param message - Human-readable error message
 * @param details - Optional additional context
 * @returns Response object with JSON body
 *
 * @example
 * // Simple usage
 * return errorResponse(ErrorCode.NOT_FOUND, 'Triple not found');
 *
 * @example
 * // With details
 * return errorResponse(ErrorCode.VALIDATION_ERROR, 'Missing parameter', { param: 'txId' });
 */
export function errorResponse(
  code: ErrorCodeType,
  message: string,
  details?: Record<string, unknown>
): Response {
  const error = createApiError(code, message, details);
  const status = ErrorCodeToStatus[code];
  return toHttpResponse(error, status);
}

/**
 * WebSocket error response codes
 * Extended set of codes specifically for WebSocket error scenarios
 */
export const WsErrorCode = {
  ...ErrorCode,
  INVALID_REQUEST: 'INVALID_REQUEST',
  MISSING_ATTACHMENT: 'MISSING_ATTACHMENT',
  MISSING_PARAMETER: 'MISSING_PARAMETER',
  QUERY_FAILED: 'QUERY_FAILED',
  RPC_ERROR: 'RPC_ERROR',
  UNKNOWN_METHOD: 'UNKNOWN_METHOD',
} as const;

export type WsErrorCodeType = (typeof WsErrorCode)[keyof typeof WsErrorCode];

/**
 * Standard WebSocket error response structure
 *
 * All WebSocket error messages follow this format:
 * {
 *   type: 'error';
 *   code: string;       // Machine-readable error code (e.g., 'INVALID_REQUEST', 'NOT_FOUND')
 *   message: string;    // Human-readable error message
 *   id?: string;        // Request ID if available (for correlation)
 *   details?: Record<string, unknown>; // Optional additional context
 * }
 */
export interface WsErrorResponse {
  type: 'error';
  code: string;
  message: string;
  id?: string;
  details?: Record<string, unknown>;
}

/**
 * Create a standardized WebSocket error response object
 *
 * @param code - Machine-readable error code (e.g., "INVALID_REQUEST", "NOT_FOUND")
 * @param message - Human-readable error message
 * @param id - Optional request ID for correlation
 * @param details - Optional additional context for the error
 * @returns WsErrorResponse object with consistent structure
 *
 * @example
 * // Simple error
 * createWsError(WsErrorCode.INVALID_REQUEST, 'Missing required field')
 *
 * @example
 * // Error with request ID
 * createWsError(WsErrorCode.NOT_FOUND, 'Entity not found', 'req-123')
 *
 * @example
 * // Error with details
 * createWsError(WsErrorCode.MISSING_PARAMETER, 'Parameter required', undefined, {
 *   parameter: 'queryId'
 * })
 */
export function createWsError(
  code: string,
  message: string,
  id?: string,
  details?: Record<string, unknown>
): WsErrorResponse {
  const error: WsErrorResponse = {
    type: 'error',
    code,
    message,
  };

  // Only include id if provided
  if (id !== undefined) {
    error.id = id;
  }

  // Only include details if provided and non-empty
  if (details && Object.keys(details).length > 0) {
    error.details = details;
  }

  return error;
}

/**
 * Create a WebSocket error response and serialize to JSON string
 *
 * This is a convenience function for sending error responses over WebSocket.
 *
 * @param code - Machine-readable error code
 * @param message - Human-readable error message
 * @param id - Optional request ID for correlation
 * @param details - Optional additional context
 * @returns JSON string of the error response
 *
 * @example
 * // Send error over WebSocket
 * ws.send(wsErrorJson(WsErrorCode.INVALID_REQUEST, 'Invalid message format'))
 */
export function wsErrorJson(
  code: string,
  message: string,
  id?: string,
  details?: Record<string, unknown>
): string {
  return JSON.stringify(createWsError(code, message, id, details));
}
