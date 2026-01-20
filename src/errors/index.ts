/**
 * Error Module Exports
 */

export {
  // HTTP API error helpers
  createApiError,
  toHttpResponse,
  errorResponse,
  ErrorCode,
  ErrorCodeToStatus,
  type ApiError,
  type ErrorCodeType,
  // WebSocket error helpers
  createWsError,
  wsErrorJson,
  WsErrorCode,
  type WsErrorResponse,
  type WsErrorCodeType,
} from './api-error.js';
