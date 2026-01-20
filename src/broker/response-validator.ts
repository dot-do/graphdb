/**
 * Shard Response Validator
 *
 * Provides type-safe validation for shard responses, ensuring that:
 * - Error responses are properly detected and typed
 * - Success responses contain valid data
 * - Malformed responses are caught and handled gracefully
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Error details returned by a shard
 */
export interface ShardError {
  code: string;
  message: string;
  shardId?: string;
  [key: string]: unknown;
}

/**
 * Successful shard response
 */
export interface ShardSuccess<T> {
  success: true;
  data: T;
}

/**
 * Error shard response
 */
export interface ShardErrorResponse {
  success: false;
  error: ShardError;
}

/**
 * Discriminated union for shard responses
 * Can be either a success with data or an error with details
 */
export type ShardResponse<T> = ShardSuccess<T> | ShardErrorResponse;

// ============================================================================
// Validation Functions
// ============================================================================

/**
 * Type guard to check if a response is an error response
 *
 * @param response - The shard response to check
 * @returns true if the response is an error, false if success
 *
 * @example
 * const response = validateShardResponse(rawData);
 * if (isShardError(response)) {
 *   console.error(`Shard error: ${response.error.code}`);
 * } else {
 *   processData(response.data);
 * }
 */
export function isShardError(response: ShardResponse<unknown>): response is ShardErrorResponse {
  return response.success === false;
}

/**
 * Create a malformed response error
 */
function createMalformedError(message: string): ShardErrorResponse {
  return {
    success: false,
    error: {
      code: 'MALFORMED_RESPONSE',
      message,
    },
  };
}

/**
 * Check if a value is a plain object (not null, not array)
 */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Validate and type-check a raw shard response
 *
 * This function takes an unknown response value (typically parsed JSON)
 * and validates it against the expected ShardResponse structure.
 *
 * Supports two formats:
 * 1. New format: { success: true, data: T } or { success: false, error: {...} }
 * 2. Legacy format: raw array data (treated as success response for backward compatibility)
 *
 * @param response - The raw response to validate (typically from JSON.parse)
 * @returns A validated ShardResponse, either preserving the original or wrapping in an error
 *
 * @example
 * const rawData = await response.json();
 * const validated = validateShardResponse<Entity[]>(rawData);
 *
 * if (validated.success) {
 *   // TypeScript knows validated.data exists and is Entity[]
 *   for (const entity of validated.data) { ... }
 * } else {
 *   // TypeScript knows validated.error exists
 *   throw new Error(validated.error.message);
 * }
 */
export function validateShardResponse<T = unknown>(response: unknown): ShardResponse<T> {
  // Handle null/undefined
  if (response === null || response === undefined) {
    return createMalformedError('Response is null or undefined');
  }

  // Handle legacy format: raw arrays are treated as success responses
  // This maintains backward compatibility with existing shard implementations
  if (Array.isArray(response)) {
    return {
      success: true,
      data: response as T,
    };
  }

  // Handle non-object types (not array, not object)
  if (!isPlainObject(response)) {
    return createMalformedError('Response is not an object');
  }

  // Check for success field - if missing, treat as legacy format
  if (!('success' in response)) {
    return createMalformedError('Response is missing success field');
  }

  // Handle error response
  if (response['success'] === false) {
    // Validate error object
    if (!('error' in response) || !isPlainObject(response['error'])) {
      return createMalformedError('Error response is missing error object');
    }

    const error = response['error'];

    // Validate error has required fields
    if (typeof error['code'] !== 'string') {
      return createMalformedError('Error object is missing code field');
    }

    if (typeof error['message'] !== 'string') {
      return createMalformedError('Error object is missing message field');
    }

    // Return the validated error response
    return {
      success: false,
      error: error as ShardError,
    };
  }

  // Handle success response
  if (response['success'] === true) {
    // Validate data field exists
    if (!('data' in response)) {
      return createMalformedError('Success response is missing data field');
    }

    // Return the validated success response
    return {
      success: true,
      data: response['data'] as T,
    };
  }

  // success field exists but is not boolean
  return createMalformedError('Response success field is not a boolean');
}
