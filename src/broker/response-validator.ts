/**
 * Shard Response Validator
 *
 * Provides type-safe validation for shard responses, ensuring that:
 * - Error responses are properly detected and typed
 * - Success responses contain valid data
 * - Malformed responses are caught and handled gracefully
 * - Schema validation against expected types
 * - Deep validation of nested structures
 * - Partial response validation
 * - Response transformation and sanitization
 * - Error aggregation and reporting
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Validation error details for a specific field
 */
export interface ValidationErrorDetail {
  field: string;
  constraint?: string;
  expected?: unknown;
  actual?: unknown;
  message?: string;
}

/**
 * Error details returned by a shard
 */
export interface ShardError {
  code: string;
  message: string;
  shardId?: string;
  path?: string;
  errors?: ValidationErrorDetail[];
  [key: string]: unknown;
}

/**
 * Successful shard response
 */
export interface ShardSuccess<T> {
  success: true;
  data: T;
  missingFields?: string[];
  isPartial?: boolean;
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

/**
 * Schema field definition
 */
export interface SchemaField {
  type: 'string' | 'number' | 'boolean' | 'array' | 'object';
  required?: boolean;
  nullable?: boolean;
  items?: SchemaField;
  properties?: Record<string, SchemaField>;
  format?: string;
  minimum?: number;
  maximum?: number;
  validator?: (value: unknown) => boolean | { valid: boolean; message?: string };
}

/**
 * Schema definition for validation
 */
export type Schema = Record<string, SchemaField>;

/**
 * Validation result from custom validators
 */
export interface CustomValidatorResult {
  valid: boolean;
  message?: string;
  fields?: string[];
}

/**
 * Cross-field validator function type
 */
export type CrossFieldValidator = (data: Record<string, unknown>) => boolean | CustomValidatorResult;

/**
 * Validation options
 */
export interface ValidationOptions {
  schema?: Schema;
  itemSchema?: Schema;
  allowPartial?: boolean;
  partialFields?: string[];
  trackMissingFields?: boolean;
  projection?: string[];
  sanitize?: boolean;
  removeFields?: string[];
  redactFields?: string[];
  coerceTypes?: boolean;
  collectAllErrors?: boolean;
  crossFieldValidator?: CrossFieldValidator;
}

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
 * Create a schema validation error
 */
function createSchemaError(
  message: string,
  path?: string,
  errors?: ValidationErrorDetail[]
): ShardErrorResponse {
  return {
    success: false,
    error: {
      code: 'SCHEMA_VALIDATION_ERROR',
      message,
      path,
      errors,
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
 * Validate a value against a schema field definition
 */
function validateField(
  value: unknown,
  schema: SchemaField,
  path: string,
  options: ValidationOptions,
  errors: ValidationErrorDetail[]
): { valid: boolean; coercedValue?: unknown } {
  // Handle nullable
  if (value === null && schema.nullable) {
    return { valid: true, coercedValue: null };
  }

  // Handle missing value (undefined)
  if (value === undefined) {
    if (schema.required && !options.allowPartial) {
      errors.push({
        field: path,
        constraint: 'required',
        message: `Field '${path}' is required`,
      });
      return { valid: false };
    }
    return { valid: true };
  }

  // Type coercion
  let coercedValue = value;
  if (options.coerceTypes) {
    if (schema.type === 'number' && typeof value === 'string') {
      const parsed = parseFloat(value);
      if (!isNaN(parsed)) {
        coercedValue = parsed;
      }
    } else if (schema.type === 'boolean' && typeof value === 'string') {
      if (value === 'true') coercedValue = true;
      else if (value === 'false') coercedValue = false;
    }
  }

  // Type validation
  let typeValid = false;
  switch (schema.type) {
    case 'string':
      typeValid = typeof coercedValue === 'string';
      break;
    case 'number':
      typeValid = typeof coercedValue === 'number' && !isNaN(coercedValue);
      break;
    case 'boolean':
      typeValid = typeof coercedValue === 'boolean';
      break;
    case 'array':
      typeValid = Array.isArray(coercedValue);
      break;
    case 'object':
      typeValid = isPlainObject(coercedValue);
      break;
  }

  if (!typeValid) {
    errors.push({
      field: path,
      constraint: 'type',
      expected: schema.type,
      actual: typeof coercedValue,
      message: `Field '${path}' expected type '${schema.type}' but got '${typeof coercedValue}'`,
    });
    return { valid: false };
  }

  // Format validation
  if (schema.format && typeof coercedValue === 'string') {
    if (schema.format === 'email') {
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(coercedValue)) {
        errors.push({
          field: path,
          constraint: 'format',
          expected: 'email',
          actual: coercedValue,
          message: `Field '${path}' is not a valid email`,
        });
        return { valid: false };
      }
    }
  }

  // Minimum/maximum validation for numbers
  if (schema.type === 'number' && typeof coercedValue === 'number') {
    if (schema.minimum !== undefined && coercedValue < schema.minimum) {
      errors.push({
        field: path,
        constraint: 'minimum',
        expected: schema.minimum,
        actual: coercedValue,
        message: `Field '${path}' must be at least ${schema.minimum}`,
      });
      return { valid: false };
    }
    if (schema.maximum !== undefined && coercedValue > schema.maximum) {
      errors.push({
        field: path,
        constraint: 'maximum',
        expected: schema.maximum,
        actual: coercedValue,
        message: `Field '${path}' must be at most ${schema.maximum}`,
      });
      return { valid: false };
    }
  }

  // Array item validation
  if (schema.type === 'array' && Array.isArray(coercedValue) && schema.items) {
    const arrayValue = coercedValue as unknown[];
    const coercedArray: unknown[] = [];
    let arrayValid = true;
    for (let i = 0; i < arrayValue.length; i++) {
      const itemResult = validateField(
        arrayValue[i],
        schema.items,
        `${path}[${i}]`,
        options,
        errors
      );
      if (!itemResult.valid) {
        arrayValid = false;
        if (!options.collectAllErrors) break;
      } else {
        coercedArray.push(itemResult.coercedValue ?? arrayValue[i]);
      }
    }
    if (!arrayValid) return { valid: false };
    coercedValue = coercedArray;
  }

  // Nested object validation
  if (schema.type === 'object' && isPlainObject(coercedValue) && schema.properties) {
    const objValue = coercedValue as Record<string, unknown>;
    const coercedObj: Record<string, unknown> = { ...objValue };
    let objValid = true;
    for (const [key, propSchema] of Object.entries(schema.properties)) {
      const propResult = validateField(
        objValue[key],
        propSchema,
        `${path}.${key}`,
        options,
        errors
      );
      if (!propResult.valid) {
        objValid = false;
        if (!options.collectAllErrors) break;
      } else if (propResult.coercedValue !== undefined) {
        coercedObj[key] = propResult.coercedValue;
      }
    }
    if (!objValid) return { valid: false };
    coercedValue = coercedObj;
  }

  // Custom validator
  if (schema.validator) {
    const validatorResult = schema.validator(coercedValue);
    if (validatorResult === false) {
      errors.push({
        field: path,
        constraint: 'custom',
        message: `Field '${path}' failed custom validation`,
      });
      return { valid: false };
    } else if (typeof validatorResult === 'object' && !validatorResult.valid) {
      errors.push({
        field: path,
        constraint: 'custom',
        message: validatorResult.message || `Field '${path}' failed custom validation`,
      });
      return { valid: false };
    }
  }

  return { valid: true, coercedValue };
}

/**
 * Validate data against a schema
 */
function validateSchema(
  data: Record<string, unknown>,
  schema: Schema,
  options: ValidationOptions,
  basePath: string = ''
): { valid: boolean; errors: ValidationErrorDetail[]; coercedData: Record<string, unknown>; missingFields?: string[] } {
  const errors: ValidationErrorDetail[] = [];
  const coercedData: Record<string, unknown> = { ...data };
  const missingFields: string[] = [];

  // Apply projection if specified - only validate projected fields
  const fieldsToValidate = options.projection || Object.keys(schema);

  for (const fieldName of fieldsToValidate) {
    const fieldSchema = schema[fieldName];
    if (!fieldSchema) continue;

    const path = basePath ? `${basePath}.${fieldName}` : fieldName;
    const value = data[fieldName];

    // Track missing fields when enabled
    if (options.trackMissingFields && value === undefined && fieldSchema.required) {
      missingFields.push(fieldName);
    }

    // Skip validation for missing fields in partial mode if not in partialFields
    if (options.allowPartial && value === undefined) {
      if (options.partialFields && !options.partialFields.includes(fieldName)) {
        continue;
      }
    }

    const result = validateField(value, fieldSchema, path, options, errors);
    if (result.valid && result.coercedValue !== undefined) {
      coercedData[fieldName] = result.coercedValue;
    }

    if (!result.valid && !options.collectAllErrors) {
      break;
    }
  }

  return { valid: errors.length === 0, errors, coercedData, missingFields };
}

/**
 * Apply sanitization transformations to data
 */
function sanitizeData(
  data: Record<string, unknown>,
  options: ValidationOptions
): Record<string, unknown> {
  const result = { ...data };

  // Remove specified fields
  if (options.removeFields) {
    for (const field of options.removeFields) {
      delete result[field];
    }
  }

  // Redact specified fields
  if (options.redactFields) {
    for (const field of options.redactFields) {
      if (field in result) {
        result[field] = '[REDACTED]';
      }
    }
  }

  return result;
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
 * @param options - Optional validation options including schema, sanitization, etc.
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
export function validateShardResponse<T = unknown>(
  response: unknown,
  options?: ValidationOptions
): ShardResponse<T> {
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

    const rawData = response['data'];

    // Apply cross-field validation first if specified
    if (options?.crossFieldValidator && isPlainObject(rawData)) {
      const crossResult = options.crossFieldValidator(rawData as Record<string, unknown>);
      if (crossResult === false) {
        return createSchemaError('Cross-field validation failed');
      } else if (typeof crossResult === 'object' && !crossResult.valid) {
        return createSchemaError(
          crossResult.message || 'Cross-field validation failed',
          undefined,
          crossResult.fields?.map((f) => ({ field: f, constraint: 'cross-field' }))
        );
      }
    }

    // Handle array data with itemSchema
    if (Array.isArray(rawData) && options?.itemSchema) {
      const validatedItems: unknown[] = [];
      for (let i = 0; i < rawData.length; i++) {
        const item = rawData[i];
        if (!isPlainObject(item)) {
          return createSchemaError(`Item at index [${i}] is not an object`, `[${i}]`);
        }
        const validation = validateSchema(item, options.itemSchema, options, `[${i}]`);
        if (!validation.valid) {
          const firstError = validation.errors[0];
          return createSchemaError(
            `Validation failed at ${firstError.field}: ${firstError.message}`,
            firstError.field,
            options.collectAllErrors ? validation.errors : undefined
          );
        }
        validatedItems.push(validation.coercedData);
      }
      return {
        success: true,
        data: (options.coerceTypes ? validatedItems : rawData) as T,
      };
    }

    // Apply schema validation if specified
    if (options?.schema && isPlainObject(rawData)) {
      const validation = validateSchema(rawData, options.schema, options);
      if (!validation.valid) {
        const firstError = validation.errors[0];
        return createSchemaError(
          `Validation failed at ${firstError.field}: ${firstError.message}`,
          firstError.field,
          options.collectAllErrors ? validation.errors : undefined
        );
      }

      let resultData = options.coerceTypes ? validation.coercedData : rawData;

      // Apply sanitization if specified
      if (options.sanitize) {
        resultData = sanitizeData(resultData as Record<string, unknown>, options);
      }

      // Build result with optional metadata
      const result: ShardSuccess<T> = {
        success: true,
        data: resultData as T,
      };

      // Track missing fields if enabled
      if (options.trackMissingFields && validation.missingFields && validation.missingFields.length > 0) {
        result.missingFields = validation.missingFields;
        result.isPartial = true;
      }

      return result;
    }

    // Apply sanitization even without schema validation
    if (options?.sanitize && isPlainObject(rawData)) {
      return {
        success: true,
        data: sanitizeData(rawData, options) as T,
      };
    }

    // Return the validated success response
    return {
      success: true,
      data: rawData as T,
    };
  }

  // success field exists but is not boolean
  return createMalformedError('Response success field is not a boolean');
}
