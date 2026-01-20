/**
 * Entity ID Validator
 *
 * Re-exports validation functions from core/validation.ts for backward compatibility.
 * The actual implementation now lives in core/ to prevent circular dependencies.
 *
 * @see ../core/validation.ts for implementation
 * @see test/security/entity-id-limits.test.ts for tests
 */

export {
  MAX_ID_LENGTH,
  EntityIdValidationError,
  EntityIdErrorCode,
  validateEntityId,
  isValidEntityIdFormat,
} from '../core/validation.js';
