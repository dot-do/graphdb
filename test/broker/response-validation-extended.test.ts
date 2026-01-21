/**
 * Extended Response Validation Tests
 *
 * TDD RED phase - tests for advanced response validation scenarios.
 * These tests define expected behavior for:
 * - Schema validation against expected types
 * - Deep validation of nested structures
 * - Partial response validation
 * - Response transformation and sanitization
 * - Error aggregation and reporting
 */

import { describe, it, expect, vi } from 'vitest';
import {
  validateShardResponse,
  isShardError,
  type ShardResponse,
  type ShardError,
} from '../../src/broker/response-validator';

describe('Schema Validation', () => {
  describe('Type checking', () => {
    it('should validate string fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
          description: 'A description',
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        description: { type: 'string', required: false },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should reject non-string for string fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 123, // Should be string
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('SCHEMA_VALIDATION_ERROR');
        expect(result.error.message).toContain('name');
        expect(result.error.message).toContain('string');
      }
    });

    it('should validate number fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          age: 25,
          score: 98.5,
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        age: { type: 'number', required: true },
        score: { type: 'number', required: false },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should validate boolean fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          isActive: true,
          isVerified: false,
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        isActive: { type: 'boolean', required: true },
        isVerified: { type: 'boolean', required: false },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should validate array fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          tags: ['tag1', 'tag2', 'tag3'],
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        tags: { type: 'array', items: { type: 'string' }, required: true },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should reject array with wrong item types', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          tags: ['tag1', 123, 'tag3'], // Mixed types
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        tags: { type: 'array', items: { type: 'string' }, required: true },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('SCHEMA_VALIDATION_ERROR');
        expect(result.error.message).toContain('tags');
      }
    });
  });

  describe('Required fields', () => {
    it('should fail when required field is missing', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          // name is missing
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.code).toBe('SCHEMA_VALIDATION_ERROR');
        expect(result.error.message).toContain('name');
        expect(result.error.message).toContain('required');
      }
    });

    it('should pass when optional field is missing', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
          // description is optional and missing
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        description: { type: 'string', required: false },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should allow null for nullable fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
          middleName: null,
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        middleName: { type: 'string', nullable: true, required: false },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });
  });
});

describe('Deep Validation', () => {
  describe('Nested object validation', () => {
    it('should validate nested objects', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/user/1',
          name: 'John',
          address: {
            street: '123 Main St',
            city: 'Springfield',
            zipCode: '12345',
          },
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        address: {
          type: 'object',
          required: true,
          properties: {
            street: { type: 'string', required: true },
            city: { type: 'string', required: true },
            zipCode: { type: 'string', required: true },
          },
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should fail on invalid nested field', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/user/1',
          name: 'John',
          address: {
            street: '123 Main St',
            city: 12345, // Should be string
            zipCode: '12345',
          },
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        address: {
          type: 'object',
          required: true,
          properties: {
            street: { type: 'string', required: true },
            city: { type: 'string', required: true },
            zipCode: { type: 'string', required: true },
          },
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toContain('address.city');
      }
    });

    it('should validate deeply nested structures', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/org/1',
          name: 'Acme Corp',
          departments: [
            {
              name: 'Engineering',
              manager: {
                name: 'Jane Doe',
                contact: {
                  email: 'jane@acme.com',
                  phone: '555-1234',
                },
              },
            },
          ],
        },
      };

      const contactSchema = {
        type: 'object',
        properties: {
          email: { type: 'string', required: true },
          phone: { type: 'string', required: false },
        },
      };

      const managerSchema = {
        type: 'object',
        properties: {
          name: { type: 'string', required: true },
          contact: contactSchema,
        },
      };

      const departmentSchema = {
        type: 'object',
        properties: {
          name: { type: 'string', required: true },
          manager: managerSchema,
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        departments: {
          type: 'array',
          items: departmentSchema,
          required: true,
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });
  });

  describe('Array of objects validation', () => {
    it('should validate all items in array', () => {
      const response = {
        success: true,
        data: [
          { $id: 'https://example.com/1', name: 'Item 1', price: 10.99 },
          { $id: 'https://example.com/2', name: 'Item 2', price: 20.99 },
          { $id: 'https://example.com/3', name: 'Item 3', price: 30.99 },
        ],
      };

      const itemSchema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        price: { type: 'number', required: true },
      };

      const result = validateShardResponse(response, { itemSchema });

      expect(result.success).toBe(true);
    });

    it('should report which array item failed validation', () => {
      const response = {
        success: true,
        data: [
          { $id: 'https://example.com/1', name: 'Item 1', price: 10.99 },
          { $id: 'https://example.com/2', name: 123, price: 20.99 }, // Invalid name
          { $id: 'https://example.com/3', name: 'Item 3', price: 30.99 },
        ],
      };

      const itemSchema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        price: { type: 'number', required: true },
      };

      const result = validateShardResponse(response, { itemSchema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toContain('[1]'); // Index of failing item
        expect(result.error.message).toContain('name');
      }
    });
  });
});

describe('Partial Response Validation', () => {
  describe('Allowing partial data', () => {
    it('should accept partial data when partial mode enabled', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          // Only $id is present, other fields missing
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        age: { type: 'number', required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        allowPartial: true,
        partialFields: ['$id'], // Only $id is required in partial mode
      });

      expect(result.success).toBe(true);
    });

    it('should indicate which fields are missing in partial response', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        age: { type: 'number', required: true },
        email: { type: 'string', required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        allowPartial: true,
        trackMissingFields: true,
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.missingFields).toContain('age');
        expect(result.missingFields).toContain('email');
        expect(result.isPartial).toBe(true);
      }
    });
  });

  describe('Projection validation', () => {
    it('should validate only projected fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
          // Other fields excluded by projection
        },
      };

      const fullSchema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        age: { type: 'number', required: true },
        email: { type: 'string', required: true },
      };

      const projection = ['$id', 'name'];

      const result = validateShardResponse(response, {
        schema: fullSchema,
        projection,
      });

      expect(result.success).toBe(true);
    });
  });
});

describe('Response Transformation', () => {
  describe('Field sanitization', () => {
    it('should remove internal fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          name: 'Test',
          _internal: 'should be removed',
          _debug: { timing: 123 },
          publicField: 'keep this',
        },
      };

      const result = validateShardResponse(response, {
        sanitize: true,
        removeFields: ['_internal', '_debug'],
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data).not.toHaveProperty('_internal');
        expect(result.data).not.toHaveProperty('_debug');
        expect(result.data).toHaveProperty('publicField');
      }
    });

    it('should redact sensitive fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/user/1',
          name: 'John Doe',
          email: 'john@example.com',
          ssn: '123-45-6789',
          password: 'secret123',
        },
      };

      const result = validateShardResponse(response, {
        sanitize: true,
        redactFields: ['ssn', 'password'],
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.ssn).toBe('[REDACTED]');
        expect(result.data.password).toBe('[REDACTED]');
        expect(result.data.email).toBe('john@example.com'); // Not redacted
      }
    });
  });

  describe('Type coercion', () => {
    it('should coerce string numbers to numbers', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          count: '42',
          price: '19.99',
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        count: { type: 'number', required: true },
        price: { type: 'number', required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        coerceTypes: true,
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.count).toBe(42);
        expect(result.data.price).toBe(19.99);
        expect(typeof result.data.count).toBe('number');
      }
    });

    it('should coerce string booleans to booleans', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          isActive: 'true',
          isDeleted: 'false',
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        isActive: { type: 'boolean', required: true },
        isDeleted: { type: 'boolean', required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        coerceTypes: true,
      });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.isActive).toBe(true);
        expect(result.data.isDeleted).toBe(false);
        expect(typeof result.data.isActive).toBe('boolean');
      }
    });
  });
});

describe('Error Aggregation', () => {
  describe('Multiple validation errors', () => {
    it('should collect all validation errors', () => {
      const response = {
        success: true,
        data: {
          $id: 123, // Should be string
          name: 456, // Should be string
          age: 'twenty', // Should be number
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        name: { type: 'string', required: true },
        age: { type: 'number', required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        collectAllErrors: true,
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.errors).toHaveLength(3);
        expect(result.error.errors).toContainEqual(
          expect.objectContaining({ field: '$id' })
        );
        expect(result.error.errors).toContainEqual(
          expect.objectContaining({ field: 'name' })
        );
        expect(result.error.errors).toContainEqual(
          expect.objectContaining({ field: 'age' })
        );
      }
    });

    it('should include error details for each field', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          email: 'not-an-email',
          age: -5,
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        email: { type: 'string', format: 'email', required: true },
        age: { type: 'number', minimum: 0, required: true },
      };

      const result = validateShardResponse(response, {
        schema,
        collectAllErrors: true,
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        const emailError = result.error.errors.find(e => e.field === 'email');
        expect(emailError).toBeDefined();
        expect(emailError?.constraint).toBe('format');
        expect(emailError?.expected).toBe('email');

        const ageError = result.error.errors.find(e => e.field === 'age');
        expect(ageError).toBeDefined();
        expect(ageError?.constraint).toBe('minimum');
        expect(ageError?.expected).toBe(0);
        expect(ageError?.actual).toBe(-5);
      }
    });
  });

  describe('Error paths', () => {
    it('should provide full path for nested errors', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          profile: {
            personal: {
              age: 'not a number',
            },
          },
        },
      };

      const schema = {
        $id: { type: 'string', required: true },
        profile: {
          type: 'object',
          properties: {
            personal: {
              type: 'object',
              properties: {
                age: { type: 'number', required: true },
              },
            },
          },
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.path).toBe('profile.personal.age');
      }
    });

    it('should provide index path for array errors', () => {
      const response = {
        success: true,
        data: {
          items: [
            { id: 1, value: 'valid' },
            { id: 2, value: 123 }, // Invalid
            { id: 3, value: 'valid' },
          ],
        },
      };

      const schema = {
        items: {
          type: 'array',
          items: {
            type: 'object',
            properties: {
              id: { type: 'number', required: true },
              value: { type: 'string', required: true },
            },
          },
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.path).toBe('items[1].value');
      }
    });
  });
});

describe('Custom Validators', () => {
  describe('Custom validation functions', () => {
    it('should apply custom validator', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          email: 'test@example.com',
        },
      };

      const emailValidator = (value: string) => {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        return emailRegex.test(value);
      };

      const schema = {
        $id: { type: 'string', required: true },
        email: {
          type: 'string',
          required: true,
          validator: emailValidator,
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(true);
    });

    it('should fail with custom validator error message', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          email: 'not-an-email',
        },
      };

      const emailValidator = (value: string) => {
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(value)) {
          return { valid: false, message: 'Invalid email format' };
        }
        return { valid: true };
      };

      const schema = {
        $id: { type: 'string', required: true },
        email: {
          type: 'string',
          required: true,
          validator: emailValidator,
        },
      };

      const result = validateShardResponse(response, { schema });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toContain('Invalid email format');
      }
    });
  });

  describe('Cross-field validation', () => {
    it('should validate relationships between fields', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          startDate: '2024-01-01',
          endDate: '2024-12-31',
        },
      };

      const crossFieldValidator = (data: Record<string, unknown>) => {
        const start = new Date(data['startDate'] as string);
        const end = new Date(data['endDate'] as string);
        return end > start;
      };

      const result = validateShardResponse(response, {
        crossFieldValidator,
      });

      expect(result.success).toBe(true);
    });

    it('should fail cross-field validation', () => {
      const response = {
        success: true,
        data: {
          $id: 'https://example.com/1',
          startDate: '2024-12-31',
          endDate: '2024-01-01', // End before start
        },
      };

      const crossFieldValidator = (data: Record<string, unknown>) => {
        const start = new Date(data['startDate'] as string);
        const end = new Date(data['endDate'] as string);
        if (end <= start) {
          return {
            valid: false,
            message: 'endDate must be after startDate',
            fields: ['startDate', 'endDate'],
          };
        }
        return { valid: true };
      };

      const result = validateShardResponse(response, {
        crossFieldValidator,
      });

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toContain('endDate must be after startDate');
      }
    });
  });
});
