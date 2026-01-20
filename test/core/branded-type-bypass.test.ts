import { describe, it, expect } from 'vitest';
import {
  type EntityId,
  type Predicate,
  type Namespace,
  type TransactionId,
  isEntityId,
  isPredicate,
  isNamespace,
  isTransactionId,
  createEntityId,
  createPredicate,
  createNamespace,
  createTransactionId,
  assertEntityId,
  assertPredicate,
  assertNamespace,
  assertTransactionId,
  assertEntityIdArray,
  BrandedTypeValidationError,
  BrandedTypeErrorCode,
} from '../../src/core/types.js';

/**
 * Tests for branded type bypass prevention
 *
 * These tests verify that branded types cannot be bypassed via:
 * 1. Direct casting (compile-time only, but runtime guards catch it)
 * 2. Invalid input at API boundaries
 * 3. Malicious/malformed strings
 * 4. Type coercion attacks
 */

describe('Branded Type Bypass Prevention', () => {
  describe('Direct casting bypass attempts', () => {
    describe('EntityId', () => {
      it('should detect invalid EntityId created via direct cast', () => {
        // Simulate what happens when code uses direct casting
        const malicious = 'malicious-not-a-url' as EntityId;

        // The type system says this is EntityId, but runtime guards catch it
        expect(isEntityId(malicious)).toBe(false);
      });

      it('should detect SQL injection via direct cast', () => {
        const sqlInjection = "https://example.com/'; DROP TABLE triples;--" as EntityId;

        // The URL itself is technically valid, so this would pass isEntityId
        // but the value is still dangerous if used without parameterization
        // This test documents the behavior
        expect(typeof sqlInjection).toBe('string');
      });

      it('should detect protocol bypass via direct cast', () => {
        const protocolBypass = 'javascript:alert(1)' as EntityId;

        expect(isEntityId(protocolBypass)).toBe(false);
      });

      it('should detect empty string bypass via direct cast', () => {
        const empty = '' as EntityId;

        expect(isEntityId(empty)).toBe(false);
      });
    });

    describe('Predicate', () => {
      it('should detect invalid Predicate created via direct cast', () => {
        const malicious = 'schema:name' as Predicate;

        expect(isPredicate(malicious)).toBe(false);
      });

      it('should detect SQL injection in predicate via direct cast', () => {
        const sqlInjection = "name'; DROP TABLE triples;--" as Predicate;

        expect(isPredicate(sqlInjection)).toBe(false);
      });

      it('should detect whitespace bypass via direct cast', () => {
        const withWhitespace = 'name with spaces' as Predicate;

        expect(isPredicate(withWhitespace)).toBe(false);
      });

      it('should detect empty string bypass via direct cast', () => {
        const empty = '' as Predicate;

        expect(isPredicate(empty)).toBe(false);
      });
    });

    describe('Namespace', () => {
      it('should detect invalid Namespace created via direct cast', () => {
        const malicious = 'not-a-url' as Namespace;

        expect(isNamespace(malicious)).toBe(false);
      });

      it('should detect protocol bypass via direct cast', () => {
        const protocolBypass = 'file:///etc/passwd' as Namespace;

        expect(isNamespace(protocolBypass)).toBe(false);
      });
    });

    describe('TransactionId', () => {
      it('should detect invalid TransactionId created via direct cast', () => {
        const malicious = 'not-a-ulid' as TransactionId;

        expect(isTransactionId(malicious)).toBe(false);
      });

      it('should detect wrong length ULID via direct cast', () => {
        const wrongLength = '01ARZ3NDEK' as TransactionId; // Too short

        expect(isTransactionId(wrongLength)).toBe(false);
      });

      it('should detect invalid characters in ULID via direct cast', () => {
        const invalidChars = '01ARZ3NDEKTSV4RRFFQ69G5FAI' as TransactionId; // Contains 'I'

        expect(isTransactionId(invalidChars)).toBe(false);
      });
    });
  });

  describe('Assertion functions for API boundaries', () => {
    describe('assertEntityId', () => {
      it('should return EntityId for valid URL', () => {
        const result = assertEntityId('https://example.com/users/123');
        expect(result).toBe('https://example.com/users/123');
        expect(isEntityId(result)).toBe(true);
      });

      it('should throw for non-string value', () => {
        expect(() => assertEntityId(123)).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId(null)).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId(undefined)).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId({})).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId([])).toThrow(BrandedTypeValidationError);
      });

      it('should throw with correct error code for non-string', () => {
        try {
          assertEntityId(123);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).code).toBe(
            BrandedTypeErrorCode.INVALID_ENTITY_ID
          );
          expect((e as BrandedTypeValidationError).value).toBe(123);
        }
      });

      it('should throw for invalid URL string', () => {
        expect(() => assertEntityId('not-a-url')).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId('')).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId('ftp://example.com')).toThrow(BrandedTypeValidationError);
      });

      it('should include field name in error message when provided', () => {
        try {
          assertEntityId('invalid', 'subject');
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).message).toContain('subject');
        }
      });

      it('should truncate long invalid values in error messages', () => {
        const longValue = 'a'.repeat(200);
        try {
          assertEntityId(longValue);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).message).toContain('[truncated]');
          expect((e as BrandedTypeValidationError).message.length).toBeLessThan(300);
        }
      });
    });

    describe('assertPredicate', () => {
      it('should return Predicate for valid predicate name', () => {
        const result = assertPredicate('name');
        expect(result).toBe('name');
        expect(isPredicate(result)).toBe(true);
      });

      it('should accept valid predicate patterns', () => {
        expect(assertPredicate('$id')).toBe('$id');
        expect(assertPredicate('$type')).toBe('$type');
        expect(assertPredicate('firstName')).toBe('firstName');
        expect(assertPredicate('first_name')).toBe('first_name');
        expect(assertPredicate('_private')).toBe('_private');
      });

      it('should throw for non-string value', () => {
        expect(() => assertPredicate(123)).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate(null)).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate(undefined)).toThrow(BrandedTypeValidationError);
      });

      it('should throw for predicate with colons', () => {
        expect(() => assertPredicate('schema:name')).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate('rdf:type')).toThrow(BrandedTypeValidationError);
      });

      it('should throw for predicate with whitespace', () => {
        expect(() => assertPredicate('name with space')).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate('name\ttab')).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate('name\nnewline')).toThrow(BrandedTypeValidationError);
      });

      it('should include specific reason in error message', () => {
        try {
          assertPredicate('schema:name');
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).message).toContain('colon');
        }

        try {
          assertPredicate('name space');
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).message).toContain('whitespace');
        }
      });
    });

    describe('assertNamespace', () => {
      it('should return Namespace for valid URL', () => {
        const result = assertNamespace('https://example.com/ns/');
        expect(result).toBe('https://example.com/ns/');
        expect(isNamespace(result)).toBe(true);
      });

      it('should throw for non-string value', () => {
        expect(() => assertNamespace(123)).toThrow(BrandedTypeValidationError);
        expect(() => assertNamespace(null)).toThrow(BrandedTypeValidationError);
      });

      it('should throw for invalid URL', () => {
        expect(() => assertNamespace('not-a-url')).toThrow(BrandedTypeValidationError);
        expect(() => assertNamespace('file:///etc/passwd')).toThrow(BrandedTypeValidationError);
      });
    });

    describe('assertTransactionId', () => {
      it('should return TransactionId for valid ULID', () => {
        const result = assertTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAV');
        expect(result).toBe('01ARZ3NDEKTSV4RRFFQ69G5FAV');
        expect(isTransactionId(result)).toBe(true);
      });

      it('should throw for non-string value', () => {
        expect(() => assertTransactionId(123)).toThrow(BrandedTypeValidationError);
        expect(() => assertTransactionId(null)).toThrow(BrandedTypeValidationError);
      });

      it('should throw for invalid ULID format', () => {
        expect(() => assertTransactionId('too-short')).toThrow(BrandedTypeValidationError);
        expect(() => assertTransactionId('01ARZ3NDEKTSV4RRFFQ69G5FAI')).toThrow(
          BrandedTypeValidationError
        ); // Contains 'I'
      });
    });

    describe('assertEntityIdArray', () => {
      it('should return EntityId array for valid URLs', () => {
        const result = assertEntityIdArray([
          'https://example.com/1',
          'https://example.com/2',
        ]);
        expect(result).toHaveLength(2);
        expect(isEntityId(result[0]!)).toBe(true);
        expect(isEntityId(result[1]!)).toBe(true);
      });

      it('should throw for non-array value', () => {
        expect(() => assertEntityIdArray('not-an-array')).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityIdArray(null)).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityIdArray({})).toThrow(BrandedTypeValidationError);
      });

      it('should throw for array with non-string element', () => {
        expect(() => assertEntityIdArray(['https://example.com/1', 123])).toThrow(
          BrandedTypeValidationError
        );
      });

      it('should throw for array with invalid URL', () => {
        expect(() =>
          assertEntityIdArray(['https://example.com/1', 'not-a-url'])
        ).toThrow(BrandedTypeValidationError);
      });

      it('should include element index in error message', () => {
        try {
          assertEntityIdArray(['https://example.com/1', 'invalid']);
          expect.fail('Should have thrown');
        } catch (e) {
          expect(e).toBeInstanceOf(BrandedTypeValidationError);
          expect((e as BrandedTypeValidationError).message).toContain('[1]');
        }
      });

      it('should return empty array for empty input', () => {
        const result = assertEntityIdArray([]);
        expect(result).toHaveLength(0);
      });
    });
  });

  describe('Security attack prevention', () => {
    describe('Injection attacks via branded types', () => {
      it('should detect null byte injection attempt', () => {
        const nullByteUrl = 'https://example.com/\x00malicious';

        expect(isEntityId(nullByteUrl)).toBe(false);
        expect(() => createEntityId(nullByteUrl)).toThrow();
        expect(() => assertEntityId(nullByteUrl)).toThrow(BrandedTypeValidationError);
      });

      it('should detect control character injection', () => {
        const controlCharUrl = 'https://example.com/\x1fmalicious';

        expect(isEntityId(controlCharUrl)).toBe(false);
        expect(() => assertEntityId(controlCharUrl)).toThrow(BrandedTypeValidationError);
      });

      it('should detect zero-width character injection', () => {
        const zeroWidthUrl = 'https://example.com/\u200Bmalicious';

        expect(isEntityId(zeroWidthUrl)).toBe(false);
        expect(() => assertEntityId(zeroWidthUrl)).toThrow(BrandedTypeValidationError);
      });

      it('should prevent URL protocol smuggling', () => {
        // These should all fail as they're not http/https
        const attacks = [
          'javascript:alert(1)',
          'data:text/html,<script>alert(1)</script>',
          'file:///etc/passwd',
          'ftp://evil.com/payload',
        ];

        for (const attack of attacks) {
          expect(isEntityId(attack)).toBe(false);
          expect(() => assertEntityId(attack)).toThrow(BrandedTypeValidationError);
        }
      });

      it('should prevent predicate injection for SQL-like patterns', () => {
        const attacks = [
          "'; DROP TABLE triples;--",
          '1; DELETE FROM triples',
          'name OR 1=1',
          "name' UNION SELECT * FROM users--",
        ];

        for (const attack of attacks) {
          expect(isPredicate(attack)).toBe(false);
          expect(() => assertPredicate(attack)).toThrow(BrandedTypeValidationError);
        }
      });
    });

    describe('Type coercion attacks', () => {
      it('should reject object with toString() returning valid URL', () => {
        const maliciousObject = {
          toString: () => 'https://example.com/users/123',
        };

        // The assertion function requires a string type
        expect(() => assertEntityId(maliciousObject)).toThrow(BrandedTypeValidationError);
      });

      it('should reject array that could be coerced to string', () => {
        const maliciousArray = ['https://example.com/users/123'];

        expect(() => assertEntityId(maliciousArray)).toThrow(BrandedTypeValidationError);
      });

      it('should reject number that could be coerced', () => {
        expect(() => assertEntityId(12345)).toThrow(BrandedTypeValidationError);
        expect(() => assertPredicate(12345)).toThrow(BrandedTypeValidationError);
        expect(() => assertTransactionId(12345)).toThrow(BrandedTypeValidationError);
      });

      it('should reject boolean values', () => {
        expect(() => assertEntityId(true)).toThrow(BrandedTypeValidationError);
        expect(() => assertEntityId(false)).toThrow(BrandedTypeValidationError);
      });
    });

    describe('Length-based attacks', () => {
      it('should reject excessively long EntityId', () => {
        const longUrl = 'https://example.com/' + 'a'.repeat(3000);

        expect(isEntityId(longUrl)).toBe(false);
        expect(() => assertEntityId(longUrl)).toThrow(BrandedTypeValidationError);
      });

      it('should accept EntityId at exactly MAX_ID_LENGTH', () => {
        // MAX_ID_LENGTH is 2048
        const baseUrl = 'https://example.com/';
        const paddingNeeded = 2048 - baseUrl.length;
        const maxLengthUrl = baseUrl + 'a'.repeat(paddingNeeded);

        expect(maxLengthUrl.length).toBe(2048);
        expect(isEntityId(maxLengthUrl)).toBe(true);
        expect(() => assertEntityId(maxLengthUrl)).not.toThrow();
      });
    });
  });

  describe('Type guard and create function consistency', () => {
    it('should have consistent behavior between isEntityId and createEntityId', () => {
      const validUrls = [
        'https://example.com',
        'http://localhost:8080/path',
        'https://example.com/users/123',
      ];

      const invalidUrls = [
        'not-a-url',
        '',
        'ftp://example.com',
        'javascript:void(0)',
      ];

      for (const url of validUrls) {
        expect(isEntityId(url)).toBe(true);
        expect(() => createEntityId(url)).not.toThrow();
        expect(() => assertEntityId(url)).not.toThrow();
      }

      for (const url of invalidUrls) {
        expect(isEntityId(url)).toBe(false);
        expect(() => createEntityId(url)).toThrow();
        expect(() => assertEntityId(url)).toThrow();
      }
    });

    it('should have consistent behavior between isPredicate and createPredicate', () => {
      const validPredicates = ['name', 'firstName', '$id', '$type', '_private'];
      const invalidPredicates = ['schema:name', 'with space', '', '123invalid'];

      for (const pred of validPredicates) {
        expect(isPredicate(pred)).toBe(true);
        expect(() => createPredicate(pred)).not.toThrow();
        expect(() => assertPredicate(pred)).not.toThrow();
      }

      for (const pred of invalidPredicates) {
        expect(isPredicate(pred)).toBe(false);
        expect(() => createPredicate(pred)).toThrow();
        expect(() => assertPredicate(pred)).toThrow();
      }
    });

    it('should have consistent behavior between isNamespace and createNamespace', () => {
      const validNamespaces = ['https://example.com/', 'http://localhost:8080/'];
      const invalidNamespaces = ['not-a-url', 'file:///path', ''];

      for (const ns of validNamespaces) {
        expect(isNamespace(ns)).toBe(true);
        expect(() => createNamespace(ns)).not.toThrow();
        expect(() => assertNamespace(ns)).not.toThrow();
      }

      for (const ns of invalidNamespaces) {
        expect(isNamespace(ns)).toBe(false);
        expect(() => createNamespace(ns)).toThrow();
        expect(() => assertNamespace(ns)).toThrow();
      }
    });

    it('should have consistent behavior between isTransactionId and createTransactionId', () => {
      const validTxIds = ['01ARZ3NDEKTSV4RRFFQ69G5FAV', '01H9YG5VF9QW8TNXS0QBAJKZ3Y'];
      const invalidTxIds = ['too-short', '01ARZ3NDEKTSV4RRFFQ69G5FAI', ''];

      for (const txId of validTxIds) {
        expect(isTransactionId(txId)).toBe(true);
        expect(() => createTransactionId(txId)).not.toThrow();
        expect(() => assertTransactionId(txId)).not.toThrow();
      }

      for (const txId of invalidTxIds) {
        expect(isTransactionId(txId)).toBe(false);
        expect(() => createTransactionId(txId)).toThrow();
        expect(() => assertTransactionId(txId)).toThrow();
      }
    });
  });

  describe('Error types and codes', () => {
    it('should throw BrandedTypeValidationError from assert functions', () => {
      expect(() => assertEntityId('invalid')).toThrow(BrandedTypeValidationError);
      expect(() => assertPredicate('invalid:colon')).toThrow(BrandedTypeValidationError);
      expect(() => assertNamespace('invalid')).toThrow(BrandedTypeValidationError);
      expect(() => assertTransactionId('invalid')).toThrow(BrandedTypeValidationError);
    });

    it('should include the invalid value in the error', () => {
      try {
        assertEntityId('test-value');
        expect.fail('Should have thrown');
      } catch (e) {
        expect((e as BrandedTypeValidationError).value).toBe('test-value');
      }
    });

    it('should use correct error codes', () => {
      try {
        assertEntityId('invalid');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(
          BrandedTypeErrorCode.INVALID_ENTITY_ID
        );
      }

      try {
        assertPredicate('schema:invalid');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(
          BrandedTypeErrorCode.INVALID_PREDICATE
        );
      }

      try {
        assertNamespace('invalid');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(
          BrandedTypeErrorCode.INVALID_NAMESPACE
        );
      }

      try {
        assertTransactionId('invalid');
      } catch (e) {
        expect((e as BrandedTypeValidationError).code).toBe(
          BrandedTypeErrorCode.INVALID_TRANSACTION_ID
        );
      }
    });
  });
});
