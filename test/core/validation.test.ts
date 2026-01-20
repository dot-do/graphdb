/**
 * Direct Tests for Core Validation Module
 *
 * Tests for the foundational validation logic in src/core/validation.ts
 * This module is tested directly here (not through security re-exports).
 *
 * @see src/core/validation.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  MAX_ID_LENGTH,
  EntityIdValidationError,
  EntityIdErrorCode,
  validateEntityId,
  isValidEntityIdFormat,
} from '../../src/core/validation.js';

describe('Core Validation Module', () => {
  describe('MAX_ID_LENGTH constant', () => {
    it('should be 2048 characters', () => {
      expect(MAX_ID_LENGTH).toBe(2048);
    });

    it('should be a positive integer', () => {
      expect(Number.isInteger(MAX_ID_LENGTH)).toBe(true);
      expect(MAX_ID_LENGTH).toBeGreaterThan(0);
    });
  });

  describe('EntityIdErrorCode enum', () => {
    it('should have EMPTY code', () => {
      expect(EntityIdErrorCode.EMPTY).toBe('EMPTY');
    });

    it('should have TOO_LONG code', () => {
      expect(EntityIdErrorCode.TOO_LONG).toBe('TOO_LONG');
    });

    it('should have INVALID_CHARACTERS code', () => {
      expect(EntityIdErrorCode.INVALID_CHARACTERS).toBe('INVALID_CHARACTERS');
    });

    it('should have INVALID_URL code', () => {
      expect(EntityIdErrorCode.INVALID_URL).toBe('INVALID_URL');
    });

    it('should have INVALID_PROTOCOL code', () => {
      expect(EntityIdErrorCode.INVALID_PROTOCOL).toBe('INVALID_PROTOCOL');
    });

    it('should have INVALID_HOSTNAME code', () => {
      expect(EntityIdErrorCode.INVALID_HOSTNAME).toBe('INVALID_HOSTNAME');
    });

    it('should have HAS_USER_INFO code', () => {
      expect(EntityIdErrorCode.HAS_USER_INFO).toBe('HAS_USER_INFO');
    });

    it('should have exactly 7 error codes', () => {
      const codes = Object.values(EntityIdErrorCode);
      expect(codes).toHaveLength(7);
    });
  });

  describe('EntityIdValidationError class', () => {
    it('should be an instance of Error', () => {
      const error = new EntityIdValidationError('test message', EntityIdErrorCode.EMPTY);
      expect(error).toBeInstanceOf(Error);
    });

    it('should have correct name property', () => {
      const error = new EntityIdValidationError('test message', EntityIdErrorCode.EMPTY);
      expect(error.name).toBe('EntityIdValidationError');
    });

    it('should store the error message', () => {
      const error = new EntityIdValidationError('custom error message', EntityIdErrorCode.EMPTY);
      expect(error.message).toBe('custom error message');
    });

    it('should store the error code', () => {
      const error = new EntityIdValidationError('test', EntityIdErrorCode.TOO_LONG);
      expect(error.code).toBe(EntityIdErrorCode.TOO_LONG);
    });

    it('should work with all error codes', () => {
      const codes = Object.values(EntityIdErrorCode);
      for (const code of codes) {
        const error = new EntityIdValidationError(`Error for ${code}`, code);
        expect(error.code).toBe(code);
        expect(error.message).toBe(`Error for ${code}`);
      }
    });
  });

  describe('validateEntityId', () => {
    describe('valid inputs', () => {
      it('should accept a simple valid HTTPS URL', () => {
        const id = 'https://example.com/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept a simple valid HTTP URL', () => {
        const id = 'http://example.com/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with multiple path segments', () => {
        const id = 'https://example.com/crm/acme/customers/abc123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with query parameters', () => {
        const id = 'https://example.com/entity/123?version=1&format=json';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with fragment', () => {
        const id = 'https://example.com/entity/123#section';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with port', () => {
        const id = 'https://example.com:8443/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with subdomains', () => {
        const id = 'https://api.data.example.com/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL at exactly MAX_ID_LENGTH', () => {
        const base = 'https://example.com/';
        const padding = 'a'.repeat(MAX_ID_LENGTH - base.length);
        const id = base + padding;
        expect(id.length).toBe(MAX_ID_LENGTH);
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept localhost URLs', () => {
        const id = 'http://localhost/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept IP address URLs', () => {
        const id = 'http://192.168.1.1/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept IPv6 URLs', () => {
        const id = 'http://[::1]/entity/123';
        expect(validateEntityId(id)).toBe(id);
      });

      it('should accept URL with percent-encoded characters', () => {
        const id = 'https://example.com/entity/hello%20world';
        expect(validateEntityId(id)).toBe(id);
      });
    });

    describe('EMPTY error code', () => {
      it('should reject null', () => {
        expect(() => validateEntityId(null as unknown as string)).toThrow(
          EntityIdValidationError
        );
        try {
          validateEntityId(null as unknown as string);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.EMPTY);
        }
      });

      it('should reject undefined', () => {
        expect(() => validateEntityId(undefined as unknown as string)).toThrow(
          EntityIdValidationError
        );
        try {
          validateEntityId(undefined as unknown as string);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.EMPTY);
        }
      });

      it('should reject non-string types', () => {
        const nonStrings = [123, {}, [], true, Symbol('test')];
        for (const value of nonStrings) {
          expect(() => validateEntityId(value as unknown as string)).toThrow(
            EntityIdValidationError
          );
        }
      });

      it('should reject empty string', () => {
        expect(() => validateEntityId('')).toThrow(EntityIdValidationError);
        try {
          validateEntityId('');
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.EMPTY);
        }
      });

      it('should reject whitespace-only string', () => {
        expect(() => validateEntityId('   ')).toThrow(EntityIdValidationError);
        try {
          validateEntityId('   ');
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.EMPTY);
        }
      });

      it('should reject string with only tabs and newlines', () => {
        // Note: tabs and newlines are control characters, so INVALID_CHARACTERS is thrown first
        // But whitespace-only detection happens before that for pure spaces
        expect(() => validateEntityId('\t\n\r')).toThrow(EntityIdValidationError);
      });
    });

    describe('TOO_LONG error code', () => {
      it('should reject ID one character over MAX_ID_LENGTH', () => {
        const base = 'https://example.com/';
        const padding = 'a'.repeat(MAX_ID_LENGTH - base.length + 1);
        const id = base + padding;
        expect(id.length).toBe(MAX_ID_LENGTH + 1);

        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.TOO_LONG);
        }
      });

      it('should reject extremely long IDs', () => {
        const id = 'https://example.com/' + 'a'.repeat(10000);
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.TOO_LONG);
        }
      });

      it('should include actual length in error message', () => {
        const id = 'https://example.com/' + 'a'.repeat(3000);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).message).toContain('3020');
          expect((e as EntityIdValidationError).message).toContain(`${MAX_ID_LENGTH}`);
        }
      });
    });

    describe('INVALID_CHARACTERS error code', () => {
      it('should reject ID with null byte', () => {
        const id = 'https://example.com/entity/123\x00injection';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_CHARACTERS
          );
        }
      });

      it('should reject ID with control characters (0x01-0x1F)', () => {
        for (let i = 1; i <= 0x1f; i++) {
          const char = String.fromCharCode(i);
          const id = `https://example.com/entity/${char}test`;
          expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        }
      });

      it('should reject ID with DEL character (0x7F)', () => {
        const id = 'https://example.com/entity/\x7Ftest';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_CHARACTERS
          );
        }
      });

      it('should reject ID with zero-width space (U+200B)', () => {
        const id = 'https://example.com/entity/\u200Bhidden';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_CHARACTERS
          );
        }
      });

      it('should reject ID with zero-width non-joiner (U+200C)', () => {
        const id = 'https://example.com/entity/\u200Chidden';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });

      it('should reject ID with zero-width joiner (U+200D)', () => {
        const id = 'https://example.com/entity/\u200Dhidden';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });

      it('should reject ID with byte order mark (U+FEFF)', () => {
        const id = 'https://example.com/entity/\uFEFFtest';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });

      it('should reject ID with soft hyphen (U+00AD)', () => {
        const id = 'https://example.com/entity/\u00ADtest';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });

      it('should reject ID with Unicode replacement character (U+FFFD)', () => {
        const id = 'https://example.com/entity/\uFFFDtest';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });
    });

    describe('INVALID_URL error code', () => {
      it('should reject plain text (not a URL)', () => {
        const id = 'not-a-url';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.INVALID_URL);
        }
      });

      it('should reject path without protocol', () => {
        const id = 'example.com/entity/123';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.INVALID_URL);
        }
      });

      it('should reject malformed URLs', () => {
        // Note: Only URLs that actually fail URL parsing are tested here
        // Some seemingly malformed URLs like 'https:example.com' and 'https:///path'
        // actually parse successfully in the URL API
        const malformed = [
          'https://',       // No hostname at all - fails URL parsing
          'https://:8080/', // Empty hostname with port - fails URL parsing
          '://example.com', // No protocol - fails URL parsing
        ];
        for (const id of malformed) {
          expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        }
      });

      it('should accept URLs with unusual but valid syntax', () => {
        // These are unusual but parse correctly in URL API
        // 'https:example.com' => hostname: 'example.com'
        // 'https:///path' => hostname: 'path'
        expect(validateEntityId('https:example.com')).toBe('https:example.com');
        expect(validateEntityId('https:///path')).toBe('https:///path');
      });

      it('should truncate long invalid URLs in error message', () => {
        const id = 'a'.repeat(200);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).message).toContain('truncated');
          expect((e as EntityIdValidationError).message.length).toBeLessThan(250);
        }
      });
    });

    describe('INVALID_PROTOCOL error code', () => {
      it('should reject file:// URLs', () => {
        const id = 'file:///etc/passwd';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_PROTOCOL
          );
        }
      });

      it('should reject ftp:// URLs', () => {
        const id = 'ftp://ftp.example.com/file';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_PROTOCOL
          );
        }
      });

      it('should reject javascript: URLs', () => {
        const id = 'javascript:alert(1)';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_PROTOCOL
          );
        }
      });

      it('should reject data: URLs', () => {
        const id = 'data:text/html,<script>alert(1)</script>';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_PROTOCOL
          );
        }
      });

      it('should reject mailto: URLs', () => {
        const id = 'mailto:test@example.com';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_PROTOCOL
          );
        }
      });

      it('should include the invalid protocol in error message', () => {
        const id = 'ftp://example.com/file';
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).message).toContain('ftp:');
        }
      });
    });

    describe('INVALID_HOSTNAME error code', () => {
      it('should handle URLs with unusual hostname parsing', () => {
        // Note: 'https:///path' actually parses with hostname='path' in URL API
        // So it passes validation. The INVALID_HOSTNAME check is for edge cases
        // where URL parsing succeeds but hostname is empty or just dots

        // URL API behaviors can vary, so we test the validation logic
        // for hostnames that are explicitly invalid (dots only)
        expect(validateEntityId('https:///path')).toBe('https:///path');
      });

      it('should reject URL with only dots as hostname', () => {
        const id = 'https://./path';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_HOSTNAME
          );
        }
      });

      it('should reject URL with double dots as hostname', () => {
        const id = 'https://../path';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      });
    });

    describe('HAS_USER_INFO error code', () => {
      it('should reject URL with username', () => {
        const id = 'https://user@example.com/entity/123';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.HAS_USER_INFO
          );
        }
      });

      it('should reject URL with username and password', () => {
        const id = 'https://user:pass@example.com/entity/123';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.HAS_USER_INFO
          );
        }
      });

      it('should reject URL with empty username but present password', () => {
        const id = 'https://:password@example.com/entity/123';
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.HAS_USER_INFO
          );
        }
      });
    });

    describe('validation order', () => {
      it('should check length before parsing URL (DoS prevention)', () => {
        // Very long invalid URL should fail on length, not URL parsing
        const id = 'not-a-url-' + 'a'.repeat(3000);
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(EntityIdErrorCode.TOO_LONG);
        }
      });

      it('should check for invalid characters before URL parsing', () => {
        // URL with null byte should fail on characters, not URL parsing
        const id = 'https://example.com/\x00';
        try {
          validateEntityId(id);
        } catch (e) {
          expect((e as EntityIdValidationError).code).toBe(
            EntityIdErrorCode.INVALID_CHARACTERS
          );
        }
      });
    });
  });

  describe('isValidEntityIdFormat', () => {
    describe('valid inputs return true', () => {
      it('should return true for valid HTTPS URL', () => {
        expect(isValidEntityIdFormat('https://example.com/entity/123')).toBe(true);
      });

      it('should return true for valid HTTP URL', () => {
        expect(isValidEntityIdFormat('http://example.com/entity/123')).toBe(true);
      });

      it('should return true for URL at MAX_ID_LENGTH', () => {
        const base = 'https://example.com/';
        const id = base + 'a'.repeat(MAX_ID_LENGTH - base.length);
        expect(isValidEntityIdFormat(id)).toBe(true);
      });

      it('should return true for URL with query and fragment', () => {
        expect(
          isValidEntityIdFormat('https://example.com/entity/123?v=1#section')
        ).toBe(true);
      });
    });

    describe('invalid inputs return false', () => {
      it('should return false for null', () => {
        expect(isValidEntityIdFormat(null as unknown as string)).toBe(false);
      });

      it('should return false for undefined', () => {
        expect(isValidEntityIdFormat(undefined as unknown as string)).toBe(false);
      });

      it('should return false for non-string types', () => {
        expect(isValidEntityIdFormat(123 as unknown as string)).toBe(false);
        expect(isValidEntityIdFormat({} as unknown as string)).toBe(false);
        expect(isValidEntityIdFormat([] as unknown as string)).toBe(false);
      });

      it('should return false for empty string', () => {
        expect(isValidEntityIdFormat('')).toBe(false);
      });

      it('should return false for whitespace-only string', () => {
        expect(isValidEntityIdFormat('   ')).toBe(false);
      });

      it('should return false for ID exceeding MAX_ID_LENGTH', () => {
        const id = 'https://example.com/' + 'a'.repeat(3000);
        expect(isValidEntityIdFormat(id)).toBe(false);
      });

      it('should return false for non-URL string', () => {
        expect(isValidEntityIdFormat('not-a-url')).toBe(false);
      });

      it('should return false for ID with invalid characters', () => {
        expect(isValidEntityIdFormat('https://example.com/\x00')).toBe(false);
        expect(isValidEntityIdFormat('https://example.com/\u200B')).toBe(false);
      });

      it('should return false for non-http(s) protocol', () => {
        expect(isValidEntityIdFormat('ftp://example.com/file')).toBe(false);
        expect(isValidEntityIdFormat('file:///etc/passwd')).toBe(false);
      });

      it('should return false for URL with user info', () => {
        expect(isValidEntityIdFormat('https://user:pass@example.com/')).toBe(false);
      });
    });

    describe('does not throw', () => {
      it('should never throw, always return boolean', () => {
        const testCases = [
          null,
          undefined,
          '',
          '   ',
          'not-a-url',
          'https://example.com/valid',
          'a'.repeat(10000),
          'https://example.com/\x00',
          'ftp://example.com/',
          'https://user:pass@example.com/',
        ];

        for (const input of testCases) {
          expect(() => isValidEntityIdFormat(input as string)).not.toThrow();
          const result = isValidEntityIdFormat(input as string);
          expect(typeof result).toBe('boolean');
        }
      });
    });
  });

  describe('Edge Cases', () => {
    it('should handle URLs with international domain names (punycode)', () => {
      const id = 'https://xn--nxasmq5b.com/entity/123';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle URLs with unicode path segments', () => {
      // Unicode in path is valid as long as no banned characters
      const id = 'https://example.com/entity/caf%C3%A9';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle root path URLs', () => {
      const id = 'https://example.com/';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle URLs without trailing slash', () => {
      const id = 'https://example.com';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle deep nested paths', () => {
      const id = 'https://example.com/a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle URLs with all standard components', () => {
      const id = 'https://example.com:8443/path/to/resource?query=value#fragment';
      expect(validateEntityId(id)).toBe(id);
    });
  });

  describe('Security Scenarios', () => {
    it('should not be vulnerable to ReDoS', () => {
      const start = Date.now();

      // Patterns that could cause catastrophic backtracking
      const patterns = [
        'https://example.com/' + 'a'.repeat(1000),
        'https://example.com/' + 'ab'.repeat(500),
        'https://example.com/' + 'aaa/'.repeat(200),
      ];

      for (const pattern of patterns) {
        try {
          validateEntityId(pattern);
        } catch {
          // Expected to fail due to length for some
        }
      }

      const elapsed = Date.now() - start;
      expect(elapsed).toBeLessThan(100);
    });

    it('should handle path traversal attempts as valid URLs', () => {
      // These are valid URLs - path traversal is a semantic issue, not validation
      const id = 'https://example.com/../../etc/passwd';
      expect(validateEntityId(id)).toBe(id);
    });

    it('should handle URL-encoded traversal attempts as valid URLs', () => {
      const id = 'https://example.com/%2e%2e%2f%2e%2e%2fetc/passwd';
      expect(validateEntityId(id)).toBe(id);
    });
  });
});
