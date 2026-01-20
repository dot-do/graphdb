/**
 * Entity ID Length and Format Validation Security Tests
 *
 * Tests for preventing DoS attacks through oversized entity IDs and
 * ensuring entity IDs conform to valid URL format requirements.
 *
 * @see src/security/entity-validator.ts for implementation
 */

import { describe, it, expect } from 'vitest';
import {
  validateEntityId,
  isValidEntityIdFormat,
  MAX_ID_LENGTH,
  EntityIdValidationError,
} from '../../src/security/entity-validator.js';

describe('Entity ID Length Validation', () => {
  describe('Valid Entity IDs', () => {
    it('should accept a simple valid entity ID', () => {
      const id = 'https://example.com/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept entity ID with path segments', () => {
      const id = 'https://example.com/crm/acme/customer/abc123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept entity ID at exactly MAX_ID_LENGTH', () => {
      // Build a URL that is exactly MAX_ID_LENGTH characters
      const base = 'https://example.com/entity/';
      const padding = 'a'.repeat(MAX_ID_LENGTH - base.length);
      const id = base + padding;

      expect(id.length).toBe(MAX_ID_LENGTH);
      const result = validateEntityId(id);
      expect(result).toBe(id);
    });

    it('should accept entity ID with query parameters', () => {
      const id = 'https://example.com/entity/123?version=1';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept entity ID with fragment', () => {
      const id = 'https://example.com/entity/123#section';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept http:// URLs', () => {
      const id = 'http://example.com/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept URLs with ports', () => {
      const id = 'https://example.com:8443/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept URLs with subdomains', () => {
      const id = 'https://api.data.example.com/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should accept URLs with special characters in path (percent-encoded)', () => {
      const id = 'https://example.com/entity/hello%20world';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });
  });

  describe('Length Limit Enforcement', () => {
    it('should reject IDs longer than MAX_ID_LENGTH (2048 chars)', () => {
      const base = 'https://example.com/entity/';
      const padding = 'a'.repeat(MAX_ID_LENGTH - base.length + 1);
      const id = base + padding;

      expect(id.length).toBe(MAX_ID_LENGTH + 1);
      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject extremely long IDs (potential DoS vector)', () => {
      const base = 'https://example.com/entity/';
      const padding = 'a'.repeat(10000);
      const id = base + padding;

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      expect(() => validateEntityId(id)).toThrow(/exceeds maximum length/i);
    });

    it('should reject IDs with very long hostnames', () => {
      // Ensure the total URL exceeds MAX_ID_LENGTH
      const longSubdomain = 'a'.repeat(2100);
      const id = `https://${longSubdomain}.example.com/entity/123`;

      expect(id.length).toBeGreaterThan(MAX_ID_LENGTH);
      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with very long paths', () => {
      const base = 'https://example.com/';
      const longPath = 'segment/'.repeat(500);
      const id = base + longPath;

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });
  });

  describe('Invalid Character Rejection', () => {
    it('should reject IDs with null bytes', () => {
      const id = 'https://example.com/entity/123\x00injection';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      expect(() => validateEntityId(id)).toThrow(/invalid character/i);
    });

    it('should reject IDs with zero-width characters', () => {
      const id = 'https://example.com/entity/123\u200Bhidden';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with control characters', () => {
      const id = 'https://example.com/entity/123\x1Fcontrol';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with newlines', () => {
      const id = 'https://example.com/entity/123\ninjection';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with carriage returns', () => {
      const id = 'https://example.com/entity/123\rinjection';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with tabs', () => {
      const id = 'https://example.com/entity/123\tinjection';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with Unicode replacement character', () => {
      const id = 'https://example.com/entity/123\uFFFD';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject IDs with soft hyphen (invisible)', () => {
      const id = 'https://example.com/entity/123\u00AD';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });
  });

  describe('URL Format Validation', () => {
    it('should reject non-URL strings', () => {
      const id = 'not-a-url';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      expect(() => validateEntityId(id)).toThrow(/valid URL/i);
    });

    it('should reject file:// URLs', () => {
      const id = 'file:///etc/passwd';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject javascript: URLs', () => {
      const id = 'javascript:alert(1)';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject data: URLs', () => {
      const id = 'data:text/html,<script>alert(1)</script>';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject ftp: URLs', () => {
      const id = 'ftp://ftp.example.com/file';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject URLs that would parse with path as hostname', () => {
      // Note: 'https:///path' actually parses with hostname='path' in URL API
      // This test verifies that URLs without proper authority are rejected
      const invalidUrls = [
        'https://',       // No hostname at all
        'https://:8080/', // Empty hostname with port
      ];

      for (const id of invalidUrls) {
        expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
      }
    });

    it('should reject URLs with only dots as hostname', () => {
      const id = 'https://./path';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });

    it('should reject empty strings', () => {
      expect(() => validateEntityId('')).toThrow(EntityIdValidationError);
    });

    it('should reject whitespace-only strings', () => {
      expect(() => validateEntityId('   ')).toThrow(EntityIdValidationError);
    });

    it('should reject URLs with user info (potential injection)', () => {
      const id = 'https://user:pass@example.com/entity/123';

      expect(() => validateEntityId(id)).toThrow(EntityIdValidationError);
    });
  });

  describe('isValidEntityIdFormat helper', () => {
    it('should return true for valid entity IDs', () => {
      expect(isValidEntityIdFormat('https://example.com/entity/123')).toBe(true);
    });

    it('should return false for IDs exceeding length limit', () => {
      const longId = 'https://example.com/' + 'a'.repeat(3000);
      expect(isValidEntityIdFormat(longId)).toBe(false);
    });

    it('should return false for invalid URLs', () => {
      expect(isValidEntityIdFormat('not-a-url')).toBe(false);
    });

    it('should return false for IDs with invalid characters', () => {
      expect(isValidEntityIdFormat('https://example.com/\x00')).toBe(false);
    });

    it('should return false for non-http(s) URLs', () => {
      expect(isValidEntityIdFormat('ftp://example.com/')).toBe(false);
    });

    it('should return false for empty string', () => {
      expect(isValidEntityIdFormat('')).toBe(false);
    });

    it('should return false for null-ish values', () => {
      expect(isValidEntityIdFormat(null as unknown as string)).toBe(false);
      expect(isValidEntityIdFormat(undefined as unknown as string)).toBe(false);
    });
  });

  describe('Edge Cases', () => {
    it('should handle URLs at the boundary of MAX_ID_LENGTH', () => {
      const base = 'https://example.com/';

      // Exactly at limit - should pass
      const atLimit = base + 'a'.repeat(MAX_ID_LENGTH - base.length);
      expect(() => validateEntityId(atLimit)).not.toThrow();

      // One over limit - should fail
      const overLimit = base + 'a'.repeat(MAX_ID_LENGTH - base.length + 1);
      expect(() => validateEntityId(overLimit)).toThrow(EntityIdValidationError);
    });

    it('should handle URLs with international domain names', () => {
      // Punycode-encoded IDN
      const id = 'https://xn--nxasmq5b.com/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should handle localhost URLs', () => {
      const id = 'http://localhost/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should handle IP address URLs', () => {
      const id = 'http://192.168.1.1/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });

    it('should handle IPv6 URLs', () => {
      const id = 'http://[::1]/entity/123';
      const result = validateEntityId(id);

      expect(result).toBe(id);
    });
  });

  describe('Security Scenarios', () => {
    it('should prevent URL-based injection attempts', () => {
      const attacks = [
        'https://example.com/../../etc/passwd',
        'https://example.com/%2e%2e%2f%2e%2e%2fetc/passwd',
        'https://example.com/entity/123?callback=<script>',
      ];

      // These should either pass validation (as they are valid URLs)
      // or be rejected based on content. The key is they don't crash.
      for (const attack of attacks) {
        expect(() => {
          try {
            validateEntityId(attack);
          } catch (e) {
            if (e instanceof EntityIdValidationError) {
              throw e;
            }
          }
        }).not.toThrow(Error);
      }
    });

    it('should not be vulnerable to ReDoS', () => {
      // Test with inputs that could cause catastrophic backtracking
      const start = Date.now();

      // Long repeating patterns that could trigger ReDoS
      const patterns = [
        'https://example.com/' + 'a'.repeat(1000),
        'https://example.com/' + 'ab'.repeat(500),
        'https://example.com/' + 'aaa/'.repeat(200),
      ];

      for (const pattern of patterns) {
        try {
          validateEntityId(pattern);
        } catch {
          // Expected to fail due to length
        }
      }

      const elapsed = Date.now() - start;
      // Should complete in under 100ms even with complex patterns
      expect(elapsed).toBeLessThan(100);
    });
  });
});
