/**
 * Authentication & Authorization Tests
 *
 * Tests for GraphDB security layer:
 * - API key authentication (service-to-service)
 * - JWT authentication (user requests)
 * - Worker binding authentication (internal calls)
 * - Permission validation
 *
 * TDD approach: Tests written first, implementation follows.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  validateApiKey,
  validateJwt,
  validateWorkerBinding,
  createAuthContext,
  AuthContext,
  AuthErrorCode,
  AuthError,
  ApiKeyConfig,
  JwtConfig,
  WorkerBindingConfig,
} from '../../src/security/auth.js';

describe('Authentication & Authorization', () => {
  describe('API Key Authentication', () => {
    const validApiKey = 'gdb_sk_test_1234567890abcdef';
    const invalidApiKey = 'invalid_key';

    const config: ApiKeyConfig = {
      keys: new Map([
        [validApiKey, { callerId: 'service-1', permissions: ['read', 'write'], namespaces: ['*'] }],
        ['gdb_sk_readonly_key', { callerId: 'readonly-service', permissions: ['read'], namespaces: ['https://example.com/'] }],
      ]),
    };

    it('should validate a correct API key', async () => {
      const result = await validateApiKey(validApiKey, config);

      expect(result.success).toBe(true);
      expect(result.context?.callerId).toBe('service-1');
      expect(result.context?.authMethod).toBe('api_key');
      expect(result.context?.permissions).toContain('read');
      expect(result.context?.permissions).toContain('write');
    });

    it('should reject an invalid API key', async () => {
      const result = await validateApiKey(invalidApiKey, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.INVALID_API_KEY);
    });

    it('should reject empty API key', async () => {
      const result = await validateApiKey('', config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.MISSING_CREDENTIALS);
    });

    it('should reject null/undefined API key', async () => {
      const result1 = await validateApiKey(null as unknown as string, config);
      const result2 = await validateApiKey(undefined as unknown as string, config);

      expect(result1.success).toBe(false);
      expect(result2.success).toBe(false);
      expect(result1.error?.code).toBe(AuthErrorCode.MISSING_CREDENTIALS);
    });

    it('should return correct namespace permissions', async () => {
      const result = await validateApiKey('gdb_sk_readonly_key', config);

      expect(result.success).toBe(true);
      expect(result.context?.namespaces).toEqual(['https://example.com/']);
      expect(result.context?.permissions).toEqual(['read']);
    });

    it('should handle wildcard namespace', async () => {
      const result = await validateApiKey(validApiKey, config);

      expect(result.success).toBe(true);
      expect(result.context?.namespaces).toContain('*');
    });
  });

  describe('JWT Authentication', () => {
    // Mock JWT for testing (in production, use proper HMAC/RS256)
    const validPayload = {
      sub: 'user-123',
      iss: 'https://auth.example.com',
      aud: 'graphdb',
      exp: Math.floor(Date.now() / 1000) + 3600, // 1 hour from now
      iat: Math.floor(Date.now() / 1000),
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/users/'],
    };

    // Base64url encode helper for creating test tokens
    function base64UrlEncode(obj: unknown): string {
      return btoa(JSON.stringify(obj))
        .replace(/\+/g, '-')
        .replace(/\//g, '_')
        .replace(/=/g, '');
    }

    // Create a test JWT (unsigned, for testing purposes)
    function createTestToken(payload: typeof validPayload): string {
      const header = { alg: 'HS256', typ: 'JWT' };
      return `${base64UrlEncode(header)}.${base64UrlEncode(payload)}.test_signature`;
    }

    const config: JwtConfig = {
      issuer: 'https://auth.example.com',
      audience: 'graphdb',
      secret: 'test-secret-key-for-testing-only',
      algorithms: ['HS256'],
      // For testing, skip signature verification
      skipSignatureVerification: true,
    };

    beforeEach(() => {
      vi.useFakeTimers();
      vi.setSystemTime(new Date());
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it('should validate a correct JWT', async () => {
      const token = createTestToken(validPayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(true);
      expect(result.context?.callerId).toBe('user-123');
      expect(result.context?.authMethod).toBe('jwt');
    });

    it('should reject expired JWT', async () => {
      const expiredPayload = {
        ...validPayload,
        exp: Math.floor(Date.now() / 1000) - 3600, // 1 hour ago
      };
      const token = createTestToken(expiredPayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.TOKEN_EXPIRED);
    });

    it('should reject JWT with wrong issuer', async () => {
      const wrongIssuerPayload = {
        ...validPayload,
        iss: 'https://wrong-issuer.com',
      };
      const token = createTestToken(wrongIssuerPayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.INVALID_ISSUER);
    });

    it('should reject JWT with wrong audience', async () => {
      const wrongAudiencePayload = {
        ...validPayload,
        aud: 'wrong-audience',
      };
      const token = createTestToken(wrongAudiencePayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.INVALID_AUDIENCE);
    });

    it('should reject malformed JWT', async () => {
      const result = await validateJwt('not.a.valid.jwt', config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.MALFORMED_TOKEN);
    });

    it('should reject empty JWT', async () => {
      const result = await validateJwt('', config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.MISSING_CREDENTIALS);
    });

    it('should extract permissions from JWT claims', async () => {
      const token = createTestToken(validPayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(true);
      expect(result.context?.permissions).toEqual(['read', 'write']);
      expect(result.context?.namespaces).toEqual(['https://example.com/users/']);
    });

    it('should reject JWT not yet valid (nbf claim)', async () => {
      const notYetValidPayload = {
        ...validPayload,
        nbf: Math.floor(Date.now() / 1000) + 3600, // 1 hour from now
      };
      const token = createTestToken(notYetValidPayload);
      const result = await validateJwt(token, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.TOKEN_NOT_YET_VALID);
    });
  });

  describe('Worker Binding Authentication', () => {
    const config: WorkerBindingConfig = {
      trustedBindings: new Set(['INTERNAL_SERVICE', 'CDC_WORKER']),
      sharedSecret: 'worker-shared-secret-for-internal-calls',
    };

    it('should validate trusted worker binding', async () => {
      const result = await validateWorkerBinding(
        'INTERNAL_SERVICE',
        'worker-shared-secret-for-internal-calls',
        config
      );

      expect(result.success).toBe(true);
      expect(result.context?.callerId).toBe('binding:INTERNAL_SERVICE');
      expect(result.context?.authMethod).toBe('worker_binding');
      expect(result.context?.permissions).toContain('internal');
    });

    it('should reject untrusted worker binding', async () => {
      const result = await validateWorkerBinding(
        'UNTRUSTED_BINDING',
        'worker-shared-secret-for-internal-calls',
        config
      );

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.UNTRUSTED_BINDING);
    });

    it('should reject worker binding with wrong secret', async () => {
      const result = await validateWorkerBinding(
        'INTERNAL_SERVICE',
        'wrong-secret',
        config
      );

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.INVALID_BINDING_SECRET);
    });

    it('should allow all namespaces for internal calls', async () => {
      const result = await validateWorkerBinding(
        'INTERNAL_SERVICE',
        'worker-shared-secret-for-internal-calls',
        config
      );

      expect(result.success).toBe(true);
      expect(result.context?.namespaces).toContain('*');
    });
  });

  describe('Auth Context Creation', () => {
    it('should create auth context from request with API key header', async () => {
      const request = new Request('https://graphdb.example.com/api', {
        headers: {
          'X-API-Key': 'gdb_sk_test_key',
        },
      });

      const config = {
        apiKey: {
          keys: new Map([
            ['gdb_sk_test_key', { callerId: 'test-service', permissions: ['read'], namespaces: ['*'] }],
          ]),
        },
      };

      const result = await createAuthContext(request, config);

      expect(result.success).toBe(true);
      expect(result.context?.authMethod).toBe('api_key');
    });

    it('should create auth context from request with Bearer token', async () => {
      // Create a test token
      const payload = {
        sub: 'user-456',
        iss: 'https://auth.example.com',
        aud: 'graphdb',
        exp: Math.floor(Date.now() / 1000) + 3600,
        iat: Math.floor(Date.now() / 1000),
        permissions: ['read'],
        namespaces: ['https://example.com/'],
      };

      const header = { alg: 'HS256', typ: 'JWT' };
      const token = `${btoa(JSON.stringify(header)).replace(/=/g, '')}.${btoa(JSON.stringify(payload)).replace(/=/g, '')}.sig`;

      const request = new Request('https://graphdb.example.com/api', {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const config = {
        jwt: {
          issuer: 'https://auth.example.com',
          audience: 'graphdb',
          secret: 'test-secret',
          algorithms: ['HS256'] as const,
          skipSignatureVerification: true,
        },
      };

      const result = await createAuthContext(request, config);

      expect(result.success).toBe(true);
      expect(result.context?.authMethod).toBe('jwt');
    });

    it('should create auth context from request with worker binding header', async () => {
      const request = new Request('https://graphdb.example.com/internal', {
        headers: {
          'X-Worker-Binding': 'CDC_WORKER',
          'X-Binding-Secret': 'internal-secret',
        },
      });

      const config = {
        workerBinding: {
          trustedBindings: new Set(['CDC_WORKER']),
          sharedSecret: 'internal-secret',
        },
      };

      const result = await createAuthContext(request, config);

      expect(result.success).toBe(true);
      expect(result.context?.authMethod).toBe('worker_binding');
    });

    it('should reject request with no credentials', async () => {
      const request = new Request('https://graphdb.example.com/api');

      const config = {
        apiKey: {
          keys: new Map(),
        },
      };

      const result = await createAuthContext(request, config);

      expect(result.success).toBe(false);
      expect(result.error?.code).toBe(AuthErrorCode.MISSING_CREDENTIALS);
    });

    it('should prioritize API key over JWT when both present', async () => {
      const request = new Request('https://graphdb.example.com/api', {
        headers: {
          'X-API-Key': 'gdb_sk_priority_key',
          Authorization: 'Bearer some.jwt.token',
        },
      });

      const config = {
        apiKey: {
          keys: new Map([
            ['gdb_sk_priority_key', { callerId: 'api-key-service', permissions: ['read'], namespaces: ['*'] }],
          ]),
        },
        jwt: {
          issuer: 'https://auth.example.com',
          audience: 'graphdb',
          secret: 'test-secret',
          algorithms: ['HS256'] as const,
          skipSignatureVerification: true,
        },
      };

      const result = await createAuthContext(request, config);

      expect(result.success).toBe(true);
      expect(result.context?.authMethod).toBe('api_key');
      expect(result.context?.callerId).toBe('api-key-service');
    });
  });

  describe('AuthError', () => {
    it('should create AuthError with correct properties', () => {
      const error = new AuthError(AuthErrorCode.INVALID_API_KEY, 'API key not found');

      expect(error.code).toBe(AuthErrorCode.INVALID_API_KEY);
      expect(error.message).toBe('API key not found');
      expect(error.name).toBe('AuthError');
    });

    it('should convert AuthError to response format', () => {
      const error = new AuthError(AuthErrorCode.TOKEN_EXPIRED, 'JWT has expired');
      const response = error.toResponse();

      expect(response.type).toBe('error');
      expect(response.code).toBe(AuthErrorCode.TOKEN_EXPIRED);
      expect(response.message).toBe('JWT has expired');
    });
  });
});
