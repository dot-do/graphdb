/**
 * Permission Model Tests
 *
 * Tests for GraphDB authorization layer:
 * - Read/write permissions per namespace
 * - Entity-level ACLs
 * - Permission checking utilities
 *
 * TDD approach: Tests written first, implementation follows.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  checkPermission,
  checkNamespaceAccess,
  hasReadPermission,
  hasWritePermission,
  hasInternalPermission,
  Permission,
  PermissionContext,
  PermissionCheckResult,
  createPermissionContext,
  EntityACL,
  checkEntityAccess,
  PermissionDeniedReason,
} from '../../src/security/permissions.js';
import type { AuthContext } from '../../src/security/auth.js';

describe('Permission Model', () => {
  describe('Basic Permission Checks', () => {
    const readOnlyContext: AuthContext = {
      callerId: 'readonly-service',
      authMethod: 'api_key',
      permissions: ['read'],
      namespaces: ['https://example.com/'],
      timestamp: Date.now(),
    };

    const readWriteContext: AuthContext = {
      callerId: 'rw-service',
      authMethod: 'api_key',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/'],
      timestamp: Date.now(),
    };

    const internalContext: AuthContext = {
      callerId: 'binding:INTERNAL_SERVICE',
      authMethod: 'worker_binding',
      permissions: ['read', 'write', 'internal'],
      namespaces: ['*'],
      timestamp: Date.now(),
    };

    it('should grant read permission to read-only context', () => {
      const result = checkPermission(readOnlyContext, 'read');

      expect(result.allowed).toBe(true);
    });

    it('should deny write permission to read-only context', () => {
      const result = checkPermission(readOnlyContext, 'write');

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.INSUFFICIENT_PERMISSIONS);
    });

    it('should grant read and write permissions to read-write context', () => {
      const readResult = checkPermission(readWriteContext, 'read');
      const writeResult = checkPermission(readWriteContext, 'write');

      expect(readResult.allowed).toBe(true);
      expect(writeResult.allowed).toBe(true);
    });

    it('should grant all permissions to internal context', () => {
      const readResult = checkPermission(internalContext, 'read');
      const writeResult = checkPermission(internalContext, 'write');
      const internalResult = checkPermission(internalContext, 'internal');

      expect(readResult.allowed).toBe(true);
      expect(writeResult.allowed).toBe(true);
      expect(internalResult.allowed).toBe(true);
    });

    it('should deny internal permission to non-internal context', () => {
      const result = checkPermission(readWriteContext, 'internal');

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.INSUFFICIENT_PERMISSIONS);
    });
  });

  describe('Namespace Access Checks', () => {
    const singleNamespaceContext: AuthContext = {
      callerId: 'ns-service',
      authMethod: 'api_key',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/crm/'],
      timestamp: Date.now(),
    };

    const multiNamespaceContext: AuthContext = {
      callerId: 'multi-ns-service',
      authMethod: 'api_key',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/crm/', 'https://example.com/hr/'],
      timestamp: Date.now(),
    };

    const wildcardContext: AuthContext = {
      callerId: 'admin-service',
      authMethod: 'api_key',
      permissions: ['read', 'write'],
      namespaces: ['*'],
      timestamp: Date.now(),
    };

    it('should allow access to authorized namespace', () => {
      const result = checkNamespaceAccess(
        singleNamespaceContext,
        'https://example.com/crm/'
      );

      expect(result.allowed).toBe(true);
    });

    it('should deny access to unauthorized namespace', () => {
      const result = checkNamespaceAccess(
        singleNamespaceContext,
        'https://example.com/hr/'
      );

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.NAMESPACE_NOT_ALLOWED);
    });

    it('should allow access to any of multiple authorized namespaces', () => {
      const crmResult = checkNamespaceAccess(
        multiNamespaceContext,
        'https://example.com/crm/'
      );
      const hrResult = checkNamespaceAccess(
        multiNamespaceContext,
        'https://example.com/hr/'
      );

      expect(crmResult.allowed).toBe(true);
      expect(hrResult.allowed).toBe(true);
    });

    it('should allow access to any namespace with wildcard', () => {
      const result1 = checkNamespaceAccess(wildcardContext, 'https://example.com/anything/');
      const result2 = checkNamespaceAccess(wildcardContext, 'https://other-domain.com/');

      expect(result1.allowed).toBe(true);
      expect(result2.allowed).toBe(true);
    });

    it('should match namespace prefix correctly', () => {
      const prefixContext: AuthContext = {
        callerId: 'prefix-service',
        authMethod: 'api_key',
        permissions: ['read'],
        namespaces: ['https://example.com/'],
        timestamp: Date.now(),
      };

      // Should match entities under the namespace prefix
      const result1 = checkNamespaceAccess(prefixContext, 'https://example.com/users/123');
      const result2 = checkNamespaceAccess(prefixContext, 'https://example.com/crm/');

      expect(result1.allowed).toBe(true);
      expect(result2.allowed).toBe(true);

      // Should not match different domain
      const result3 = checkNamespaceAccess(prefixContext, 'https://other.com/');
      expect(result3.allowed).toBe(false);
    });
  });

  describe('Combined Permission and Namespace Checks', () => {
    const limitedContext: AuthContext = {
      callerId: 'limited-service',
      authMethod: 'api_key',
      permissions: ['read'],
      namespaces: ['https://example.com/public/'],
      timestamp: Date.now(),
    };

    it('should require both permission and namespace access', () => {
      const permContext = createPermissionContext(limitedContext);

      // Has read permission and namespace access - should succeed
      const result1 = permContext.check('read', 'https://example.com/public/data');
      expect(result1.allowed).toBe(true);

      // Has read permission but wrong namespace - should fail
      const result2 = permContext.check('read', 'https://example.com/private/data');
      expect(result2.allowed).toBe(false);

      // Has namespace access but wrong permission - should fail
      const result3 = permContext.check('write', 'https://example.com/public/data');
      expect(result3.allowed).toBe(false);
    });
  });

  describe('Entity-Level ACLs (Optional)', () => {
    const userContext: AuthContext = {
      callerId: 'user-123',
      authMethod: 'jwt',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/'],
      timestamp: Date.now(),
    };

    const otherUserContext: AuthContext = {
      callerId: 'user-456',
      authMethod: 'jwt',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/'],
      timestamp: Date.now(),
    };

    const entityAcl: EntityACL = {
      entityId: 'https://example.com/documents/doc-1',
      owner: 'user-123',
      readers: ['user-456', 'user-789'],
      writers: [],
      public: false,
    };

    const publicEntityAcl: EntityACL = {
      entityId: 'https://example.com/documents/public-doc',
      owner: 'user-123',
      readers: [],
      writers: [],
      public: true,
    };

    it('should allow owner full access to entity', () => {
      const readResult = checkEntityAccess(userContext, entityAcl, 'read');
      const writeResult = checkEntityAccess(userContext, entityAcl, 'write');

      expect(readResult.allowed).toBe(true);
      expect(writeResult.allowed).toBe(true);
    });

    it('should allow readers read-only access', () => {
      const readResult = checkEntityAccess(otherUserContext, entityAcl, 'read');
      const writeResult = checkEntityAccess(otherUserContext, entityAcl, 'write');

      expect(readResult.allowed).toBe(true);
      expect(writeResult.allowed).toBe(false);
      expect(writeResult.reason).toBe(PermissionDeniedReason.ENTITY_ACL_DENIED);
    });

    it('should deny access to users not in ACL', () => {
      const unauthorizedContext: AuthContext = {
        callerId: 'user-999',
        authMethod: 'jwt',
        permissions: ['read', 'write'],
        namespaces: ['https://example.com/'],
        timestamp: Date.now(),
      };

      const result = checkEntityAccess(unauthorizedContext, entityAcl, 'read');

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.ENTITY_ACL_DENIED);
    });

    it('should allow public read access to public entities', () => {
      const anyContext: AuthContext = {
        callerId: 'anonymous',
        authMethod: 'api_key',
        permissions: ['read'],
        namespaces: ['https://example.com/'],
        timestamp: Date.now(),
      };

      const result = checkEntityAccess(anyContext, publicEntityAcl, 'read');

      expect(result.allowed).toBe(true);
    });

    it('should still require owner/writer for writes on public entities', () => {
      const anyContext: AuthContext = {
        callerId: 'anonymous',
        authMethod: 'api_key',
        permissions: ['read', 'write'],
        namespaces: ['https://example.com/'],
        timestamp: Date.now(),
      };

      const result = checkEntityAccess(anyContext, publicEntityAcl, 'write');

      expect(result.allowed).toBe(false);
    });
  });

  describe('Helper Functions', () => {
    const mixedContext: AuthContext = {
      callerId: 'mixed-service',
      authMethod: 'api_key',
      permissions: ['read', 'write'],
      namespaces: ['https://example.com/'],
      timestamp: Date.now(),
    };

    it('hasReadPermission should check for read permission', () => {
      expect(hasReadPermission(mixedContext)).toBe(true);
    });

    it('hasWritePermission should check for write permission', () => {
      expect(hasWritePermission(mixedContext)).toBe(true);
    });

    it('hasInternalPermission should check for internal permission', () => {
      expect(hasInternalPermission(mixedContext)).toBe(false);

      const internalContext: AuthContext = {
        ...mixedContext,
        permissions: ['read', 'write', 'internal'],
      };
      expect(hasInternalPermission(internalContext)).toBe(true);
    });
  });

  describe('Permission Denied Response', () => {
    it('should include helpful information in denied response', () => {
      const context: AuthContext = {
        callerId: 'test-service',
        authMethod: 'api_key',
        permissions: ['read'],
        namespaces: ['https://example.com/crm/'],
        timestamp: Date.now(),
      };

      const result = checkPermission(context, 'write');

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.INSUFFICIENT_PERMISSIONS);
      expect(result.requiredPermission).toBe('write');
      expect(result.callerPermissions).toEqual(['read']);
    });

    it('should include namespace information in denied response', () => {
      const context: AuthContext = {
        callerId: 'test-service',
        authMethod: 'api_key',
        permissions: ['read'],
        namespaces: ['https://example.com/crm/'],
        timestamp: Date.now(),
      };

      const result = checkNamespaceAccess(context, 'https://other.com/');

      expect(result.allowed).toBe(false);
      expect(result.reason).toBe(PermissionDeniedReason.NAMESPACE_NOT_ALLOWED);
      expect(result.requestedNamespace).toBe('https://other.com/');
      expect(result.allowedNamespaces).toEqual(['https://example.com/crm/']);
    });
  });
});
