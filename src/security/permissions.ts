/**
 * Permission Model for GraphDB
 *
 * Provides authorization layer for:
 * - Read/write permissions per namespace
 * - Entity-level ACLs (optional)
 * - Permission checking utilities
 *
 * Integration points:
 * - Shard DO: Authorize namespace access before operations
 * - Broker DO: Check permissions before forwarding queries
 * - Query executor: Validate access during query execution
 */

import type { AuthContext } from './auth.js';

// ============================================================================
// Permission Types
// ============================================================================

/**
 * Standard permission types
 */
export type Permission = 'read' | 'write' | 'internal' | string;

/**
 * Reasons why permission was denied
 */
export enum PermissionDeniedReason {
  /** Caller does not have the required permission */
  INSUFFICIENT_PERMISSIONS = 'INSUFFICIENT_PERMISSIONS',
  /** Caller does not have access to the requested namespace */
  NAMESPACE_NOT_ALLOWED = 'NAMESPACE_NOT_ALLOWED',
  /** Entity-level ACL denied access */
  ENTITY_ACL_DENIED = 'ENTITY_ACL_DENIED',
  /** No auth context provided */
  NO_AUTH_CONTEXT = 'NO_AUTH_CONTEXT',
}

/**
 * Result of a permission check
 */
export interface PermissionCheckResult {
  /** Whether the permission was granted */
  allowed: boolean;
  /** Reason for denial (if not allowed) */
  reason?: PermissionDeniedReason;
  /** The permission that was required */
  requiredPermission?: string;
  /** The caller's actual permissions (for debugging) */
  callerPermissions?: string[];
  /** The requested namespace (for namespace checks) */
  requestedNamespace?: string;
  /** The allowed namespaces (for namespace checks) */
  allowedNamespaces?: string[];
}

// ============================================================================
// Entity-Level ACL Types
// ============================================================================

/**
 * Entity-level Access Control List
 *
 * Optional fine-grained access control for individual entities.
 */
export interface EntityACL {
  /** The entity this ACL applies to */
  entityId: string;
  /** The owner of the entity (full access) */
  owner: string;
  /** List of caller IDs that can read this entity */
  readers: string[];
  /** List of caller IDs that can write this entity */
  writers: string[];
  /** Whether the entity is publicly readable */
  public: boolean;
}

// ============================================================================
// Permission Checking Functions
// ============================================================================

/**
 * Check if an auth context has a specific permission
 *
 * @param context - The authenticated context to check
 * @param permission - The required permission
 * @returns Permission check result
 */
export function checkPermission(
  context: AuthContext,
  permission: Permission
): PermissionCheckResult {
  if (!context) {
    return {
      allowed: false,
      reason: PermissionDeniedReason.NO_AUTH_CONTEXT,
      requiredPermission: permission,
    };
  }

  const hasPermission = context.permissions.includes(permission);

  if (hasPermission) {
    return { allowed: true };
  }

  return {
    allowed: false,
    reason: PermissionDeniedReason.INSUFFICIENT_PERMISSIONS,
    requiredPermission: permission,
    callerPermissions: [...context.permissions],
  };
}

/**
 * Check if an auth context has access to a specific namespace
 *
 * Namespace matching rules:
 * - Wildcard '*' matches all namespaces
 * - Exact match if namespace equals allowed namespace
 * - Prefix match if namespace starts with allowed namespace
 *
 * @param context - The authenticated context to check
 * @param namespace - The namespace to check access for
 * @returns Permission check result
 */
export function checkNamespaceAccess(
  context: AuthContext,
  namespace: string
): PermissionCheckResult {
  if (!context) {
    return {
      allowed: false,
      reason: PermissionDeniedReason.NO_AUTH_CONTEXT,
      requestedNamespace: namespace,
    };
  }

  // Check if wildcard is present
  if (context.namespaces.includes('*')) {
    return { allowed: true };
  }

  // Check for exact or prefix match
  const hasAccess = context.namespaces.some((allowedNs) => {
    // Exact match
    if (namespace === allowedNs) {
      return true;
    }
    // Prefix match (namespace is under an allowed namespace)
    if (namespace.startsWith(allowedNs)) {
      return true;
    }
    return false;
  });

  if (hasAccess) {
    return { allowed: true };
  }

  return {
    allowed: false,
    reason: PermissionDeniedReason.NAMESPACE_NOT_ALLOWED,
    requestedNamespace: namespace,
    allowedNamespaces: [...context.namespaces],
  };
}

/**
 * Check if an auth context has access to an entity based on its ACL
 *
 * ACL checking rules:
 * - Owner has full access
 * - Writers have write access (implies read)
 * - Readers have read access
 * - Public entities can be read by anyone
 *
 * @param context - The authenticated context to check
 * @param acl - The entity's access control list
 * @param permission - The required permission ('read' or 'write')
 * @returns Permission check result
 */
export function checkEntityAccess(
  context: AuthContext,
  acl: EntityACL,
  permission: 'read' | 'write'
): PermissionCheckResult {
  if (!context) {
    return {
      allowed: false,
      reason: PermissionDeniedReason.NO_AUTH_CONTEXT,
      requiredPermission: permission,
    };
  }

  const callerId = context.callerId;

  // Owner has full access
  if (callerId === acl.owner) {
    return { allowed: true };
  }

  // Check write permission
  if (permission === 'write') {
    // Only owner or writers can write
    if (acl.writers.includes(callerId)) {
      return { allowed: true };
    }
    return {
      allowed: false,
      reason: PermissionDeniedReason.ENTITY_ACL_DENIED,
      requiredPermission: permission,
    };
  }

  // Check read permission
  if (permission === 'read') {
    // Public entities can be read by anyone
    if (acl.public) {
      return { allowed: true };
    }
    // Writers can also read
    if (acl.writers.includes(callerId)) {
      return { allowed: true };
    }
    // Readers can read
    if (acl.readers.includes(callerId)) {
      return { allowed: true };
    }
    return {
      allowed: false,
      reason: PermissionDeniedReason.ENTITY_ACL_DENIED,
      requiredPermission: permission,
    };
  }

  return {
    allowed: false,
    reason: PermissionDeniedReason.ENTITY_ACL_DENIED,
    requiredPermission: permission,
  };
}

// ============================================================================
// Permission Context (Convenience Wrapper)
// ============================================================================

/**
 * Permission context wrapping an auth context
 *
 * Provides convenient methods for checking permissions and namespace access
 * in a single call.
 */
export interface PermissionContext {
  /** The underlying auth context */
  authContext: AuthContext;

  /**
   * Check both permission and namespace access
   *
   * @param permission - Required permission
   * @param namespace - Target namespace
   * @returns Combined permission check result
   */
  check(permission: Permission, namespace: string): PermissionCheckResult;

  /**
   * Check only permission (no namespace check)
   *
   * @param permission - Required permission
   * @returns Permission check result
   */
  hasPermission(permission: Permission): boolean;

  /**
   * Check only namespace access (no permission check)
   *
   * @param namespace - Target namespace
   * @returns Namespace access check result
   */
  hasNamespaceAccess(namespace: string): boolean;
}

/**
 * Create a permission context from an auth context
 *
 * @param authContext - The authenticated context
 * @returns Permission context with convenience methods
 */
export function createPermissionContext(authContext: AuthContext): PermissionContext {
  return {
    authContext,

    check(permission: Permission, namespace: string): PermissionCheckResult {
      // First check permission
      const permResult = checkPermission(authContext, permission);
      if (!permResult.allowed) {
        return permResult;
      }

      // Then check namespace access
      return checkNamespaceAccess(authContext, namespace);
    },

    hasPermission(permission: Permission): boolean {
      return checkPermission(authContext, permission).allowed;
    },

    hasNamespaceAccess(namespace: string): boolean {
      return checkNamespaceAccess(authContext, namespace).allowed;
    },
  };
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Check if an auth context has read permission
 *
 * @param context - The authenticated context
 * @returns True if context has read permission
 */
export function hasReadPermission(context: AuthContext): boolean {
  return checkPermission(context, 'read').allowed;
}

/**
 * Check if an auth context has write permission
 *
 * @param context - The authenticated context
 * @returns True if context has write permission
 */
export function hasWritePermission(context: AuthContext): boolean {
  return checkPermission(context, 'write').allowed;
}

/**
 * Check if an auth context has internal permission
 *
 * Internal permission is typically only granted to worker bindings
 * and allows access to internal-only operations.
 *
 * @param context - The authenticated context
 * @returns True if context has internal permission
 */
export function hasInternalPermission(context: AuthContext): boolean {
  return checkPermission(context, 'internal').allowed;
}

/**
 * Create an ACL for a new entity
 *
 * @param entityId - The entity ID
 * @param ownerId - The owner's caller ID
 * @param options - Optional ACL configuration
 * @returns A new EntityACL
 */
export function createEntityACL(
  entityId: string,
  ownerId: string,
  options?: {
    readers?: string[];
    writers?: string[];
    public?: boolean;
  }
): EntityACL {
  return {
    entityId,
    owner: ownerId,
    readers: options?.readers ?? [],
    writers: options?.writers ?? [],
    public: options?.public ?? false,
  };
}

/**
 * Check if a permission result indicates access was denied
 *
 * @param result - The permission check result
 * @returns True if access was denied
 */
export function isDenied(result: PermissionCheckResult): boolean {
  return !result.allowed;
}

/**
 * Check if a permission result indicates access was granted
 *
 * @param result - The permission check result
 * @returns True if access was granted
 */
export function isAllowed(result: PermissionCheckResult): boolean {
  return result.allowed;
}

/**
 * Merge multiple permission check results
 *
 * Returns denied if any check was denied.
 *
 * @param results - Array of permission check results
 * @returns Combined result (denied if any denied)
 */
export function mergePermissionResults(
  results: PermissionCheckResult[]
): PermissionCheckResult {
  for (const result of results) {
    if (!result.allowed) {
      return result;
    }
  }
  return { allowed: true };
}
