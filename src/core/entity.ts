/**
 * Entity interface and URL utilities for GraphDB
 *
 * Provides Entity type definition, URL-to-storage path conversion,
 * namespace resolution, and entity validation.
 */

import type { EntityId, Namespace } from './types';
import { ObjectType, isEntityId, createNamespace } from './types';

// ============================================================================
// Triple Value (for internal triple storage)
// ============================================================================

/**
 * Triple value - the object part of a subject-predicate-object triple.
 * Value varies based on the ObjectType.
 *
 * Only one value field should be set, corresponding to the type field.
 *
 * @example
 * ```typescript
 * const stringValue: TripleValue = { type: ObjectType.STRING, string: "hello" };
 * const refValue: TripleValue = { type: ObjectType.REF, ref: entityId };
 * ```
 */
export interface TripleValue {
  type: ObjectType;
  ref?: EntityId; // For REF type
  string?: string; // For STRING type
  int64?: bigint; // For INT64 type
  float64?: number; // For FLOAT64 type
  bool?: boolean; // For BOOL type
  timestamp?: bigint; // For TIMESTAMP (epoch ms, bigint for precision)
  json?: unknown; // For JSON type
  lat?: number; // For GEO_POINT
  lng?: number; // For GEO_POINT
}

// ============================================================================
// Entity Interface
// ============================================================================

/**
 * Entity - A graph entity with URL-based identity.
 *
 * Uses JSON-LD style $id, $type, $context for semantic compatibility.
 * Field names MUST NOT contain colons (JS/TS native naming).
 *
 * @example
 * ```typescript
 * const user: Entity = {
 *   $id: createEntityId("https://example.com/users/123"),
 *   $type: "User",
 *   $context: "https://example.com/users",
 *   _namespace: createNamespace("https://example.com/"),
 *   _localId: "123",
 *   name: "Alice",
 *   email: "alice@example.com"
 * };
 * ```
 */
export interface Entity {
  /** URL-based entity identifier */
  $id: EntityId;
  /** Entity type(s) - string or array of strings */
  $type: string | string[];
  /** Context URL (usually parent path of $id) */
  $context: string;

  // Derived from $id for storage routing
  /** Namespace extracted from $id hostname */
  _namespace: Namespace;
  /** Local ID extracted from $id path */
  _localId: string;

  /** Properties (JS/TS native field names - NO colons) */
  [predicate: string]: unknown;
}

// ============================================================================
// Field Name Validation
// ============================================================================

/**
 * Valid characters for field names: alphanumeric, underscore, and $ prefix
 * NO colons, spaces, or other special characters
 */
const FIELD_NAME_PATTERN = /^[$_a-zA-Z][a-zA-Z0-9_$]*$/;

/**
 * Check if a field name is valid (no colons, valid JS identifier-like).
 *
 * Valid field names must:
 * - Not contain colons (no RDF prefixes)
 * - Not contain whitespace
 * - Match JS identifier pattern (start with letter, $, or _, followed by alphanumeric)
 *
 * @param name - The field name to validate
 * @returns True if the field name is valid
 * @example
 * ```typescript
 * isValidFieldName("name")        // true
 * isValidFieldName("$type")       // true
 * isValidFieldName("schema:name") // false - contains colon
 * ```
 */
export function isValidFieldName(name: string): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  // Must not contain colons (no RDF prefixes)
  if (name.includes(':')) {
    return false;
  }

  // Must not contain whitespace
  if (/\s/.test(name)) {
    return false;
  }

  // Must match valid field name pattern
  return FIELD_NAME_PATTERN.test(name);
}

// ============================================================================
// URL Utilities
// ============================================================================

/**
 * Convert a URL to a reverse-domain storage path.
 *
 * Domain parts are reversed and prefixed with dots for hierarchical storage.
 * This enables efficient prefix-based lookups in key-value stores.
 *
 * @param url - The URL to convert (must be valid http/https)
 * @returns The reverse-domain storage path
 * @throws Error if the URL is not a valid EntityId
 * @example
 * ```typescript
 * urlToStoragePath("https://example.com/crm/acme/customer/123")
 * // Returns: ".com/.example/crm/acme/customer/123"
 *
 * urlToStoragePath("https://api.example.com/users/456")
 * // Returns: ".com/.example/.api/users/456"
 * ```
 */
export function urlToStoragePath(url: string): string {
  if (!isEntityId(url)) {
    throw new Error(
      `Invalid URL for storage path: "${url}". Must be a valid http:// or https:// URL.`
    );
  }

  const parsed = new URL(url);

  // Reverse the hostname parts and prefix each with dots, join with /
  // e.g., "example.com" -> ["com", "example"] -> ".com/.example"
  const hostParts = parsed.hostname.split('.');
  const reversedHost = hostParts
    .reverse()
    .map((part) => `.${part}`)
    .join('/');

  // Combine with path (pathname already starts with / or is empty)
  const path = parsed.pathname;

  // Handle edge case: if pathname is "/" we return just the host with /
  // If pathname is empty (no trailing slash), we return just the host
  if (path === '/') {
    return reversedHost + '/';
  } else if (path === '') {
    return reversedHost;
  }

  // Pathname starts with / which works as separator
  return reversedHost + path;
}

/**
 * Convert a storage path back to a URL.
 *
 * Reverses urlToStoragePath() by reconstructing the original URL
 * from the reverse-domain storage path.
 *
 * @param path - The storage path to convert
 * @returns The original URL (always https://)
 * @example
 * ```typescript
 * storagePathToUrl(".com/.example/crm/acme/customer/123")
 * // Returns: "https://example.com/crm/acme/customer/123"
 * ```
 */
export function storagePathToUrl(path: string): string {
  // Split by / but keep track of which parts are domain parts (start with .)
  const parts = path.split('/');

  // Collect domain parts (start with .)
  const domainParts: string[] = [];
  let pathStartIndex = 0;

  for (let i = 0; i < parts.length; i++) {
    const part = parts[i];
    if (part !== undefined && part.startsWith('.')) {
      // Remove the leading dot and add to domain parts
      domainParts.push(part.substring(1));
      pathStartIndex = i + 1;
    } else {
      break;
    }
  }

  // Reverse domain parts to get proper hostname
  const hostname = domainParts.reverse().join('.');

  // Get the path parts
  const pathParts = parts.slice(pathStartIndex);
  const pathname = pathParts.length > 0 ? '/' + pathParts.join('/') : '';

  // Handle trailing slash case
  if (path.endsWith('/') && !pathname.endsWith('/')) {
    return `https://${hostname}${pathname || '/'}`;
  }

  return `https://${hostname}${pathname}`;
}

/**
 * Resolve namespace, context, and localId from a URL.
 *
 * Extracts the hierarchical components of an entity URL for routing
 * and context resolution.
 *
 * @param url - Entity URL (e.g., "https://example.com/crm/acme/customer/123")
 * @returns Object with namespace, context, and localId
 * @throws Error if the URL is not a valid EntityId
 * @example
 * ```typescript
 * const result = resolveNamespace("https://example.com/crm/acme/customer/123");
 * // result = {
 * //   namespace: "https://example.com",
 * //   context: "https://example.com/crm/acme/customer",
 * //   localId: "123"
 * // }
 * ```
 */
export function resolveNamespace(url: string): {
  namespace: Namespace;
  context: string;
  localId: string;
} {
  if (!isEntityId(url)) {
    throw new Error(
      `Invalid URL for namespace resolution: "${url}". Must be a valid http:// or https:// URL.`
    );
  }

  const parsed = new URL(url);

  // Namespace is just protocol + hostname
  const namespace = createNamespace(`${parsed.protocol}//${parsed.hostname}`);

  // Path segments (filter out empty strings from leading/trailing slashes)
  const pathSegments = parsed.pathname.split('/').filter((s) => s !== '');

  // Local ID is the last path segment (or empty if no path)
  const localId = pathSegments.length > 0 ? (pathSegments[pathSegments.length - 1] ?? '') : '';

  // Handle trailing slash - context includes full path, localId is empty
  if (parsed.pathname.endsWith('/') && parsed.pathname !== '/') {
    return {
      namespace,
      context: `${parsed.protocol}//${parsed.hostname}${parsed.pathname.slice(0, -1)}`,
      localId: '',
    };
  }

  // Context is the path minus the last segment
  if (pathSegments.length === 0) {
    return {
      namespace,
      context: `${parsed.protocol}//${parsed.hostname}`,
      localId: '',
    };
  }

  if (pathSegments.length === 1) {
    return {
      namespace,
      context: `${parsed.protocol}//${parsed.hostname}`,
      localId: pathSegments[0] ?? '',
    };
  }

  return {
    namespace,
    context: `${parsed.protocol}//${parsed.hostname}/${pathSegments.slice(0, -1).join('/')}`,
    localId,
  };
}

/**
 * Parse an EntityId into its component parts.
 *
 * Breaks down a URL-based entity ID into its constituent parts
 * for routing and storage operations.
 *
 * @param id - EntityId to parse
 * @returns Object with protocol, hostname, path array, and localId
 * @example
 * ```typescript
 * const parts = parseEntityId(createEntityId("https://example.com/users/123"));
 * // parts = {
 * //   protocol: "https:",
 * //   hostname: "example.com",
 * //   path: ["users", "123"],
 * //   localId: "123"
 * // }
 * ```
 */
export function parseEntityId(id: EntityId): {
  protocol: string;
  hostname: string;
  path: string[];
  localId: string;
} {
  const parsed = new URL(id);

  // Path segments (filter out empty strings)
  const pathSegments = parsed.pathname.split('/').filter((s) => s !== '');

  return {
    protocol: parsed.protocol,
    hostname: parsed.hostname,
    path: pathSegments,
    localId: pathSegments.length > 0 ? (pathSegments[pathSegments.length - 1] ?? '') : '',
  };
}

// ============================================================================
// Entity Construction and Validation
// ============================================================================

/** Reserved field names that cannot be overwritten by user properties */
const RESERVED_FIELDS = new Set(['$id', '$type', '$context', '_namespace', '_localId']);

/**
 * Create an Entity from an ID, type, and properties.
 *
 * Automatically resolves namespace and context from the entity ID
 * and validates all property field names.
 *
 * @param id - EntityId (URL)
 * @param type - Type or array of types
 * @param props - Additional properties (field names must not contain colons)
 * @returns Entity object with resolved namespace and context
 * @throws Error if properties contain invalid field names or reserved fields
 * @example
 * ```typescript
 * const user = createEntity(
 *   createEntityId("https://example.com/users/123"),
 *   "User",
 *   { name: "Alice", email: "alice@example.com" }
 * );
 * ```
 */
export function createEntity(
  id: EntityId,
  type: string | string[],
  props: Record<string, unknown>
): Entity {
  // Validate all property field names
  for (const key of Object.keys(props)) {
    // Check for reserved fields
    if (RESERVED_FIELDS.has(key)) {
      throw new Error(
        `Cannot use reserved field name "${key}" in entity properties.`
      );
    }

    // Check for colons (no RDF prefixes)
    if (key.includes(':')) {
      throw new Error(
        `Invalid field name "${key}". Field names must NOT contain colons (no RDF prefixes).`
      );
    }

    // Check for valid field name pattern
    if (!isValidFieldName(key)) {
      throw new Error(
        `Invalid field name "${key}". Must be a valid JS identifier-like name.`
      );
    }
  }

  // Resolve namespace info from ID
  const { namespace, context, localId } = resolveNamespace(id);

  return {
    $id: id,
    $type: type,
    $context: context,
    _namespace: namespace,
    _localId: localId,
    ...props,
  };
}

/**
 * Validate an Entity object.
 *
 * Checks that:
 * - $id is present and is a valid EntityId
 * - $type is present
 * - No field names contain colons
 *
 * @param entity - Entity to validate
 * @returns Object with valid boolean and array of error messages
 * @example
 * ```typescript
 * const result = validateEntity(entity);
 * if (!result.valid) {
 *   console.error("Validation errors:", result.errors);
 * }
 * ```
 */
export function validateEntity(entity: Entity): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Check required fields
  if (!entity.$id) {
    errors.push('$id is required');
  } else if (!isEntityId(entity.$id)) {
    errors.push('$id must be a valid http:// or https:// URL');
  }

  if (!entity.$type) {
    errors.push('$type is required');
  }

  // Check all field names for colons
  for (const key of Object.keys(entity)) {
    // Skip reserved/system fields
    if (RESERVED_FIELDS.has(key)) {
      continue;
    }

    if (key.includes(':')) {
      errors.push(
        `Field "${key}" contains a colon. Field names must NOT contain colons (no RDF prefixes).`
      );
    }
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

