/**
 * Shard Router for GraphDB
 *
 * Routes queries and entity lookups to appropriate Durable Object shards.
 * Uses consistent hashing based on namespace to determine shard assignment.
 *
 * Key design principles:
 * - Deterministic routing: same namespace always routes to same shard
 * - Namespace extraction: derive namespace from URL structure
 * - Cross-namespace query detection: identify when queries span multiple shards
 * - Cache-friendly: generate stable cache keys for read queries
 */

import type { EntityId, Namespace } from '../core/types';
import { createNamespace } from '../core/types';
import { fnv1aHash, hashToHex } from '../core/hash';
import {
  DEFAULT_SHARD_COUNT as SHARD_COUNT_DEFAULT,
  DEFAULT_CACHE_TTL_SECONDS,
  MAX_QUERY_COST,
  CROSS_NAMESPACE_COST,
  BASE_QUERY_COST,
  FILTER_COST_MULTIPLIER,
  URL_CONTEXT_LOOKBACK_CHARS,
  CACHE_KEY_HEX_PAD_LENGTH,
} from './constants.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Information about a shard that handles a specific namespace.
 *
 * Used by the router to identify which Durable Object shard
 * should handle queries for a given namespace.
 *
 * @example
 * ```typescript
 * const shard: ShardInfo = {
 *   namespace: createNamespace("https://example.com/"),
 *   shardId: "shard-42-a1b2c3d4"
 * };
 * ```
 */
export interface ShardInfo {
  /** Namespace this shard is responsible for */
  namespace: Namespace;
  /** Durable Object ID name for this shard */
  shardId: string;
  /** Optional region hint for locality */
  region?: string;
}

/**
 * Result from routing a query.
 *
 * Contains the list of shards to query and optional caching hints.
 *
 * @example
 * ```typescript
 * const result = routeQuery("https://example.com/users/123.friends");
 * for (const shard of result.shards) {
 *   const stub = env.SHARD.get(env.SHARD.idFromName(shard.shardId));
 *   // Query the shard...
 * }
 * ```
 */
export interface RouteResult {
  /** One or more shards to query */
  shards: ShardInfo[];
  /** Optional cache key for result (present if query is cacheable) */
  cacheKey?: string;
  /** Cache TTL in seconds (present if query is cacheable) */
  ttl?: number;
}

// ============================================================================
// Constants (re-exported from constants.ts)
// ============================================================================

/** Default number of shards for multi-shard scaling */
export const DEFAULT_SHARD_COUNT = SHARD_COUNT_DEFAULT;

// ============================================================================
// Namespace Extraction
// ============================================================================

/**
 * Extract namespace from an entity ID URL.
 *
 * Strategy:
 * - Parse URL to get origin + first path segment
 * - If multiple path segments exist, uses origin + first segment
 * - Otherwise, uses just the origin
 *
 * @param entityId - The entity ID URL to extract namespace from
 * @returns The namespace for routing purposes
 * @example
 * ```typescript
 * extractNamespace(createEntityId("https://example.com/crm/acme/customer/123"))
 * // Returns: "https://example.com/crm/"
 *
 * extractNamespace(createEntityId("https://example.com/users/123"))
 * // Returns: "https://example.com/"
 * ```
 */
export function extractNamespace(entityId: EntityId): Namespace {
  try {
    const url = new URL(entityId);
    const pathParts = url.pathname.split('/').filter((p) => p.length > 0);

    // If there's at least one path segment and more parts after it,
    // use origin + first segment as namespace
    if (pathParts.length > 1) {
      return createNamespace(`${url.origin}/${pathParts[0]}/`);
    }

    // Otherwise, use just the origin
    return createNamespace(`${url.origin}/`);
  } catch {
    // Fallback: use the whole string as namespace
    return createNamespace(entityId);
  }
}

/**
 * Extract all URLs from a query string
 * Finds https:// URLs in the query for namespace detection
 *
 * URLs are terminated by:
 * - Whitespace
 * - Query operators: [ ] ( ) ' "
 * - Traversal dot followed by non-path character (e.g., .friends but not .com)
 */
function extractUrlsFromQuery(query: string): string[] {
  // Pattern matches URLs up until traversal notation or operators
  // URL path can contain dots (like .com, .org) but we stop at .word patterns
  // that look like property traversals (lowercase word after dot at end of URL-like portion)
  const urlPattern = /https?:\/\/[^\s\[\]()'"]+?(?=\.[a-z]+[^\w/]|\.(?=[a-z]+$)|\s|\[|\]|\(|\)|'|"|$)/gi;
  const matches = query.match(urlPattern) || [];

  // Also try simpler pattern if above yields no results
  if (matches.length === 0) {
    const simplePattern = /https?:\/\/[^\s\[\]()'"]+/g;
    const simpleMatches = query.match(simplePattern) || [];
    return simpleMatches.map((url) => {
      // Trim trailing property traversals (e.g., .friends, .name)
      // Keep domain dots (.com, .org, .io)
      return url.replace(/\.([a-z_][a-z0-9_]*)$/i, '');
    });
  }

  return matches;
}

/**
 * Count traversal hops in a query string
 * Traversals are dot-separated property accesses like .friends, .name
 */
function countTraversalHops(query: string): number {
  // Find traversal patterns: .propertyName after URL or another traversal
  // These are lowercase property names (not TLDs like .com)
  const traversalPattern = /\.([a-z_][a-z0-9_]*)/gi;
  const matches = query.match(traversalPattern) || [];

  // Filter out TLDs and path segments
  const tlds = new Set([
    'com',
    'org',
    'net',
    'io',
    'dev',
    'co',
    'edu',
    'gov',
    'mil',
  ]);
  let hops = 0;

  for (const match of matches) {
    const prop = match.slice(1).toLowerCase();
    // Skip if it looks like a TLD or is in a URL context
    if (!tlds.has(prop)) {
      // Check if this is after a URL path (has / before the dot)
      const idx = query.indexOf(match);
      if (idx > 0) {
        // Look back to check if this is in a URL context
        const before = query.slice(Math.max(0, idx - URL_CONTEXT_LOOKBACK_CHARS), idx);
        // If the dot is preceded by path-like content (ends with /something)
        // and the property looks like a real traversal, count it
        if (before.includes('/') || hops > 0) {
          hops++;
        }
      }
    }
  }

  return hops;
}

// ============================================================================
// Routing Functions
// ============================================================================

/**
 * Route a single entity lookup to its shard.
 *
 * Determines which shard should handle queries for a specific entity
 * based on its namespace.
 *
 * @param entityId - The entity ID to route
 * @returns ShardInfo containing the shard ID and namespace
 * @example
 * ```typescript
 * const shard = routeEntity(createEntityId("https://example.com/users/123"));
 * const stub = env.SHARD.get(env.SHARD.idFromName(shard.shardId));
 * ```
 */
export function routeEntity(entityId: EntityId): ShardInfo {
  const namespace = extractNamespace(entityId);
  const shardId = getShardId(namespace);

  return {
    namespace,
    shardId,
  };
}

/**
 * Route a query that may span multiple namespaces.
 *
 * Analyzes the query string to identify all URLs/namespaces involved,
 * then returns the appropriate shards to query. Also determines if the
 * query is cacheable and provides cache hints.
 *
 * This is the primary routing function used by the snippet layer to
 * direct queries to the correct Durable Object shards.
 *
 * @param query - The query string (may contain multiple entity URLs)
 * @returns RouteResult with shards to query and optional cache hints
 * @example
 * ```typescript
 * // Single namespace query
 * const result1 = routeQuery("https://example.com/users/123.friends");
 * // result1.shards.length === 1
 *
 * // Cross-namespace query
 * const result2 = routeQuery(
 *   "https://org-a.com/users/1.partners -> https://org-b.com/users/2"
 * );
 * // result2.shards.length === 2
 *
 * // Cacheable query
 * const result3 = routeQuery("https://example.com/users/123");
 * // result3.cacheKey !== undefined
 * // result3.ttl === 300 (default 5 minutes)
 * ```
 */
export function routeQuery(query: string): RouteResult {
  // Extract all URLs from the query
  const urls = extractUrlsFromQuery(query);

  // Get unique namespaces
  const namespaceSet = new Set<string>();
  const shards: ShardInfo[] = [];

  for (const url of urls) {
    try {
      // Try to parse as EntityId
      const urlObj = new URL(url);
      if (urlObj.protocol === 'http:' || urlObj.protocol === 'https:') {
        const namespace = extractNamespaceFromUrl(url);
        if (!namespaceSet.has(namespace)) {
          namespaceSet.add(namespace);
          const ns = createNamespace(namespace);
          shards.push({
            namespace: ns,
            shardId: getShardId(ns),
          });
        }
      }
    } catch {
      // Skip invalid URLs
    }
  }

  // Ensure at least one shard if no URLs found
  if (shards.length === 0 && urls.length > 0) {
    const firstUrl = urls[0];
    if (firstUrl !== undefined) {
      const ns = createNamespace(firstUrl);
      shards.push({
        namespace: ns,
        shardId: getShardId(ns),
      });
    }
  }

  // Determine if query is cacheable
  const cacheable = canServeFromCache(query);

  const result: RouteResult = {
    shards,
  };
  if (cacheable) {
    result.cacheKey = generateCacheKey(query);
    result.ttl = DEFAULT_CACHE_TTL_SECONDS;
  }
  return result;
}

/**
 * Helper to extract namespace from URL string (not EntityId branded)
 */
function extractNamespaceFromUrl(url: string): string {
  try {
    const urlObj = new URL(url);
    const pathParts = urlObj.pathname.split('/').filter((p) => p.length > 0);

    if (pathParts.length > 1) {
      return `${urlObj.origin}/${pathParts[0]}/`;
    }

    return `${urlObj.origin}/`;
  } catch {
    return url;
  }
}

/**
 * Get shard index from namespace using consistent hashing.
 *
 * Uses FNV-1a hash modulo shard count to determine which shard
 * a namespace belongs to. This provides deterministic routing
 * and good distribution across shards.
 *
 * @param namespace - The namespace to route
 * @param shardCount - Number of shards (default: DEFAULT_SHARD_COUNT = 256)
 * @returns Shard index in range [0, shardCount)
 * @example
 * ```typescript
 * const ns = createNamespace("https://example.com/");
 * const idx = getShardIndex(ns);      // 0-255
 * const idx16 = getShardIndex(ns, 16); // 0-15
 * ```
 */
export function getShardIndex(
  namespace: Namespace,
  shardCount: number = DEFAULT_SHARD_COUNT
): number {
  const hash = fnv1aHash(namespace);
  return hash % shardCount;
}

/**
 * Get shard ID from namespace using consistent hashing.
 *
 * Generates a unique shard ID that includes both the shard index
 * (for routing) and a hash suffix (for uniqueness).
 *
 * @param namespace - The namespace to route
 * @param shardCount - Number of shards (default: DEFAULT_SHARD_COUNT = 256)
 * @returns Shard ID string in format "shard-{index}-{hash}"
 * @example
 * ```typescript
 * const ns = createNamespace("https://example.com/");
 * const id = getShardId(ns); // "shard-42-a1b2c3d4"
 * const stub = env.SHARD.get(env.SHARD.idFromName(id));
 * ```
 */
export function getShardId(
  namespace: Namespace,
  shardCount: number = DEFAULT_SHARD_COUNT
): string {
  const hash = fnv1aHash(namespace);
  const shardIndex = hash % shardCount;
  return `shard-${shardIndex}-${hashToHex(hash)}`;
}

// ============================================================================
// Caching Functions
// ============================================================================

/**
 * Determine if a query can be served from cache.
 *
 * Cacheable queries are:
 * - Read-only (no MUTATE, INSERT, DELETE, UPDATE keywords)
 * - Not time-sensitive (no NOW(), CURRENT_TIMESTAMP)
 *
 * @param query - The query string to check
 * @returns True if the query is cacheable
 * @example
 * ```typescript
 * canServeFromCache("https://example.com/users/123.friends"); // true
 * canServeFromCache("UPDATE https://example.com/users/123"); // false
 * canServeFromCache("SELECT * WHERE createdAt > NOW()");     // false
 * ```
 */
export function canServeFromCache(query: string): boolean {
  const upperQuery = query.toUpperCase();

  // Check for mutation keywords
  const mutationKeywords = ['MUTATE', 'INSERT', 'DELETE', 'UPDATE', 'SET'];
  for (const keyword of mutationKeywords) {
    if (upperQuery.includes(keyword)) {
      return false;
    }
  }

  // Check for time-sensitive functions
  const timeFunctions = ['NOW()', 'CURRENT_TIMESTAMP', 'CURRENT_DATE'];
  for (const fn of timeFunctions) {
    if (upperQuery.includes(fn)) {
      return false;
    }
  }

  return true;
}

/**
 * Generate a deterministic cache key for a query.
 *
 * Uses FNV-1a hash to produce a short, consistent key.
 * The key is prefixed with "gdb-" for namespacing in shared caches.
 *
 * @param query - The query string to generate a key for
 * @returns Cache key string in format "gdb-{hash}"
 * @example
 * ```typescript
 * const key = generateCacheKey("https://example.com/users/123.friends");
 * // key = "gdb-a1b2c3d4" (deterministic for same query)
 * await cache.put(key, response, { expirationTtl: 300 });
 * ```
 */
export function generateCacheKey(query: string): string {
  // Normalize: trim whitespace
  const normalized = query.trim();

  // Hash the normalized query
  const hash = fnv1aHash(normalized);

  // Return as hex string with prefix (8-char hex for consistent key length)
  return `gdb-${hash.toString(16).padStart(CACHE_KEY_HEX_PAD_LENGTH, '0')}`;
}

// ============================================================================
// Cost Estimation
// ============================================================================

/**
 * Estimate query cost for rate limiting.
 *
 * Cost factors:
 * - Base cost: 1 for entity lookup
 * - Hop cost: +1 for each traversal (detected by '.')
 * - Cross-namespace: +5 for each additional namespace
 * - Filter cost: +2 for filters (detected by '[?')
 *
 * Capped at MAX_QUERY_COST (100).
 *
 * @param query - The query string to estimate cost for
 * @returns Estimated cost (1-100)
 * @example
 * ```typescript
 * estimateQueryCost("user:123");               // 1 (base)
 * estimateQueryCost("user:123.friends");       // 2 (base + 1 hop)
 * estimateQueryCost("user:123.friends.posts"); // 3 (base + 2 hops)
 * estimateQueryCost("user:123.friends[?age > 30]"); // 4 (base + 1 hop + filter)
 * ```
 */
export function estimateQueryCost(query: string): number {
  let cost = BASE_QUERY_COST; // Base cost for entity lookup

  // Count traversal hops using specialized function
  const traversalHops = countTraversalHops(query);
  cost += traversalHops;

  // Count unique namespaces
  const urls = extractUrlsFromQuery(query);
  const namespaceSet = new Set<string>();
  for (const url of urls) {
    const ns = extractNamespaceFromUrl(url);
    namespaceSet.add(ns);
  }

  // Add cost for cross-namespace queries (first namespace is "free")
  if (namespaceSet.size > 1) {
    cost += (namespaceSet.size - 1) * CROSS_NAMESPACE_COST;
  }

  // Add cost for filters (each filter adds to query complexity)
  const filterCount = (query.match(/\[\?/g) || []).length;
  cost += filterCount * FILTER_COST_MULTIPLIER;

  // Cap at maximum to prevent abuse
  return Math.min(cost, MAX_QUERY_COST);
}
