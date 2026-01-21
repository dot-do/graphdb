/**
 * Cloudflare Snippet: Bloom Filter Router
 *
 * Implements FREE negative lookup elimination at the edge.
 * If an entity definitely doesn't exist, return 404 immediately.
 * If entity might exist, pass through to origin.
 *
 * IMPORTANT: Cloudflare Snippets Constraints
 * ------------------------------------------
 * - 32KB max script size (STRICTLY ENFORCED - see npm run check:snippet-size)
 * - 5ms max compute time
 * - 32MB max memory
 * - No Node.js APIs - pure JS only
 *
 * Size Budget: This script must stay under 32KB when bundled/minified.
 * Run `npm run check:snippet-size` to verify compliance before deployment.
 * CI will fail if the bundled size exceeds 32KB.
 *
 * This implementation: ~1.6KB minified (~5% of budget)
 */

// Bloom filter configuration - injected at build time
// These values are placeholders replaced by build process
const BLOOM_CONFIG = {
  // Base64-encoded bloom filter bits
  filter: "{{BLOOM_FILTER_BASE64}}",
  // Number of hash functions
  k: 7,
  // Number of bits in filter
  m: 95850,
  // Version for cache busting
  version: "{{BLOOM_VERSION}}"
};

// Pre-decode filter on cold start (outside request handler)
let filterBits = null;

/**
 * Decode base64 to Uint8Array - minimal implementation
 * Works in Cloudflare Workers/Snippets environment
 */
function decodeBase64(str) {
  const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const lookup = new Uint8Array(128);
  for (let i = 0; i < chars.length; i++) {
    lookup[chars.charCodeAt(i)] = i;
  }

  // Remove padding and calculate output length
  let len = str.length;
  while (str[len - 1] === "=") len--;
  const outLen = (len * 6) >> 3;

  const out = new Uint8Array(outLen);
  let bits = 0;
  let value = 0;
  let outIdx = 0;

  for (let i = 0; i < len; i++) {
    value = (value << 6) | lookup[str.charCodeAt(i)];
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out[outIdx++] = (value >> bits) & 0xff;
    }
  }

  return out;
}

/**
 * Initialize bloom filter from config
 * Called once on cold start
 */
function initFilter() {
  if (filterBits !== null) return;

  // Check if filter is configured (not placeholder)
  if (BLOOM_CONFIG.filter.startsWith("{{")) {
    filterBits = new Uint8Array(0);
    return;
  }

  filterBits = decodeBase64(BLOOM_CONFIG.filter);
}

/**
 * Fast hash function using FNV-1a variant
 * Produces two independent hashes for double hashing
 *
 * @param {string} key - Entity ID to hash
 * @returns {[number, number]} - Two 32-bit hashes
 */
function hash(key) {
  // FNV-1a parameters
  const FNV_PRIME = 0x01000193;
  const FNV_OFFSET = 0x811c9dc5;

  let h1 = FNV_OFFSET;
  let h2 = FNV_OFFSET;

  for (let i = 0; i < key.length; i++) {
    const c = key.charCodeAt(i);
    // First hash: standard FNV-1a
    h1 ^= c;
    h1 = Math.imul(h1, FNV_PRIME);
    // Second hash: FNV-1a with different mixing
    h2 ^= c;
    h2 = Math.imul(h2, FNV_PRIME);
    h2 ^= h2 >>> 16;
  }

  // Ensure positive 32-bit integers
  return [h1 >>> 0, h2 >>> 0];
}

/**
 * Check if a bit is set in the bloom filter
 * Uses byte-level access for efficiency
 *
 * @param {number} bitIndex - Index of bit to check
 * @returns {boolean} - True if bit is set
 */
function getBit(bitIndex) {
  const byteIndex = bitIndex >>> 3;
  const bitOffset = bitIndex & 7;

  if (byteIndex >= filterBits.length) return false;

  return (filterBits[byteIndex] & (1 << bitOffset)) !== 0;
}

/**
 * Check if entity might exist in the bloom filter
 * Uses double hashing: h(i) = h1 + i*h2
 *
 * @param {string} entityId - Entity ID to check
 * @returns {boolean} - False = definitely not in set, True = might be in set
 */
function mightExist(entityId) {
  initFilter();

  // Empty filter means unconfigured - pass through
  if (filterBits.length === 0) return true;

  const [h1, h2] = hash(entityId);
  const m = BLOOM_CONFIG.m;
  const k = BLOOM_CONFIG.k;

  for (let i = 0; i < k; i++) {
    // Double hashing: position = (h1 + i * h2) mod m
    const pos = ((h1 + Math.imul(i, h2)) >>> 0) % m;
    if (!getBit(pos)) {
      return false; // Definitely not in set
    }
  }

  return true; // Might be in set (could be false positive)
}

/**
 * Extract entity ID from request path
 * Supports patterns:
 * - /entities/{id}
 * - /api/v1/entities/{id}
 * - /graph/{type}/{id}
 *
 * @param {URL} url - Request URL
 * @returns {string|null} - Entity ID or null if not an entity lookup
 */
function extractEntityId(url) {
  const path = url.pathname;

  // Pattern: /entities/{id}
  const entityMatch = path.match(/^\/entities\/([^\/]+)$/);
  if (entityMatch) return entityMatch[1];

  // Pattern: /api/v1/entities/{id}
  const apiMatch = path.match(/^\/api\/v\d+\/entities\/([^\/]+)$/);
  if (apiMatch) return apiMatch[1];

  // Pattern: /graph/{type}/{id}
  const graphMatch = path.match(/^\/graph\/[^\/]+\/([^\/]+)$/);
  if (graphMatch) return graphMatch[1];

  return null;
}

/**
 * Create 404 response for non-existent entity
 * Includes cache headers for edge caching
 *
 * @param {string} entityId - Entity ID that doesn't exist
 * @returns {Response} - 404 response
 */
function notFoundResponse(entityId) {
  return new Response(
    JSON.stringify({
      error: "not_found",
      message: `Entity '${entityId}' does not exist`,
      bloom_version: BLOOM_CONFIG.version
    }),
    {
      status: 404,
      headers: {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=60",
        "X-Bloom-Result": "negative",
        "X-Bloom-Version": BLOOM_CONFIG.version
      }
    }
  );
}

/**
 * Main request handler for Cloudflare Snippet
 *
 * @param {Request} request - Incoming request
 * @returns {Response|undefined} - 404 if entity doesn't exist, undefined to pass through
 */
export default {
  async fetch(request, env, ctx) {
    // Only check GET requests for entities
    if (request.method !== "GET") {
      return; // Pass through to origin
    }

    const url = new URL(request.url);
    const entityId = extractEntityId(url);

    // Not an entity lookup - pass through
    if (!entityId) {
      return; // Pass through to origin
    }

    // Check bloom filter
    if (!mightExist(entityId)) {
      // Entity definitely doesn't exist - return 404 immediately
      return notFoundResponse(entityId);
    }

    // Entity might exist - pass through to origin
    // Origin will do the real lookup and return actual data or 404
    return; // Pass through
  }
};

// Export for testing
export { mightExist, hash, extractEntityId, initFilter, BLOOM_CONFIG };
