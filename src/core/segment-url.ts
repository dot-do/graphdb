/**
 * URL Sort Key Utilities
 *
 * Generates sort keys from URLs for storage locality optimization.
 * Data stores actual URLs (usable, dereferenceable), sort keys are derived.
 *
 * Example:
 *   URL: https://blog.example.com/posts/2024/hello?q=foo#top
 *   Sort Key: com,example,blog,/posts,/2024,/hello
 *
 * Benefits:
 * - URLs remain valid and usable
 * - Sort keys provide storage locality (all example.com adjacent)
 * - Prefix queries on sort key: 'com,example,%'
 * - Compression benefits from sorted data
 */

/**
 * Known external ID property mappings to canonical URL patterns
 * Property ID → [domain segments reversed, path prefix, value transform]
 */
export const EXTERNAL_ID_MAPPINGS: Record<string, {
  segments: string;
  transform?: (value: string) => string;
}> = {
  // Social/Web
  P2002: { segments: 'com,x' },                          // Twitter/X username
  P2003: { segments: 'com,instagram' },                  // Instagram username
  P2013: { segments: 'com,facebook' },                   // Facebook ID
  P2037: { segments: 'com,github' },                     // GitHub username
  P2397: { segments: 'com,youtube,channel' },            // YouTube channel ID
  P4264: { segments: 'com,linkedin,company' },           // LinkedIn company ID
  P6634: { segments: 'com,linkedin,in' },                // LinkedIn person ID

  // Entertainment
  P345:  { segments: 'com,imdb,name' },                  // IMDb ID (person)
  P4947: { segments: 'com,imdb,title' },                 // IMDb ID (title)
  P1651: { segments: 'com,youtube,watch', transform: v => `?v=${v}` }, // YouTube video
  P3984: { segments: 'com,subreddit,r' },                // Reddit subreddit
  P4265: { segments: 'com,reddit,user' },                // Reddit username
  P1953: { segments: 'com,discogs,artist' },             // Discogs artist
  P1954: { segments: 'com,discogs,master' },             // Discogs master
  P1902: { segments: 'com,spotify,artist' },             // Spotify artist ID
  P2205: { segments: 'com,spotify,album' },              // Spotify album ID
  P4903: { segments: 'com,spotify,track' },              // Spotify track ID

  // Academic/Research
  P496:  { segments: 'org,orcid' },                      // ORCID
  P214:  { segments: 'org,viaf' },                       // VIAF ID
  P213:  { segments: 'org,isni' },                       // ISNI
  P227:  { segments: 'de,dnb,d-nb' },                    // GND ID
  P244:  { segments: 'gov,loc,id' },                     // Library of Congress
  P698:  { segments: 'gov,nih,nlm,ncbi,pubmed' },        // PubMed ID
  P932:  { segments: 'gov,nih,nlm,ncbi,pmc' },           // PMC ID
  P356:  { segments: 'org,doi' },                        // DOI
  P818:  { segments: 'org,arxiv' },                      // arXiv ID

  // Geography
  P625:  { segments: 'org,openstreetmap,node' },         // OSM node (special handling for coords)
  P402:  { segments: 'org,openstreetmap,relation' },     // OSM relation
  P1566: { segments: 'org,geonames' },                   // GeoNames ID

  // Reference
  P18:   { segments: 'org,wikimedia,commons,wiki,File:' }, // Commons image
  P373:  { segments: 'org,wikimedia,commons,wiki,Category:' }, // Commons category
  P910:  { segments: 'org,wikipedia,en,wiki' },          // Wikipedia article

  // Business
  P1278: { segments: 'org,lei' },                        // Legal Entity Identifier
  P3347: { segments: 'gov,sec,edgar,cik' },              // SEC CIK
  P2671: { segments: 'com,google,kg,g' },                // Google Knowledge Graph ID
  P646:  { segments: 'com,google,freebase,m' },          // Freebase ID
};

/**
 * Generate a sort key from a URL (reversed host + path segments)
 * Used for storage locality, not as primary identifier.
 */
export function urlToSortKey(url: string): string {
  try {
    const parsed = new URL(url);
    const parts: string[] = [];

    // Host: reverse and split by dot
    const hostParts = parsed.hostname.split('.').reverse();
    parts.push(...hostParts);

    // Port (if non-standard)
    if (parsed.port && parsed.port !== '443' && parsed.port !== '80') {
      parts.push(`:${parsed.port}`);
    }

    // Path segments (keep leading slash on each)
    if (parsed.pathname && parsed.pathname !== '/') {
      const pathParts = parsed.pathname.split('/').filter(p => p);
      for (const p of pathParts) {
        parts.push(`/${p}`);
      }
    }

    // Query string (as single segment)
    if (parsed.search) {
      parts.push(parsed.search);
    }

    // Fragment (as single segment)
    if (parsed.hash) {
      parts.push(parsed.hash);
    }

    return parts.join(',');
  } catch {
    // If URL parsing fails, return as-is with a marker
    return `_invalid,${url.replace(/,/g, '%2C')}`;
  }
}

/**
 * Generate a host-only sort key (no path) from a URL
 */
export function urlToHostSortKey(url: string): string {
  try {
    const parsed = new URL(url);
    return parsed.hostname.split('.').reverse().join(',');
  } catch {
    return `_invalid`;
  }
}

/**
 * Convert sort key back to URL (for debugging/display)
 */
export function sortKeyToUrl(segments: string, scheme: string = 'https'): string {
  if (segments.startsWith('_invalid,')) {
    return segments.slice(9).replace(/%2C/g, ',');
  }

  const parts = segments.split(',');
  let hostname = '';
  let port = '';
  let path = '';
  let query = '';
  let hash = '';

  for (const part of parts) {
    if (part.startsWith('/')) {
      path += part;
    } else if (part.startsWith('?')) {
      query = part;
    } else if (part.startsWith('#')) {
      hash = part;
    } else if (part.startsWith(':')) {
      port = part;
    } else {
      // Host segment (reversed)
      hostname = hostname ? `${part}.${hostname}` : part;
    }
  }

  return `${scheme}://${hostname}${port}${path || '/'}${query}${hash}`;
}

/**
 * Convert a Wikidata entity ID to segmented format
 */
export function wikidataEntityToSegments(id: string): string {
  if (id.startsWith('Q')) {
    return `org,wikidata,entity,${id}`;
  } else if (id.startsWith('P')) {
    return `org,wikidata,prop,${id}`;
  } else if (id.startsWith('L')) {
    return `org,wikidata,lexeme,${id}`;
  }
  return `org,wikidata,${id}`;
}

/**
 * Convert an external ID to segmented canonical URL format
 */
export function externalIdToSegments(propertyId: string, value: string): string | null {
  const mapping = EXTERNAL_ID_MAPPINGS[propertyId];
  if (!mapping) {
    return null; // Unknown external ID type
  }

  const transformedValue = mapping.transform ? mapping.transform(value) : value;
  return `${mapping.segments},${transformedValue}`;
}

/**
 * Convert a Common Crawl reversed hostname to segmented format
 */
export function reversedHostToSegments(reversed: string): string {
  // CC format: "com.example.www" → "com,example,www"
  return reversed.split('.').join(',');
}

/**
 * Check if a segmented URL matches a prefix pattern
 */
export function matchesPrefix(segments: string, prefix: string): boolean {
  return segments.startsWith(prefix);
}

/**
 * Get the domain portion of segmented URL (TLD + domain)
 */
export function getDomain(segments: string): string {
  const parts = segments.split(',');
  // Find first path/query/hash segment
  let domainEnd = parts.length;
  for (let i = 0; i < parts.length; i++) {
    const part = parts[i]!;
    if (part.startsWith('/') || part.startsWith('?') || part.startsWith('#') || part.startsWith(':')) {
      domainEnd = i;
      break;
    }
  }
  return parts.slice(0, Math.min(domainEnd, 2)).join(',');
}

/**
 * Get the full host portion of segmented URL
 */
export function getHost(segments: string): string {
  const parts = segments.split(',');
  const hostParts: string[] = [];
  for (const part of parts) {
    if (part.startsWith('/') || part.startsWith('?') || part.startsWith('#')) {
      break;
    }
    if (!part.startsWith(':')) {
      hostParts.push(part);
    }
  }
  return hostParts.join(',');
}
