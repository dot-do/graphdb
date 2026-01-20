/**
 * GraphDB Explorer - Shared URL Explorer Functionality
 *
 * Provides interactive HTML-based graph exploration endpoints for all loaders.
 * Renders entities with clickable REF links for navigating the graph.
 *
 * Features:
 * - HTML rendering of entities with clickable $id and REF fields
 * - Search functionality
 * - Random entity discovery
 * - Clean, minimal CSS design
 * - Content negotiation (HTML vs JSON based on Accept header)
 *
 * @packageDocumentation
 */

// ============================================================================
// Types
// ============================================================================

/**
 * An entity with its predicates/values
 */
export interface Entity {
  $id: string;
  $type?: string;
  [predicate: string]: unknown;
}

/**
 * Search result from the search function
 */
export interface SearchResult {
  $id: string;
  $type?: string;
  label?: string;
  description?: string;
}

/**
 * Options for creating explorer routes
 */
export interface ExplorerOptions {
  /** Namespace identifier (e.g., 'imdb', 'wikidata') */
  namespace: string;
  /** Display name for the explorer (e.g., 'IMDB Graph Explorer') */
  displayName: string;
  /** Base URL for the worker (e.g., 'https://imdb-graph.workers.do') */
  baseUrl: string;
  /** Function to get an entity by ID */
  getEntity: (id: string) => Promise<Entity | null>;
  /** Function to search entities */
  searchEntities: (query: string, limit?: number) => Promise<SearchResult[]>;
  /** Function to get a random entity ID */
  getRandomEntityId: () => Promise<string | null>;
  /** Optional function to get entity count for stats */
  getEntityCount?: () => Promise<number>;
}

/**
 * Route handler result
 */
export interface RouteResult {
  handled: boolean;
  response?: Response;
}

// ============================================================================
// CSS Styles
// ============================================================================

const EXPLORER_CSS = `
:root {
  --bg: #fafafa;
  --surface: #ffffff;
  --border: #e0e0e0;
  --text: #333333;
  --text-muted: #666666;
  --link: #0066cc;
  --link-hover: #004499;
  --ref-bg: #f0f7ff;
  --ref-border: #cce0ff;
  --type-bg: #e8f5e9;
  --type-text: #2e7d32;
  --code-bg: #f5f5f5;
}

* {
  box-sizing: border-box;
  margin: 0;
  padding: 0;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  font-size: 15px;
  line-height: 1.6;
  color: var(--text);
  background: var(--bg);
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

header {
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border);
}

header h1 {
  font-size: 24px;
  font-weight: 600;
  margin-bottom: 8px;
}

nav.breadcrumb {
  font-size: 14px;
  color: var(--text-muted);
}

nav.breadcrumb a {
  color: var(--link);
  text-decoration: none;
}

nav.breadcrumb a:hover {
  text-decoration: underline;
}

nav.breadcrumb span {
  margin: 0 8px;
}

.search-box {
  margin-bottom: 24px;
}

.search-box form {
  display: flex;
  gap: 8px;
}

.search-box input[type="text"] {
  flex: 1;
  padding: 10px 14px;
  font-size: 15px;
  border: 1px solid var(--border);
  border-radius: 6px;
  outline: none;
}

.search-box input[type="text"]:focus {
  border-color: var(--link);
  box-shadow: 0 0 0 3px rgba(0, 102, 204, 0.1);
}

.search-box button {
  padding: 10px 20px;
  font-size: 15px;
  font-weight: 500;
  color: white;
  background: var(--link);
  border: none;
  border-radius: 6px;
  cursor: pointer;
}

.search-box button:hover {
  background: var(--link-hover);
}

.entity-card {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 20px;
  margin-bottom: 20px;
}

.entity-id {
  font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
  font-size: 14px;
  color: var(--link);
  word-break: break-all;
  margin-bottom: 16px;
}

.entity-type {
  display: inline-block;
  padding: 4px 10px;
  font-size: 13px;
  font-weight: 500;
  color: var(--type-text);
  background: var(--type-bg);
  border-radius: 4px;
  margin-bottom: 16px;
}

.predicates-table {
  width: 100%;
  border-collapse: collapse;
}

.predicates-table th,
.predicates-table td {
  padding: 10px 12px;
  text-align: left;
  border-bottom: 1px solid var(--border);
}

.predicates-table th {
  font-weight: 500;
  color: var(--text-muted);
  width: 200px;
  vertical-align: top;
}

.predicates-table tr:last-child th,
.predicates-table tr:last-child td {
  border-bottom: none;
}

.ref-link {
  display: inline-block;
  padding: 4px 10px;
  font-size: 13px;
  color: var(--link);
  background: var(--ref-bg);
  border: 1px solid var(--ref-border);
  border-radius: 4px;
  text-decoration: none;
  word-break: break-all;
}

.ref-link:hover {
  background: #e0efff;
  text-decoration: none;
}

.ref-list {
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
}

.value-json {
  font-family: 'SF Mono', Monaco, 'Cascadia Code', monospace;
  font-size: 13px;
  background: var(--code-bg);
  padding: 8px 12px;
  border-radius: 4px;
  overflow-x: auto;
  white-space: pre-wrap;
  word-break: break-word;
}

.search-results {
  list-style: none;
}

.search-results li {
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 12px;
}

.search-results li:hover {
  border-color: var(--link);
}

.search-results a {
  color: var(--link);
  text-decoration: none;
  font-weight: 500;
}

.search-results a:hover {
  text-decoration: underline;
}

.search-results .type {
  font-size: 13px;
  color: var(--text-muted);
  margin-left: 8px;
}

.search-results .description {
  font-size: 14px;
  color: var(--text-muted);
  margin-top: 4px;
}

.actions {
  display: flex;
  gap: 12px;
  margin-top: 16px;
}

.actions a {
  color: var(--link);
  text-decoration: none;
  font-size: 14px;
}

.actions a:hover {
  text-decoration: underline;
}

.empty-state {
  text-align: center;
  padding: 48px 20px;
  color: var(--text-muted);
}

.empty-state h2 {
  font-size: 18px;
  margin-bottom: 8px;
  color: var(--text);
}

.stats {
  font-size: 14px;
  color: var(--text-muted);
  margin-bottom: 24px;
}

footer {
  margin-top: 40px;
  padding-top: 20px;
  border-top: 1px solid var(--border);
  font-size: 13px;
  color: var(--text-muted);
  text-align: center;
}

footer a {
  color: var(--link);
  text-decoration: none;
}

@media (max-width: 600px) {
  .predicates-table th {
    width: 120px;
  }

  .search-box form {
    flex-direction: column;
  }

  .search-box button {
    width: 100%;
  }
}
`;

// ============================================================================
// HTML Rendering
// ============================================================================

/**
 * Escape HTML special characters
 */
function escapeHtml(str: string): string {
  return str
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

/**
 * Check if a value looks like an entity ID (URL)
 */
function isEntityId(value: unknown): value is string {
  if (typeof value !== 'string') return false;
  return value.startsWith('http://') || value.startsWith('https://');
}

/**
 * Render a value as HTML, making REF links clickable
 */
function renderValue(value: unknown, baseUrl: string): string {
  if (value === null || value === undefined) {
    return '<span class="value-null">null</span>';
  }

  // Check for entity ID / REF
  if (isEntityId(value)) {
    const encodedId = encodeURIComponent(value);
    return `<a href="${baseUrl}/entity/${encodedId}" class="ref-link">${escapeHtml(value)}</a>`;
  }

  // Check for array of REFs
  if (Array.isArray(value)) {
    if (value.length === 0) {
      return '<span class="value-null">[]</span>';
    }

    // If all items are entity IDs, render as ref links
    if (value.every(isEntityId)) {
      const links = value.map((v) => {
        const encodedId = encodeURIComponent(v);
        return `<a href="${baseUrl}/entity/${encodedId}" class="ref-link">${escapeHtml(v)}</a>`;
      });
      return `<div class="ref-list">${links.join('')}</div>`;
    }

    // Otherwise render as JSON
    return `<pre class="value-json">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
  }

  // Check for object (JSON)
  if (typeof value === 'object') {
    return `<pre class="value-json">${escapeHtml(JSON.stringify(value, null, 2))}</pre>`;
  }

  // Primitive values
  return `<span>${escapeHtml(String(value))}</span>`;
}

/**
 * Render an entity as an HTML card
 */
function renderEntityHtml(entity: Entity, baseUrl: string, displayName: string): string {
  const predicates = Object.entries(entity)
    .filter(([key]) => key !== '$id' && key !== '$type')
    .map(([predicate, value]) => {
      return `
        <tr>
          <th>${escapeHtml(predicate)}</th>
          <td>${renderValue(value, baseUrl)}</td>
        </tr>
      `;
    })
    .join('');

  const entityIdLink = `<a href="${baseUrl}/entity/${encodeURIComponent(entity.$id)}" class="entity-id">${escapeHtml(entity.$id)}</a>`;
  const typeTag = entity.$type
    ? `<span class="entity-type">${escapeHtml(entity.$type)}</span>`
    : '';

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escapeHtml(entity.$type || 'Entity')} - ${displayName}</title>
  <style>${EXPLORER_CSS}</style>
</head>
<body>
  <header>
    <h1>${displayName}</h1>
    <nav class="breadcrumb">
      <a href="${baseUrl}/explore">Explore</a>
      <span>/</span>
      <span>Entity</span>
    </nav>
  </header>

  <div class="entity-card">
    ${entityIdLink}
    ${typeTag}
    <table class="predicates-table">
      <tbody>
        ${predicates}
      </tbody>
    </table>
  </div>

  <div class="actions">
    <a href="${baseUrl}/random">View Random Entity</a>
    <a href="${baseUrl}/explore">Back to Search</a>
  </div>

  <footer>
    <a href="${baseUrl}/">API Documentation</a>
  </footer>
</body>
</html>
  `.trim();
}

/**
 * Render the explore landing page
 */
function renderExplorePage(
  displayName: string,
  baseUrl: string,
  entityCount?: number
): string {
  const statsHtml = entityCount !== undefined
    ? `<p class="stats">${entityCount.toLocaleString()} entities available for exploration</p>`
    : '';

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${displayName}</title>
  <style>${EXPLORER_CSS}</style>
</head>
<body>
  <header>
    <h1>${displayName}</h1>
    <nav class="breadcrumb">
      <span>Explore</span>
    </nav>
  </header>

  ${statsHtml}

  <div class="search-box">
    <form action="${baseUrl}/search" method="get">
      <input type="text" name="q" placeholder="Search for entities..." autofocus>
      <button type="submit">Search</button>
    </form>
  </div>

  <div class="actions">
    <a href="${baseUrl}/random">Discover a Random Entity</a>
    <a href="${baseUrl}/">View API Documentation</a>
  </div>

  <footer>
    <p>Interactive graph explorer powered by <a href="https://graphdb.workers.do">GraphDB</a></p>
  </footer>
</body>
</html>
  `.trim();
}

/**
 * Render search results page
 */
function renderSearchResults(
  query: string,
  results: SearchResult[],
  displayName: string,
  baseUrl: string
): string {
  let resultsHtml: string;

  if (results.length === 0) {
    resultsHtml = `
      <div class="empty-state">
        <h2>No results found</h2>
        <p>Try a different search term or <a href="${baseUrl}/random">discover a random entity</a></p>
      </div>
    `;
  } else {
    const items = results.map((result) => {
      const label = result.label || result.$id;
      const typeSpan = result.$type ? `<span class="type">(${escapeHtml(result.$type)})</span>` : '';
      const description = result.description
        ? `<div class="description">${escapeHtml(result.description)}</div>`
        : '';

      return `
        <li>
          <a href="${baseUrl}/entity/${encodeURIComponent(result.$id)}">${escapeHtml(label)}</a>
          ${typeSpan}
          ${description}
        </li>
      `;
    }).join('');

    resultsHtml = `
      <p class="stats">${results.length} result${results.length === 1 ? '' : 's'} for "${escapeHtml(query)}"</p>
      <ul class="search-results">
        ${items}
      </ul>
    `;
  }

  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Search: ${escapeHtml(query)} - ${displayName}</title>
  <style>${EXPLORER_CSS}</style>
</head>
<body>
  <header>
    <h1>${displayName}</h1>
    <nav class="breadcrumb">
      <a href="${baseUrl}/explore">Explore</a>
      <span>/</span>
      <span>Search</span>
    </nav>
  </header>

  <div class="search-box">
    <form action="${baseUrl}/search" method="get">
      <input type="text" name="q" value="${escapeHtml(query)}" placeholder="Search for entities...">
      <button type="submit">Search</button>
    </form>
  </div>

  ${resultsHtml}

  <div class="actions">
    <a href="${baseUrl}/random">View Random Entity</a>
    <a href="${baseUrl}/explore">New Search</a>
  </div>

  <footer>
    <a href="${baseUrl}/">API Documentation</a>
  </footer>
</body>
</html>
  `.trim();
}

/**
 * Render error page
 */
function renderErrorPage(
  title: string,
  message: string,
  displayName: string,
  baseUrl: string
): string {
  return `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${escapeHtml(title)} - ${displayName}</title>
  <style>${EXPLORER_CSS}</style>
</head>
<body>
  <header>
    <h1>${displayName}</h1>
    <nav class="breadcrumb">
      <a href="${baseUrl}/explore">Explore</a>
      <span>/</span>
      <span>Error</span>
    </nav>
  </header>

  <div class="empty-state">
    <h2>${escapeHtml(title)}</h2>
    <p>${escapeHtml(message)}</p>
  </div>

  <div class="actions">
    <a href="${baseUrl}/explore">Back to Search</a>
    <a href="${baseUrl}/random">Try a Random Entity</a>
  </div>

  <footer>
    <a href="${baseUrl}/">API Documentation</a>
  </footer>
</body>
</html>
  `.trim();
}

// ============================================================================
// Content Negotiation
// ============================================================================

/**
 * Check if request wants HTML (browser) or JSON (API)
 */
function wantsHtml(request: Request): boolean {
  const accept = request.headers.get('Accept') || '';
  // Check if HTML is preferred over JSON
  const htmlIndex = accept.indexOf('text/html');
  const jsonIndex = accept.indexOf('application/json');

  if (htmlIndex === -1 && jsonIndex === -1) {
    // Neither specified, check for browser user agents
    const ua = request.headers.get('User-Agent') || '';
    return /Mozilla|Chrome|Safari|Firefox|Edge/.test(ua);
  }

  if (htmlIndex === -1) return false;
  if (jsonIndex === -1) return true;
  return htmlIndex < jsonIndex;
}

// ============================================================================
// Explorer Route Handler
// ============================================================================

/**
 * Create explorer route handlers
 *
 * Returns a function that can handle explorer-related routes:
 * - GET /explore - Landing page with search
 * - GET /entity/{encoded_id} - View entity details
 * - GET /search?q=term - Search results
 * - GET /random - Redirect to random entity
 *
 * @param options Explorer configuration
 * @returns Route handler function
 *
 * @example
 * ```typescript
 * const explorer = createExplorerRoutes({
 *   namespace: 'imdb',
 *   displayName: 'IMDB Graph Explorer',
 *   baseUrl: 'https://imdb-graph.workers.do',
 *   getEntity: async (id) => fetchEntityFromR2(id),
 *   searchEntities: async (q) => searchIndex(q),
 *   getRandomEntityId: async () => pickRandomFromIndex(),
 * });
 *
 * // In worker fetch handler:
 * const result = await explorer(request, url);
 * if (result.handled) {
 *   return result.response;
 * }
 * ```
 */
export function createExplorerRoutes(options: ExplorerOptions) {
  const {
    displayName,
    baseUrl,
    getEntity,
    searchEntities,
    getRandomEntityId,
    getEntityCount,
  } = options;

  return async function handleExplorerRoute(
    request: Request,
    url: URL
  ): Promise<RouteResult> {
    const path = url.pathname;

    // GET /explore - Landing page
    if (path === '/explore' && request.method === 'GET') {
      if (wantsHtml(request)) {
        const count = getEntityCount ? await getEntityCount() : undefined;
        const html = renderExplorePage(displayName, baseUrl, count);
        return {
          handled: true,
          response: new Response(html, {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          }),
        };
      }
      // JSON response for API clients
      return {
        handled: true,
        response: new Response(
          JSON.stringify({
            name: displayName,
            endpoints: {
              '/explore': 'Interactive exploration landing page',
              '/entity/{id}': 'View entity by ID (URL-encoded)',
              '/search?q=term': 'Search entities',
              '/random': 'Redirect to a random entity',
            },
          }),
          { headers: { 'Content-Type': 'application/json' } }
        ),
      };
    }

    // GET /entity/{encoded_id} - View entity
    if (path.startsWith('/entity/') && request.method === 'GET') {
      const encodedId = path.slice('/entity/'.length);
      if (!encodedId) {
        if (wantsHtml(request)) {
          const html = renderErrorPage(
            'Invalid Entity ID',
            'Please provide an entity ID in the URL.',
            displayName,
            baseUrl
          );
          return {
            handled: true,
            response: new Response(html, {
              status: 400,
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            }),
          };
        }
        return {
          handled: true,
          response: new Response(
            JSON.stringify({ error: 'Missing entity ID' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
          ),
        };
      }

      const entityId = decodeURIComponent(encodedId);
      const entity = await getEntity(entityId);

      if (!entity) {
        if (wantsHtml(request)) {
          const html = renderErrorPage(
            'Entity Not Found',
            `No entity found with ID: ${entityId}`,
            displayName,
            baseUrl
          );
          return {
            handled: true,
            response: new Response(html, {
              status: 404,
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            }),
          };
        }
        return {
          handled: true,
          response: new Response(
            JSON.stringify({ error: 'Entity not found', id: entityId }),
            { status: 404, headers: { 'Content-Type': 'application/json' } }
          ),
        };
      }

      if (wantsHtml(request)) {
        const html = renderEntityHtml(entity, baseUrl, displayName);
        return {
          handled: true,
          response: new Response(html, {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          }),
        };
      }

      return {
        handled: true,
        response: new Response(JSON.stringify(entity, null, 2), {
          headers: { 'Content-Type': 'application/json' },
        }),
      };
    }

    // GET /search?q=term - Search entities
    if (path === '/search' && request.method === 'GET') {
      const query = url.searchParams.get('q') || '';
      const limit = parseInt(url.searchParams.get('limit') || '50', 10);

      if (!query.trim()) {
        if (wantsHtml(request)) {
          // Redirect to explore page if no query
          return {
            handled: true,
            response: Response.redirect(`${baseUrl}/explore`, 302),
          };
        }
        return {
          handled: true,
          response: new Response(
            JSON.stringify({ error: 'Missing search query parameter "q"' }),
            { status: 400, headers: { 'Content-Type': 'application/json' } }
          ),
        };
      }

      const results = await searchEntities(query.trim(), limit);

      if (wantsHtml(request)) {
        const html = renderSearchResults(query, results, displayName, baseUrl);
        return {
          handled: true,
          response: new Response(html, {
            headers: { 'Content-Type': 'text/html; charset=utf-8' },
          }),
        };
      }

      return {
        handled: true,
        response: new Response(
          JSON.stringify({ query, results, count: results.length }),
          { headers: { 'Content-Type': 'application/json' } }
        ),
      };
    }

    // GET /random - Redirect to random entity
    if (path === '/random' && request.method === 'GET') {
      const randomId = await getRandomEntityId();

      if (!randomId) {
        if (wantsHtml(request)) {
          const html = renderErrorPage(
            'No Entities Available',
            'The graph appears to be empty. Try loading some data first.',
            displayName,
            baseUrl
          );
          return {
            handled: true,
            response: new Response(html, {
              status: 404,
              headers: { 'Content-Type': 'text/html; charset=utf-8' },
            }),
          };
        }
        return {
          handled: true,
          response: new Response(
            JSON.stringify({ error: 'No entities available' }),
            { status: 404, headers: { 'Content-Type': 'application/json' } }
          ),
        };
      }

      const redirectUrl = `${baseUrl}/entity/${encodeURIComponent(randomId)}`;

      if (wantsHtml(request)) {
        return {
          handled: true,
          response: Response.redirect(redirectUrl, 302),
        };
      }

      // For API clients, return the entity ID
      return {
        handled: true,
        response: new Response(
          JSON.stringify({ id: randomId, url: redirectUrl }),
          { headers: { 'Content-Type': 'application/json' } }
        ),
      };
    }

    // Route not handled by explorer
    return { handled: false };
  };
}

/**
 * Render an entity to JSON with proper formatting
 */
export function renderEntityJson(entity: Entity): string {
  return JSON.stringify(entity, null, 2);
}

/**
 * Render an entity to HTML with clickable links
 */
export function renderEntity(entity: Entity, baseUrl: string, displayName: string): string {
  return renderEntityHtml(entity, baseUrl, displayName);
}
