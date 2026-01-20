/**
 * GraphDB Result Materializer (E7.3: GREEN)
 *
 * Materializes query results from triples to entities.
 * Handles grouping, reference expansion, field projection, and formatting.
 *
 * Following TDD approach: tests written first, then implementation to GREEN.
 */

import type { Triple } from '../core/triple';
import type { Entity } from '../core/entity';
import { createEntity } from '../core/entity';
import { createEntityId } from '../core/types';
import { extractJsonValue } from '../core/type-converters';
import type { ExecutionResult } from './executor';

// ============================================================================
// Constants
// ============================================================================

/**
 * Maximum depth for reference expansion to prevent infinite loops.
 * When following REF links, expansion will stop at this depth.
 */
export const MAX_EXPANSION_DEPTH = 10;

// ============================================================================
// Types
// ============================================================================

/**
 * Options for materializing triples to entities
 */
export interface MaterializeOptions {
  /** Expand REF values to nested entities */
  expandRefs?: boolean;
  /** Maximum depth for reference expansion */
  maxDepth?: number;
  /** Include _namespace and _localId metadata fields */
  includeMetadata?: boolean;
  /** Only include specific fields in output */
  fields?: string[];
}

/**
 * Options for reference expansion
 */
export interface ExpandRefsOptions {
  /** Maximum depth for reference expansion (default: 1) */
  maxDepth?: number;
  /** Include expansion metadata in result (_expansionMeta) */
  includeMetadata?: boolean;
}

/**
 * Expansion metadata included when includeMetadata is true
 */
export interface ExpansionMeta {
  /** Whether the maximum depth limit was reached during expansion */
  maxDepthReached: boolean;
  /** The actual depth reached during expansion */
  actualDepth: number;
}

/**
 * Entity with optional expansion metadata
 */
export type ExpansionResult = Entity & {
  _expansionMeta?: ExpansionMeta;
};

/**
 * Formatted result for API response
 */
export interface FormattedResult {
  /** Materialized entity data */
  data: Entity[];
  /** Pagination information */
  pagination?: {
    cursor: string;
    hasMore: boolean;
  };
  /** Execution metadata */
  meta?: {
    duration: number;
    shardQueries: number;
  };
}

/**
 * Entity resolver function for expanding references
 */
export type EntityResolver = (id: string) => Promise<Entity | null>;

// ============================================================================
// Main Materialization Functions
// ============================================================================

/**
 * Materialize triples to entities
 *
 * Groups triples by subject and converts them to Entity objects.
 * Each entity includes all predicates from its associated triples.
 *
 * @param triples - Array of triples to materialize
 * @returns Array of materialized entities
 */
export function materializeTriples(triples: Triple[]): Entity[] {
  if (triples.length === 0) {
    return [];
  }

  const grouped = groupBySubject(triples);
  const entities: Entity[] = [];

  for (const [subjectId, subjectTriples] of grouped) {
    // Build properties from triples
    const props: Record<string, unknown> = {};
    let entityType: string | string[] = 'Thing'; // Default type

    for (const triple of subjectTriples) {
      const predicate = triple.predicate;
      const value = toJsonValue(triple);

      // Handle $type specially
      if (predicate === '$type') {
        entityType = value as string;
        continue;
      }

      // Handle multiple values for same predicate
      if (predicate in props) {
        const existing = props[predicate];
        if (Array.isArray(existing)) {
          existing.push(value);
        } else {
          props[predicate] = [existing, value];
        }
      } else {
        props[predicate] = value;
      }
    }

    // Create entity with the properties
    const entity = createEntity(
      createEntityId(subjectId),
      entityType,
      props
    );

    entities.push(entity);
  }

  return entities;
}

/**
 * Group triples by subject
 *
 * Creates a Map from subject ID to its associated triples.
 * Used internally for materialization.
 *
 * @param triples - Array of triples to group
 * @returns Map from subject ID to triples
 */
export function groupBySubject(triples: Triple[]): Map<string, Triple[]> {
  const groups = new Map<string, Triple[]>();

  for (const triple of triples) {
    const subjectId = triple.subject as string;
    const existing = groups.get(subjectId);
    if (existing !== undefined) {
      existing.push(triple);
    } else {
      groups.set(subjectId, [triple]);
    }
  }

  return groups;
}

/**
 * Internal state for tracking expansion depth
 */
interface ExpansionState {
  maxDepthReached: boolean;
  actualDepth: number;
}

/**
 * Expand references to nested entities
 *
 * Recursively resolves REF type values to full nested entities.
 * Respects maxDepth to prevent infinite loops.
 *
 * @param entity - Entity with references to expand
 * @param resolver - Function to resolve entity by ID
 * @param options - Expansion options (maxDepth, includeMetadata)
 * @returns Entity with expanded references (and optional metadata)
 */
export async function expandRefs(
  entity: Entity,
  resolver: EntityResolver,
  options?: ExpandRefsOptions
): Promise<Entity | ExpansionResult> {
  const maxDepth = options?.maxDepth ?? 1;
  const includeMetadata = options?.includeMetadata ?? false;

  // Track expansion state
  const state: ExpansionState = {
    maxDepthReached: false,
    actualDepth: 0,
  };

  const result = await expandRefsRecursive(entity, resolver, maxDepth, 0, state);

  // Add metadata if requested
  if (includeMetadata) {
    (result as ExpansionResult)._expansionMeta = {
      maxDepthReached: state.maxDepthReached,
      actualDepth: state.actualDepth,
    };
  }

  return result;
}

/**
 * Internal recursive reference expansion
 */
async function expandRefsRecursive(
  entity: Entity,
  resolver: EntityResolver,
  maxDepth: number,
  currentDepth: number,
  state: ExpansionState
): Promise<Entity> {
  // Create a shallow copy of the entity
  const result: Entity = { ...entity };

  // Iterate over all properties
  for (const [key, value] of Object.entries(entity)) {
    // Skip system fields
    if (key.startsWith('$') || key.startsWith('_')) {
      continue;
    }

    // Handle arrays
    if (Array.isArray(value)) {
      const expandedArray: unknown[] = [];
      for (const item of value) {
        if (isRefValue(item)) {
          if (currentDepth < maxDepth) {
            const resolved = await resolver(item['@ref']);
            if (resolved) {
              // Update state to track depth
              state.actualDepth = Math.max(state.actualDepth, currentDepth + 1);

              // Recursively expand the resolved entity
              const expanded = await expandRefsRecursive(
                resolved,
                resolver,
                maxDepth,
                currentDepth + 1,
                state
              );
              expandedArray.push(expanded);
            } else {
              expandedArray.push(item);
            }
          } else {
            // Depth limit reached
            state.maxDepthReached = true;
            expandedArray.push(item);
          }
        } else {
          expandedArray.push(item);
        }
      }
      (result as Record<string, unknown>)[key] = expandedArray;
    } else if (isRefValue(value)) {
      if (currentDepth < maxDepth) {
        // Expand single ref
        const resolved = await resolver(value['@ref']);
        if (resolved) {
          // Update state to track depth
          state.actualDepth = Math.max(state.actualDepth, currentDepth + 1);

          // Recursively expand the resolved entity
          const expanded = await expandRefsRecursive(
            resolved,
            resolver,
            maxDepth,
            currentDepth + 1,
            state
          );
          (result as Record<string, unknown>)[key] = expanded;
        }
        // If null, keep original ref
      } else {
        // Depth limit reached
        state.maxDepthReached = true;
      }
    }
  }

  return result;
}

/**
 * Project specific fields from an entity
 *
 * Returns a partial entity containing only the specified fields.
 * Always includes $id, $type, and $context.
 *
 * @param entity - Entity to project
 * @param fields - Fields to include
 * @returns Partial entity with only specified fields
 */
export function projectFields(entity: Entity, fields: string[]): Partial<Entity> {
  // Always include $id, $type, $context
  const result: Partial<Entity> = {
    $id: entity.$id,
    $type: entity.$type,
    $context: entity.$context,
  };

  // Include requested fields
  for (const field of fields) {
    if (field in entity) {
      (result as Record<string, unknown>)[field] = (entity as Record<string, unknown>)[field];
    }
  }

  return result;
}

/**
 * Format execution result for API response
 *
 * Converts ExecutionResult to a formatted response suitable for API clients.
 * Optionally includes pagination and metadata.
 *
 * @param result - Execution result to format
 * @param options - Materialization options
 * @returns Formatted result with data, pagination, and meta
 */
export function formatResult(
  result: ExecutionResult,
  options?: MaterializeOptions
): FormattedResult {
  // Determine source of data: entities or triples
  let data: Entity[];
  if (result.entities.length > 0) {
    data = result.entities;
  } else if (result.triples.length > 0) {
    data = materializeTriples(result.triples);
  } else {
    data = [];
  }

  // Apply field projection if specified
  if (options?.fields && options.fields.length > 0) {
    data = data.map((entity) => projectFields(entity, options.fields!) as Entity);
  }

  // Optionally strip metadata fields
  if (options?.includeMetadata === false) {
    data = data.map((entity) => {
      const copy = { ...entity };
      delete (copy as Record<string, unknown>)['_namespace'];
      delete (copy as Record<string, unknown>)['_localId'];
      return copy;
    });
  }

  // Build response
  const formatted: FormattedResult = {
    data,
  };

  // Include pagination if there are more results
  if (result.hasMore && result.cursor) {
    formatted.pagination = {
      cursor: result.cursor,
      hasMore: result.hasMore,
    };
  }

  // Include metadata
  formatted.meta = {
    duration: result.stats.durationMs,
    shardQueries: result.stats.shardQueries,
  };

  return formatted;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Convert typed object value to JSON-serializable format
 * Uses consolidated type-converters module with wrapRefs option for REF expansion
 */
function toJsonValue(triple: Triple): unknown {
  return extractJsonValue(triple.object, { wrapRefs: true });
}

/**
 * Check if a value is a reference type that can be expanded
 */
function isRefValue(value: unknown): value is { '@ref': string } {
  return (
    typeof value === 'object' &&
    value !== null &&
    '@ref' in value &&
    typeof (value as Record<string, unknown>)['@ref'] === 'string'
  );
}
