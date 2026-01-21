/**
 * Hybrid Search Module
 *
 * Combines multiple index types for comprehensive search:
 * - FTS + Vector (semantic + keyword)
 * - FTS + Geo (text + location)
 * - Vector + Geo (semantic + location)
 * - All three combined
 *
 * @packageDocumentation
 */

// ============================================================================
// TYPES
// ============================================================================

/**
 * A single search result from any index type
 */
export interface SearchResult {
  /** The entity ID that matched */
  entityId: string;
  /** The score (higher is better, normalized to 0-1 where possible) */
  score: number;
  /** Which index produced this result */
  source: 'fts' | 'vector' | 'geo';
}

/**
 * An index searcher that can be combined with others
 */
export interface IndexSearcher {
  /** Execute a search and return results */
  search: () => Promise<SearchResult[]>;
  /** Name of this searcher (for debugging) */
  name: string;
}

/**
 * A weighted searcher for hybrid search
 */
export interface WeightedSearcher {
  searcher: IndexSearcher;
  weight: number;
}

/**
 * Options for hybrid search
 */
export interface HybridSearchOptions {
  /** The searchers to combine with their weights */
  searchers: WeightedSearcher[];
  /** Maximum number of results to return */
  limit: number;
  /** Fusion method to use */
  fusionMethod?: 'weighted_average' | 'rrf';
}

// ============================================================================
// SCORE NORMALIZATION
// ============================================================================

/**
 * Normalize a score from various sources to [0, 1] range
 *
 * @param score - The raw score
 * @param type - The type of score ('bm25', 'cosine', 'geo_distance')
 * @returns Normalized score in [0, 1]
 */
export function normalizeScore(score: number, type: 'bm25' | 'cosine' | 'geo_distance'): number {
  switch (type) {
    case 'bm25': {
      // BM25 scores are typically unbounded positive values
      // Use sigmoid-like normalization: score / (1 + score)
      // This maps [0, inf) to [0, 1)
      if (score <= 0) return 0;
      return score / (1 + score);
    }
    case 'cosine': {
      // Cosine similarity is already in [-1, 1] or [0, 1]
      // Clamp to [0, 1] range
      return Math.max(0, Math.min(1, score));
    }
    case 'geo_distance': {
      // Distance in km - lower is better
      // 0 distance = max score (1.0)
      // Use exponential decay: e^(-distance)
      // This gives 1.0 at distance 0, decaying toward 0
      if (score <= 0) return 1;
      return Math.exp(-score);
    }
  }
}

// ============================================================================
// SCORE COMBINATION
// ============================================================================

/**
 * Combine scores from multiple sources using weights
 *
 * @param scores - Object mapping source to score
 * @param weights - Object mapping source to weight
 * @returns Combined score
 */
export function combineScores(
  scores: Record<string, number>,
  weights: Record<string, number>
): number {
  // Calculate weighted sum, only considering sources that have scores
  let weightedSum = 0;
  let totalWeight = 0;

  for (const source of Object.keys(scores)) {
    const score = scores[source];
    const weight = weights[source] ?? 0;

    if (weight > 0 && score !== undefined) {
      weightedSum += score * weight;
      totalWeight += weight;
    }
  }

  // Return normalized weighted average
  if (totalWeight === 0) return 0;
  return weightedSum / totalWeight;
}

// ============================================================================
// RECIPROCAL RANK FUSION
// ============================================================================

/**
 * Combine multiple ranked lists using Reciprocal Rank Fusion
 *
 * RRF score for a document d appearing at rank r in list l:
 * RRF(d) = sum over all lists l of: 1 / (k + r_l(d))
 *
 * @param resultLists - Array of result lists (each sorted by relevance)
 * @param k - Constant to control how much to weight lower-ranked items (default 60)
 * @returns Combined results sorted by RRF score
 */
export function reciprocalRankFusion(
  resultLists: SearchResult[][],
  k: number = 60
): SearchResult[] {
  // Map to accumulate RRF scores for each entity
  const rrfScores = new Map<string, number>();
  // Track the source from one of the results (for the output)
  const entitySources = new Map<string, 'fts' | 'vector' | 'geo'>();

  // Process each result list
  for (const results of resultLists) {
    // Each result's rank is its position in the list (1-indexed)
    for (let rank = 0; rank < results.length; rank++) {
      const result = results[rank];
      if (!result) continue;

      const { entityId, source } = result;
      // RRF formula: 1 / (k + rank), where rank is 1-indexed
      const rrfContribution = 1 / (k + rank + 1);

      const currentScore = rrfScores.get(entityId) ?? 0;
      rrfScores.set(entityId, currentScore + rrfContribution);

      // Keep track of source (use the first one we encounter)
      if (!entitySources.has(entityId)) {
        entitySources.set(entityId, source);
      }
    }
  }

  // Convert to array of SearchResult and sort by RRF score (descending)
  const combined: SearchResult[] = [];
  for (const [entityId, score] of rrfScores) {
    combined.push({
      entityId,
      score,
      source: entitySources.get(entityId) ?? 'fts',
    });
  }

  // Sort by score descending
  combined.sort((a, b) => b.score - a.score);

  return combined;
}

// ============================================================================
// HYBRID SEARCH
// ============================================================================

/**
 * Execute a hybrid search combining multiple index types
 *
 * @param options - Search options including searchers and weights
 * @returns Combined search results
 */
export async function hybridSearch(options: HybridSearchOptions): Promise<SearchResult[]> {
  const { searchers, limit, fusionMethod = 'weighted_average' } = options;

  // Execute all searchers in parallel
  const searchPromises = searchers.map(async ({ searcher, weight }) => {
    const results = await searcher.search();
    return { results, weight };
  });

  const searchResults = await Promise.all(searchPromises);

  // Use RRF or weighted average based on fusion method
  if (fusionMethod === 'rrf') {
    // For RRF, we use the result lists directly
    const resultLists = searchResults.map(sr => sr.results);
    const combined = reciprocalRankFusion(resultLists);
    return combined.slice(0, limit);
  }

  // Weighted average fusion
  // First, collect all unique entity IDs and their scores from each source
  const entityScores = new Map<string, Map<string, { score: number; weight: number }>>();

  for (const { results, weight } of searchResults) {
    // Deduplicate within a single searcher's results (keep highest score)
    const deduped = new Map<string, SearchResult>();
    for (const result of results) {
      const existing = deduped.get(result.entityId);
      if (!existing || result.score > existing.score) {
        deduped.set(result.entityId, result);
      }
    }

    for (const result of deduped.values()) {
      const { entityId, score, source } = result;

      // Normalize score based on source type
      let normalizedScore: number;
      if (source === 'fts') {
        normalizedScore = normalizeScore(score, 'bm25');
      } else if (source === 'vector') {
        normalizedScore = normalizeScore(score, 'cosine');
      } else {
        // geo scores in mock are already similarity scores (0-1)
        normalizedScore = score;
      }

      if (!entityScores.has(entityId)) {
        entityScores.set(entityId, new Map());
      }

      const sourceScores = entityScores.get(entityId)!;
      sourceScores.set(source, { score: normalizedScore, weight });
    }
  }

  // Calculate the total weight across all searchers (for normalization)
  const totalWeightAcrossAllSearchers = searchResults.reduce((sum, sr) => sum + sr.weight, 0);

  // Calculate combined scores for each entity
  const combined: SearchResult[] = [];

  for (const [entityId, sourceScores] of entityScores) {
    let weightedSum = 0;
    let primarySource: 'fts' | 'vector' | 'geo' = 'fts';
    let maxWeightedScore = -1;

    for (const [source, { score, weight }] of sourceScores) {
      const weightedScore = score * weight;
      weightedSum += weightedScore;

      // Track source with highest weighted score for the final result
      if (weightedScore > maxWeightedScore) {
        maxWeightedScore = weightedScore;
        primarySource = source as 'fts' | 'vector' | 'geo';
      }
    }

    // Normalize by total weight across ALL searchers
    // This means entities appearing in fewer sources get lower scores
    const combinedScore = totalWeightAcrossAllSearchers > 0 ? weightedSum / totalWeightAcrossAllSearchers : 0;

    combined.push({
      entityId,
      score: combinedScore,
      source: primarySource,
    });
  }

  // Sort by combined score (descending) and apply limit
  combined.sort((a, b) => b.score - a.score);
  return combined.slice(0, limit);
}
