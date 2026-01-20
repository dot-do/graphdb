/**
 * HNSW Distance Functions
 *
 * Pure TypeScript implementation of common distance metrics used for
 * vector similarity search in HNSW indexes.
 *
 * @packageDocumentation
 */

/**
 * Distance function type - all distance functions follow this signature.
 * Lower values indicate more similar vectors.
 */
export type DistanceFunction = (a: number[], b: number[]) => number;

/**
 * Calculate cosine distance between two vectors.
 *
 * Cosine distance = 1 - cosine_similarity
 * Range: [0, 2] where 0 = identical direction, 2 = opposite direction
 *
 * This metric is ideal for normalized embeddings (e.g., text embeddings)
 * as it measures angular similarity independent of magnitude.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Cosine distance (0 = identical, 2 = opposite)
 * @throws Error if vectors have different dimensions
 */
export function cosineDistance(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i]! * b[i]!;
    normA += a[i]! * a[i]!;
    normB += b[i]! * b[i]!;
  }

  // Handle zero vectors - return max distance
  if (normA === 0 || normB === 0) {
    return 2;
  }

  const similarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
  return 1 - similarity;
}

/**
 * Calculate Euclidean (L2) distance between two vectors.
 *
 * Range: [0, +infinity) where 0 = identical vectors
 *
 * This metric measures the straight-line distance in Euclidean space.
 * Ideal for spatial data or when magnitude matters.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Euclidean distance (>= 0)
 * @throws Error if vectors have different dimensions
 */
export function euclideanDistance(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let sum = 0;
  for (let i = 0; i < a.length; i++) {
    const diff = a[i]! - b[i]!;
    sum += diff * diff;
  }

  return Math.sqrt(sum);
}

/**
 * Calculate inner product distance (for Maximum Inner Product Search).
 *
 * Converts inner product to a distance metric: distance = 1 - dot_product
 * For normalized vectors, this equals cosine distance.
 *
 * Note: Unlike cosine distance, this metric is sensitive to vector magnitude.
 * For un-normalized vectors, larger magnitudes result in smaller distances.
 *
 * @param a - First vector
 * @param b - Second vector
 * @returns Inner product distance (1 - dot_product)
 * @throws Error if vectors have different dimensions
 */
export function innerProduct(a: number[], b: number[]): number {
  if (a.length !== b.length) {
    throw new Error(`Vector dimension mismatch: ${a.length} vs ${b.length}`);
  }

  let dotProduct = 0;
  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i]! * b[i]!;
  }

  // Convert to distance: higher inner product = lower distance
  return 1 - dotProduct;
}

/**
 * Calculate cosine similarity between two vectors.
 *
 * Cosine similarity measures the cosine of the angle between two vectors.
 * Range: [-1, 1] where 1 = identical direction, -1 = opposite direction, 0 = orthogonal
 *
 * Note: For normalized vectors, this is equivalent to the dot product.
 * Related: cosineDistance = 1 - cosineSimilarity
 *
 * @param a - First vector (Float32Array or number array)
 * @param b - Second vector (Float32Array or number array)
 * @returns Cosine similarity (1 = identical, -1 = opposite, 0 = orthogonal)
 */
export function cosineSimilarity(a: Float32Array | number[], b: Float32Array | number[]): number {
  let dotProduct = 0;
  let normA = 0;
  let normB = 0;

  for (let i = 0; i < a.length; i++) {
    dotProduct += a[i]! * b[i]!;
    normA += a[i]! * a[i]!;
    normB += b[i]! * b[i]!;
  }

  if (normA === 0 || normB === 0) return 0;
  return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
}
