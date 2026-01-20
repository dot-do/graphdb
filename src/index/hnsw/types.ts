/**
 * HNSW (Hierarchical Navigable Small World) Graph Types
 *
 * This module defines the core types for the HNSW graph construction algorithm.
 * HNSW is a graph-based approximate nearest neighbor search algorithm that
 * organizes vectors in a hierarchical structure of navigable small world graphs.
 */

/**
 * Represents a node in the HNSW graph.
 * Each node exists at multiple layers (from 0 to maxLayer) and maintains
 * connections to other nodes at each layer.
 */
export interface HNSWNode {
  /** Unique identifier for this node */
  id: string;

  /** The maximum layer this node exists in (0-indexed) */
  maxLayer: number;

  /**
   * Connections to other nodes, organized by layer.
   * Key: layer number (0 to maxLayer)
   * Value: Set of connected node IDs at that layer
   */
  connections: Map<number, Set<string>>;
}

/**
 * Configuration parameters for HNSW graph construction.
 * These parameters control the trade-off between search quality,
 * construction speed, and memory usage.
 */
export interface HNSWConfig {
  /**
   * Maximum number of connections per node at layers > 0.
   * Higher values improve search quality but increase memory usage.
   * Default: 16
   */
  M: number;

  /**
   * Maximum number of connections per node at layer 0.
   * Typically set to M * 2 for better search quality at the base layer.
   * Default: M * 2 (32)
   */
  M0: number;

  /**
   * Beam width during construction (size of dynamic candidate list).
   * Higher values improve construction quality but slow down insertion.
   * Default: 200
   */
  efConstruction: number;

  /**
   * Level multiplier for random level generation.
   * Controls the probability distribution of node layers.
   * Default: 1 / ln(M)
   */
  mL: number;
}

/**
 * A candidate node during search, with its distance from the query.
 */
export interface SearchCandidate {
  /** Node ID */
  id: string;

  /** Distance from the query vector */
  distance: number;
}

/**
 * Distance function type for comparing vectors.
 * Returns a non-negative distance value (lower = more similar).
 */
export type DistanceFunction = (a: string, b: string) => number;

/**
 * Default HNSW configuration values based on the original paper.
 */
export const DEFAULT_HNSW_CONFIG: HNSWConfig = {
  M: 16,
  M0: 32,
  efConstruction: 200,
  mL: 1 / Math.log(16), // 1 / ln(M)
};

/**
 * Creates an HNSWConfig with defaults for any unspecified values.
 */
export function createHNSWConfig(partial: Partial<HNSWConfig> = {}): HNSWConfig {
  const M = partial.M ?? DEFAULT_HNSW_CONFIG.M;
  return {
    M,
    M0: partial.M0 ?? M * 2,
    efConstruction: partial.efConstruction ?? DEFAULT_HNSW_CONFIG.efConstruction,
    mL: partial.mL ?? 1 / Math.log(M),
  };
}
