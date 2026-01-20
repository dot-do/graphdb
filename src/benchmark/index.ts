/**
 * Benchmark Module Index
 *
 * Re-exports all benchmark functionality for GraphDB.
 */

export { handleBenchmarkRequest } from './benchmark-worker.js';
export { handleBenchEndpoint, handleBenchCors } from './bench-handler.js';
export {
  DATASETS,
  getDatasetGenerator,
  estimateTripleCount,
  generateULID,
  generateONETDataset,
  generateIMDBDataset,
  randomEntityId,
  type DatasetConfig,
  type GeneratorProgress,
  type ProgressCallback,
} from './datasets.js';
export {
  SCENARIOS,
  listScenarios,
  getScenarioRunner,
  runPointLookup,
  runTraversal1Hop,
  runTraversal3Hop,
  runWriteThroughput,
  runBloomFilterHitRate,
  runEdgeCacheHitRate,
  type LatencyStats,
  type ThroughputStats,
  type CacheStats,
  type BenchmarkResult,
  type ScenarioContext,
  type ScenarioRunner,
} from './scenarios.js';
export {
  InMemoryTripleStore,
  inferObjectType,
  rowToTriples,
  executeBenchQuery,
  generateTestData,
  BENCH_QUERIES,
  type BenchQuery,
} from './in-memory-store.js';
