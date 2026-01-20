/**
 * Test utilities for index tests
 *
 * These functions are internal to the combined-index module but need to be
 * accessible for tests to suppress warnings during test runs.
 */

/**
 * Flag to suppress experimental vector index warnings (for testing)
 * This is a test-only utility and should not be used in production code.
 */
let _suppressVectorWarnings = false;

/**
 * Suppress experimental vector index warnings (useful for tests)
 */
export function suppressVectorIndexWarnings(suppress: boolean = true): void {
  _suppressVectorWarnings = suppress;
}

/**
 * Check if vector index warnings are currently suppressed
 */
export function areVectorWarningsSuppressed(): boolean {
  return _suppressVectorWarnings;
}
