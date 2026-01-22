/**
 * Vitest Configuration for Node.js environment tests
 *
 * This configuration runs tests that don't need the Cloudflare Workers runtime
 * in a standard Node.js environment, which is faster and avoids the workerd
 * module resolution issues.
 *
 * Usage:
 *   npx vitest run --config vitest.node.config.ts test/protocol/client.test.ts
 */

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    testTimeout: 30000,
    hookTimeout: 10000,
    reporters: ['verbose'],
  },
});
