/**
 * Vitest Configuration for E2E Tests
 *
 * This configuration runs E2E tests in a standard Node.js environment
 * (not in the Cloudflare Workers pool) so they can make real network
 * requests to deployed Cloudflare Workers.
 *
 * Usage:
 *   npx vitest run --config vitest.e2e.config.ts
 *
 * Or with environment variables:
 *   CLOUDFLARE_API_TOKEN=xxx CLOUDFLARE_ACCOUNT_ID=yyy npx vitest run --config vitest.e2e.config.ts
 */

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    // Run only E2E tests
    include: ['test/e2e/cloudflare-integration.test.ts'],

    // Use standard Node.js environment (not workers pool)
    // This allows real WebSocket connections to Cloudflare
    environment: 'node',

    // Longer timeouts for network operations
    testTimeout: 120000, // 2 minutes per test
    hookTimeout: 60000, // 1 minute for setup/teardown

    // Run tests sequentially to avoid rate limiting
    sequence: {
      concurrent: false,
    },

    // Retry failed tests once (network can be flaky)
    retry: 1,

    // Reporter for better visibility
    reporters: ['verbose'],

    // Global setup/teardown if needed
    // globalSetup: './test/e2e/setup.ts',
    // globalTeardown: './test/e2e/teardown.ts',
  },
});
