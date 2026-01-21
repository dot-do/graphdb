/**
 * Migration: example_migration
 * Version: 5
 * Created: 2026-01-20T00:00:00.000Z
 *
 * Description:
 *   Example migration file demonstrating the migration format.
 *   This adds an optional metadata table for storing application-level
 *   key-value pairs separate from the schema_meta table.
 *
 * NOTE: This is an example migration. Delete or modify as needed.
 */

/**
 * SQL to apply this migration (upgrade)
 */
export const up = `
-- Create application metadata table
CREATE TABLE IF NOT EXISTS app_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
);

-- Index for efficient key lookups
CREATE INDEX IF NOT EXISTS idx_app_metadata_updated ON app_metadata(updated_at);
`;

/**
 * SQL to reverse this migration (downgrade)
 */
export const down = `
-- Remove application metadata table
DROP INDEX IF EXISTS idx_app_metadata_updated;
DROP TABLE IF EXISTS app_metadata;
`;

/**
 * Migration metadata
 */
export const meta = {
  version: 5,
  name: 'example_migration',
  createdAt: '2026-01-20T00:00:00.000Z',
};
