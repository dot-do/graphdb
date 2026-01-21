# Schema Migration Guide

This document describes how to use the schema migration CLI for GraphDB production deployments.

## Overview

GraphDB uses a versioned schema migration system to manage database schema changes. Migrations are applied automatically at runtime by the Durable Object, but this CLI tool provides:

- **Visibility**: View current schema version and pending migrations
- **Planning**: Preview SQL that will be executed before deployment
- **Custom migrations**: Add application-specific schema changes
- **Rollback preparation**: Generate rollback SQL for emergency situations

## Quick Start

```bash
# Check current migration status
npm run migrate:status

# Preview pending migrations
npm run migrate:up

# Preview rollback SQL
npm run migrate:down

# Create a new migration
npm run migrate:create -- --name add_user_preferences
```

## Commands

### `migrate:status`

Shows the current schema version and lists all available migrations with their applied/pending status.

```bash
npm run migrate:status

# Example output:
# === GraphDB Schema Migration Status ===
#
# Current Version: 4
# Latest Version:  5
# Target Version:  4 (code)
#
# --- Migrations ---
#
# Version    | Description                              | Status
# ---------------------------------------------------------------------------
# 1          | Create schema_meta                       | Applied
# 2          | Create chunks                            | Applied
# 3          | Drop triples                             | Applied
# 4          | Create triples                           | Applied
# 5          | Create app_metadata                      | Pending
#
# 1 migration(s) pending. Run 'npm run migrate:up' to apply.
```

### `migrate:up`

Shows the SQL that will be executed to apply pending migrations. The actual migration is performed automatically by the DO at runtime.

```bash
# Preview all pending migrations
npm run migrate:up

# Preview migrations up to a specific version
npm run migrate:up -- --version 5
```

**Options:**
- `--version N`: Migrate up to version N (default: latest)
- `--current N`: Simulate current version as N (for testing)

### `migrate:down`

Shows the SQL needed to rollback migrations. Use this for emergency rollback planning.

```bash
# Preview rollback of last migration
npm run migrate:down

# Preview rollback to a specific version
npm run migrate:down -- --version 3
```

**Options:**
- `--version N`: Rollback to version N (default: current - 1)
- `--current N`: Simulate current version as N (for testing)

**WARNING**: Rollbacks may cause data loss. Always backup before rolling back.

### `migrate:create`

Creates a new migration file in the `migrations/` directory.

```bash
npm run migrate:create -- --name add_analytics_table
```

This creates a file like `migrations/0005_add_analytics_table.ts` with a template.

## Migration File Format

Migration files in the `migrations/` directory follow this format:

```typescript
/**
 * Migration: add_analytics_table
 * Version: 5
 */

export const up = `
-- SQL to apply this migration
CREATE TABLE IF NOT EXISTS analytics (
  id TEXT PRIMARY KEY,
  event_type TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_analytics_type ON analytics(event_type);
`;

export const down = `
-- SQL to reverse this migration
DROP INDEX IF EXISTS idx_analytics_type;
DROP TABLE IF EXISTS analytics;
`;

export const meta = {
  version: 5,
  name: 'add_analytics_table',
  createdAt: '2026-01-20T00:00:00.000Z',
};
```

### Naming Convention

Migration files must follow this naming convention:
```
{version}_{name}.ts
```

Where:
- `{version}` is a 4-digit zero-padded number (e.g., `0005`)
- `{name}` is a descriptive snake_case name

Examples:
- `0005_add_analytics_table.ts`
- `0006_add_user_preferences.ts`
- `0007_create_audit_log.ts`

## Core vs Custom Migrations

### Core Migrations (src/shard/schema.ts)

Core migrations are defined in `src/shard/schema.ts` and are part of the GraphDB package. These handle the fundamental schema:

- Version 1: `schema_meta` table (version tracking)
- Version 2: `chunks` table (BLOB storage)
- Version 3: Remove legacy `triples` table
- Version 4: Add `triples` + index tables (SPO, POS, FTS, Geo, Vector)

### Custom Migrations (migrations/)

Custom migrations in the `migrations/` directory are for application-specific schema additions. They:

- Start at version 5+ (after core migrations)
- Are loaded automatically by the CLI
- Override core migrations if same version (not recommended)

## How Migrations Work at Runtime

1. **DO Initialization**: When a Shard DO starts, `initializeSchema()` is called
2. **Version Check**: Current version is read from `schema_meta` table
3. **Migration Loop**: All migrations between current and target version are applied
4. **Version Update**: `schema_meta` is updated after each migration

```typescript
// In your DO code (automatic)
import { initializeSchema } from '@dotdo/graphdb/shard';

export class ShardDO implements DurableObject {
  constructor(ctx: DurableObjectState, env: Env) {
    initializeSchema(ctx.storage.sql);
  }
}
```

## Production Deployment Workflow

### Pre-deployment

1. **Check status**: `npm run migrate:status`
2. **Review migrations**: `npm run migrate:up`
3. **Backup data**: Export critical data before major migrations
4. **Test in staging**: Deploy to staging environment first

### Deployment

1. **Deploy code**: `npm run deploy` (includes new migrations)
2. **Migrations apply automatically** when DOs wake up
3. **Monitor**: Check logs for migration success/failure

### Post-deployment

1. **Verify**: Run queries to confirm schema changes
2. **Monitor performance**: Watch for any slowdowns from new indexes

### Emergency Rollback

If a migration causes issues:

1. **Generate rollback SQL**: `npm run migrate:down -- --version N`
2. **Manual execution**: Execute SQL in affected DOs
3. **Deploy previous version**: Revert code to previous version
4. **Post-mortem**: Analyze what went wrong

## Best Practices

### DO

- Keep migrations small and focused
- Include both `up` and `down` SQL
- Test migrations in development first
- Use `IF NOT EXISTS` and `IF EXISTS` guards
- Add indexes in the same migration as tables

### DON'T

- Combine unrelated changes in one migration
- Skip version numbers
- Modify existing migrations after deployment
- Drop tables without backup plans
- Use transactions (SQLite in DO has implicit transactions)

## Troubleshooting

### Migration stuck

If a migration fails partway through:

1. Check DO logs for the error
2. Manually fix the schema issue
3. Update `schema_meta` version manually

```sql
-- Check current version
SELECT * FROM schema_meta WHERE key = 'schema_version';

-- Manually set version (after fixing issues)
INSERT OR REPLACE INTO schema_meta (key, value)
VALUES ('schema_version', '4');
```

### Version mismatch

If the CLI shows a different version than the DO:

1. The CLI uses `SCHEMA_VERSION` from code
2. The DO reads from `schema_meta` table
3. They should match after successful migration

### Custom migration not found

Ensure your migration file:

1. Is in the `migrations/` directory
2. Follows the `{version}_{name}.ts` naming convention
3. Exports `up`, `down`, and `meta`
4. Has a version number greater than core migrations (>= 5)

## API Reference

### Schema Exports

```typescript
import {
  SCHEMA_VERSION,      // Current target version
  MIGRATIONS,          // Array of Migration objects
  initializeSchema,    // Initialize/migrate schema
  getCurrentVersion,   // Read current version from DB
  migrateToVersion,    // Migrate to specific version
  runMigration,        // Run single migration
} from '@dotdo/graphdb/shard';
```

### Migration Type

```typescript
interface Migration {
  version: number;     // Migration version number
  up: string;          // SQL for upgrade
  down: string;        // SQL for downgrade
}
```

## Related Documentation

- [ARCHITECTURE.md](../ARCHITECTURE.md) - System architecture overview
- [CLAUDE.md](../CLAUDE.md) - Development context and patterns
- [CHANGELOG.md](../CHANGELOG.md) - Version history
