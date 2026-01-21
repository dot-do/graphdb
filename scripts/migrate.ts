#!/usr/bin/env npx tsx
/**
 * Schema Migration CLI for GraphDB
 *
 * This CLI tool provides commands for managing database schema migrations
 * in production deployments. It integrates with the existing migration system
 * defined in src/shard/schema.ts.
 *
 * Commands:
 *   migrate:status - Show current schema version and pending migrations
 *   migrate:up     - Apply pending migrations (or specific version)
 *   migrate:down   - Rollback last migration (or to specific version)
 *   migrate:create - Create a new migration file
 *
 * Usage:
 *   npx tsx scripts/migrate.ts status
 *   npx tsx scripts/migrate.ts up
 *   npx tsx scripts/migrate.ts up --version 5
 *   npx tsx scripts/migrate.ts down
 *   npx tsx scripts/migrate.ts down --version 3
 *   npx tsx scripts/migrate.ts create --name add_new_index
 *
 * @module scripts/migrate
 */

import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

// Get directory paths
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const projectRoot = path.resolve(__dirname, '..');
const migrationsDir = path.join(projectRoot, 'migrations');

// Import schema definitions (these are the source of truth)
// Note: In production, these would be loaded from the deployed schema
import {
  SCHEMA_VERSION,
  MIGRATIONS,
  type Migration,
} from '../src/shard/schema.js';

/**
 * ANSI color codes for terminal output
 */
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
};

/**
 * Print colored output to console
 */
function log(message: string, color: keyof typeof colors = 'reset'): void {
  console.log(`${colors[color]}${message}${colors.reset}`);
}

/**
 * Print a table row
 */
function tableRow(cols: string[], widths: number[]): string {
  return cols.map((col, i) => col.padEnd(widths[i] || 20)).join(' | ');
}

/**
 * Load custom migrations from the migrations/ directory
 */
function loadCustomMigrations(): Migration[] {
  const customMigrations: Migration[] = [];

  if (!fs.existsSync(migrationsDir)) {
    return customMigrations;
  }

  const files = fs.readdirSync(migrationsDir)
    .filter((f) => f.endsWith('.ts') || f.endsWith('.js'))
    .sort();

  for (const file of files) {
    const match = file.match(/^(\d{4})[-_]/);
    if (match) {
      const version = parseInt(match[1], 10);
      const filePath = path.join(migrationsDir, file);
      const content = fs.readFileSync(filePath, 'utf-8');

      // Extract up and down SQL from the migration file
      const upMatch = content.match(/export\s+const\s+up\s*=\s*`([^`]+)`/s);
      const downMatch = content.match(/export\s+const\s+down\s*=\s*`([^`]+)`/s);

      if (upMatch && downMatch) {
        customMigrations.push({
          version,
          up: upMatch[1],
          down: downMatch[1],
        });
      }
    }
  }

  return customMigrations;
}

/**
 * Get all migrations (core + custom) sorted by version
 */
function getAllMigrations(): Migration[] {
  const customMigrations = loadCustomMigrations();
  const allMigrations = [...MIGRATIONS, ...customMigrations];

  // Sort by version and deduplicate (custom overrides core)
  const migrationMap = new Map<number, Migration>();
  for (const m of allMigrations) {
    migrationMap.set(m.version, m);
  }

  return Array.from(migrationMap.values()).sort((a, b) => a.version - b.version);
}

/**
 * Get the latest migration version
 */
function getLatestVersion(): number {
  const allMigrations = getAllMigrations();
  if (allMigrations.length === 0) return 0;
  return Math.max(...allMigrations.map((m) => m.version));
}

/**
 * Command: migrate:status
 *
 * Shows current schema version and lists all available migrations
 * with their status (applied/pending).
 */
function showStatus(currentVersion: number): void {
  const allMigrations = getAllMigrations();
  const latestVersion = getLatestVersion();

  log('\n=== GraphDB Schema Migration Status ===\n', 'bright');

  log(`Current Version: ${currentVersion}`, currentVersion === latestVersion ? 'green' : 'yellow');
  log(`Latest Version:  ${latestVersion}`, 'cyan');
  log(`Target Version:  ${SCHEMA_VERSION} (code)`, 'dim');

  log('\n--- Migrations ---\n', 'bright');

  const widths = [10, 40, 12];
  log(tableRow(['Version', 'Description', 'Status'], widths), 'dim');
  log('-'.repeat(70), 'dim');

  for (const migration of allMigrations) {
    const status = migration.version <= currentVersion ? 'Applied' : 'Pending';
    const statusColor = status === 'Applied' ? 'green' : 'yellow';

    // Extract a description from the up SQL (first CREATE or ALTER statement)
    const upSql = migration.up.trim();
    let description = 'Migration';
    const createMatch = upSql.match(/CREATE\s+(?:TABLE|INDEX|VIRTUAL TABLE)\s+(?:IF NOT EXISTS\s+)?(\w+)/i);
    const alterMatch = upSql.match(/ALTER\s+TABLE\s+(\w+)/i);
    const dropMatch = upSql.match(/DROP\s+(?:TABLE|INDEX)\s+(?:IF EXISTS\s+)?(\w+)/i);

    if (createMatch) {
      description = `Create ${createMatch[1]}`;
    } else if (alterMatch) {
      description = `Alter ${alterMatch[1]}`;
    } else if (dropMatch) {
      description = `Drop ${dropMatch[1]}`;
    }

    console.log(
      `${colors[statusColor]}${tableRow(
        [String(migration.version), description.substring(0, 38), status],
        widths
      )}${colors.reset}`
    );
  }

  log('');

  if (currentVersion < latestVersion) {
    const pending = allMigrations.filter((m) => m.version > currentVersion);
    log(`${pending.length} migration(s) pending. Run 'npm run migrate:up' to apply.`, 'yellow');
  } else {
    log('Database is up to date.', 'green');
  }

  log('');
}

/**
 * Command: migrate:up
 *
 * Applies pending migrations up to the specified version (or latest).
 * Outputs the SQL that would be executed.
 */
function migrateUp(currentVersion: number, targetVersion?: number): void {
  const allMigrations = getAllMigrations();
  const latestVersion = getLatestVersion();
  const target = targetVersion ?? latestVersion;

  if (target <= currentVersion) {
    log(`\nAlready at version ${currentVersion}. Nothing to migrate.`, 'green');
    return;
  }

  const pendingMigrations = allMigrations.filter(
    (m) => m.version > currentVersion && m.version <= target
  );

  if (pendingMigrations.length === 0) {
    log('\nNo pending migrations to apply.', 'green');
    return;
  }

  log('\n=== Applying Migrations ===\n', 'bright');
  log(`From version ${currentVersion} to ${target}`, 'cyan');
  log(`Migrations to apply: ${pendingMigrations.length}\n`, 'dim');

  for (const migration of pendingMigrations) {
    log(`--- Migration ${migration.version} ---`, 'yellow');
    log('\nSQL to execute:', 'dim');
    log(migration.up.trim(), 'cyan');
    log('');
  }

  log('=== Migration SQL Summary ===\n', 'bright');
  log('To apply these migrations, execute the SQL above in your DO SQLite instance.', 'dim');
  log('The schema.ts initializeSchema() function handles this automatically at runtime.\n', 'dim');

  // Output combined SQL for copy/paste
  log('Combined SQL:', 'bright');
  log('```sql');
  for (const migration of pendingMigrations) {
    log(`-- Migration version ${migration.version}`);
    log(migration.up.trim());
    log(`\nINSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', '${migration.version}');`);
    log('');
  }
  log('```\n');
}

/**
 * Command: migrate:down
 *
 * Rolls back migrations to the specified version.
 * Outputs the SQL that would be executed.
 */
function migrateDown(currentVersion: number, targetVersion?: number): void {
  const allMigrations = getAllMigrations();
  const target = targetVersion ?? currentVersion - 1;

  if (target < 0) {
    log('\nCannot rollback below version 0.', 'red');
    return;
  }

  if (target >= currentVersion) {
    log(`\nAlready at version ${currentVersion}. Nothing to rollback.`, 'green');
    return;
  }

  const rollbackMigrations = allMigrations
    .filter((m) => m.version <= currentVersion && m.version > target)
    .reverse(); // Roll back in reverse order

  if (rollbackMigrations.length === 0) {
    log('\nNo migrations to rollback.', 'green');
    return;
  }

  log('\n=== Rolling Back Migrations ===\n', 'bright');
  log(`From version ${currentVersion} to ${target}`, 'cyan');
  log(`Migrations to rollback: ${rollbackMigrations.length}\n`, 'dim');

  for (const migration of rollbackMigrations) {
    log(`--- Rollback Migration ${migration.version} ---`, 'yellow');
    log('\nSQL to execute:', 'dim');
    log(migration.down.trim(), 'cyan');
    log('');
  }

  log('=== Rollback SQL Summary ===\n', 'bright');
  log('To rollback these migrations, execute the SQL above in your DO SQLite instance.', 'dim');
  log('WARNING: Rollbacks may cause data loss. Ensure you have backups.\n', 'red');

  // Output combined SQL for copy/paste
  log('Combined SQL:', 'bright');
  log('```sql');
  for (const migration of rollbackMigrations) {
    log(`-- Rollback migration version ${migration.version}`);
    log(migration.down.trim());
    log(`\nINSERT OR REPLACE INTO schema_meta (key, value) VALUES ('schema_version', '${migration.version - 1}');`);
    log('');
  }
  log('```\n');
}

/**
 * Command: migrate:create
 *
 * Creates a new migration file in the migrations/ directory.
 */
function createMigration(name: string): void {
  // Ensure migrations directory exists
  if (!fs.existsSync(migrationsDir)) {
    fs.mkdirSync(migrationsDir, { recursive: true });
  }

  // Determine next version number
  const existingVersions = getAllMigrations().map((m) => m.version);
  const nextVersion = existingVersions.length > 0
    ? Math.max(...existingVersions) + 1
    : SCHEMA_VERSION + 1;

  // Format version with leading zeros (4 digits)
  const versionStr = String(nextVersion).padStart(4, '0');

  // Sanitize name
  const safeName = name.replace(/[^a-zA-Z0-9_-]/g, '_').toLowerCase();
  const filename = `${versionStr}_${safeName}.ts`;
  const filepath = path.join(migrationsDir, filename);

  const template = `/**
 * Migration: ${safeName}
 * Version: ${nextVersion}
 * Created: ${new Date().toISOString()}
 *
 * Description:
 *   TODO: Add description of what this migration does
 */

/**
 * SQL to apply this migration (upgrade)
 */
export const up = \`
-- TODO: Add your upgrade SQL here
-- Example:
-- CREATE TABLE IF NOT EXISTS new_table (
--   id TEXT PRIMARY KEY,
--   name TEXT NOT NULL
-- );
-- CREATE INDEX IF NOT EXISTS idx_new_table_name ON new_table(name);
\`;

/**
 * SQL to reverse this migration (downgrade)
 */
export const down = \`
-- TODO: Add your downgrade SQL here
-- Example:
-- DROP INDEX IF EXISTS idx_new_table_name;
-- DROP TABLE IF EXISTS new_table;
\`;

/**
 * Migration metadata
 */
export const meta = {
  version: ${nextVersion},
  name: '${safeName}',
  createdAt: '${new Date().toISOString()}',
};
`;

  fs.writeFileSync(filepath, template);

  log('\n=== Migration Created ===\n', 'bright');
  log(`File: ${filepath}`, 'green');
  log(`Version: ${nextVersion}`, 'cyan');
  log('\nEdit the file to add your migration SQL.', 'dim');
  log('Then run "npm run migrate:up" to apply.\n', 'dim');
}

/**
 * Print help message
 */
function printHelp(): void {
  log('\n=== GraphDB Schema Migration CLI ===\n', 'bright');
  log('Usage: npx tsx scripts/migrate.ts <command> [options]\n', 'dim');

  log('Commands:', 'cyan');
  log('  status              Show current schema version and migration status');
  log('  up [--version N]    Apply pending migrations (optionally to version N)');
  log('  down [--version N]  Rollback migrations (optionally to version N)');
  log('  create --name NAME  Create a new migration file\n');

  log('Examples:', 'cyan');
  log('  npx tsx scripts/migrate.ts status');
  log('  npx tsx scripts/migrate.ts up');
  log('  npx tsx scripts/migrate.ts up --version 5');
  log('  npx tsx scripts/migrate.ts down');
  log('  npx tsx scripts/migrate.ts down --version 3');
  log('  npx tsx scripts/migrate.ts create --name add_user_preferences\n');

  log('npm scripts:', 'cyan');
  log('  npm run migrate:status   - Show migration status');
  log('  npm run migrate:up       - Apply pending migrations');
  log('  npm run migrate:down     - Rollback last migration');
  log('  npm run migrate:create   - Create new migration (requires --name)\n');

  log('Notes:', 'dim');
  log('  - Core migrations are defined in src/shard/schema.ts');
  log('  - Custom migrations go in migrations/ directory');
  log('  - Migrations are applied automatically at DO runtime');
  log('  - This CLI generates SQL for manual review/execution\n');
}

/**
 * Parse command line arguments
 */
function parseArgs(args: string[]): { command: string; options: Record<string, string> } {
  const command = args[0] || 'help';
  const options: Record<string, string> = {};

  for (let i = 1; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      const key = args[i].slice(2);
      const value = args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : 'true';
      options[key] = value;
      if (value !== 'true') i++;
    }
  }

  return { command, options };
}

/**
 * Main CLI entry point
 */
async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const { command, options } = parseArgs(args);

  // For CLI purposes, we simulate the current version
  // In production, this would be read from the actual database
  const simulatedCurrentVersion = parseInt(options['current'] || String(SCHEMA_VERSION), 10);

  switch (command) {
    case 'status':
      showStatus(simulatedCurrentVersion);
      break;

    case 'up':
      const upTarget = options['version'] ? parseInt(options['version'], 10) : undefined;
      migrateUp(simulatedCurrentVersion, upTarget);
      break;

    case 'down':
      const downTarget = options['version'] ? parseInt(options['version'], 10) : undefined;
      migrateDown(simulatedCurrentVersion, downTarget);
      break;

    case 'create':
      if (!options['name']) {
        log('\nError: --name is required for create command.', 'red');
        log('Usage: npx tsx scripts/migrate.ts create --name my_migration\n', 'dim');
        process.exit(1);
      }
      createMigration(options['name']);
      break;

    case 'help':
    case '--help':
    case '-h':
      printHelp();
      break;

    default:
      log(`\nUnknown command: ${command}`, 'red');
      printHelp();
      process.exit(1);
  }
}

// Run CLI
main().catch((error) => {
  log(`\nError: ${error.message}`, 'red');
  process.exit(1);
});
