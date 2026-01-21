#!/usr/bin/env npx tsx
/**
 * Backup/Restore CLI for R2 CDC Lakehouse
 *
 * Provides disaster recovery capabilities by managing backups and restores
 * from the R2 CDC lakehouse.
 *
 * Commands:
 *   list         List available backup snapshots
 *   info         Get detailed information about a specific backup
 *   validate     Validate backup integrity
 *   restore      Restore from a backup (dry-run by default)
 *   export       Export backup events to a local file
 *
 * Usage:
 *   npx tsx scripts/backup.ts list --namespace https://example.com/crm
 *   npx tsx scripts/backup.ts info --namespace https://example.com/crm --date 2024-01-15
 *   npx tsx scripts/backup.ts validate --namespace https://example.com/crm --date 2024-01-15
 *   npx tsx scripts/backup.ts restore --namespace https://example.com/crm --timestamp 2024-01-15T12:00:00Z
 *   npx tsx scripts/backup.ts export --namespace https://example.com/crm --date 2024-01-15 --output backup.json
 *
 * Environment Variables:
 *   R2_BUCKET_NAME      R2 bucket name (default: graphdb-lakehouse-prod)
 *   CLOUDFLARE_API_TOKEN  Cloudflare API token for R2 access
 *   CLOUDFLARE_ACCOUNT_ID Cloudflare account ID
 */

import { execSync } from 'child_process';
import { writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// ============================================================================
// CONFIGURATION
// ============================================================================

const R2_BUCKET_NAME = process.env.R2_BUCKET_NAME || 'graphdb-lakehouse-prod';

// ============================================================================
// ARGUMENT PARSING
// ============================================================================

interface CLIArgs {
  command: string;
  namespace?: string;
  date?: string;
  startDate?: string;
  endDate?: string;
  timestamp?: string;
  output?: string;
  limit?: number;
  batchSize?: number;
  dryRun?: boolean;
  includeDeletes?: boolean;
  verbose?: boolean;
  help?: boolean;
}

function parseArgs(): CLIArgs {
  const args = process.argv.slice(2);
  const result: CLIArgs = {
    command: args[0] || 'help',
    dryRun: true, // Default to dry-run for safety
    includeDeletes: true,
    verbose: false,
    help: false,
  };

  for (let i = 1; i < args.length; i++) {
    const arg = args[i];
    const nextArg = args[i + 1];

    switch (arg) {
      case '--namespace':
      case '-n':
        result.namespace = nextArg;
        i++;
        break;
      case '--date':
      case '-d':
        result.date = nextArg;
        i++;
        break;
      case '--start-date':
        result.startDate = nextArg;
        i++;
        break;
      case '--end-date':
        result.endDate = nextArg;
        i++;
        break;
      case '--timestamp':
      case '-t':
        result.timestamp = nextArg;
        i++;
        break;
      case '--output':
      case '-o':
        result.output = nextArg;
        i++;
        break;
      case '--limit':
      case '-l':
        result.limit = parseInt(nextArg || '10', 10);
        i++;
        break;
      case '--batch-size':
      case '-b':
        result.batchSize = parseInt(nextArg || '1000', 10);
        i++;
        break;
      case '--execute':
      case '-x':
        result.dryRun = false;
        break;
      case '--no-deletes':
        result.includeDeletes = false;
        break;
      case '--verbose':
      case '-v':
        result.verbose = true;
        break;
      case '--help':
      case '-h':
        result.help = true;
        break;
    }
  }

  return result;
}

// ============================================================================
// OUTPUT UTILITIES
// ============================================================================

function log(message: string): void {
  console.log(message);
}

function logVerbose(args: CLIArgs, message: string): void {
  if (args.verbose) {
    console.log(`  [verbose] ${message}`);
  }
}

function logError(message: string): void {
  console.error(`ERROR: ${message}`);
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
  return `${(ms / 3600000).toFixed(1)}h`;
}

function formatTimestamp(timestamp: bigint | undefined): string {
  if (!timestamp) return 'N/A';
  return new Date(Number(timestamp)).toISOString();
}

// ============================================================================
// R2 ACCESS VIA WRANGLER
// ============================================================================

/**
 * List objects in R2 using wrangler
 */
function r2List(prefix: string): string[] {
  try {
    const output = execSync(
      `wrangler r2 object list ${R2_BUCKET_NAME} --prefix "${prefix}" --json`,
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    const objects = JSON.parse(output);
    return objects.map((obj: { key: string }) => obj.key);
  } catch (error) {
    // If no objects found, return empty array
    return [];
  }
}

/**
 * Get object metadata from R2 using wrangler
 */
function r2Head(key: string): { size: number; etag: string } | null {
  try {
    const output = execSync(
      `wrangler r2 object get ${R2_BUCKET_NAME} "${key}" --header-only --json`,
      { encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] }
    );
    const meta = JSON.parse(output);
    return { size: meta.size || 0, etag: meta.etag || '' };
  } catch {
    return null;
  }
}

/**
 * Download object from R2 using wrangler
 */
function r2Get(key: string): Uint8Array | null {
  try {
    const output = execSync(
      `wrangler r2 object get ${R2_BUCKET_NAME} "${key}"`,
      { encoding: 'buffer', stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: 100 * 1024 * 1024 }
    );
    return new Uint8Array(output);
  } catch {
    return null;
  }
}

// ============================================================================
// PATH UTILITIES (Mirrored from r2-writer.ts)
// ============================================================================

function parseNamespaceToPath(namespace: string): string {
  const url = new URL(namespace);
  const domainParts = url.hostname.split('.');
  const reversedDomain = domainParts.reverse().map((part) => `.${part}`).join('/');
  const pathParts = url.pathname.split('/').filter((p) => p.length > 0);
  const pathStr = pathParts.length > 0 ? '/' + pathParts.join('/') : '';
  return reversedDomain + pathStr;
}

function parseCDCPath(path: string): { date: string; sequence: string } | null {
  const walMatch = path.match(/\/_wal\/(\d{4}-\d{2}-\d{2})\/(\d{6}-\d{3}|\d{3})\.gcol$/);
  if (!walMatch) return null;
  return { date: walMatch[1]!, sequence: walMatch[2]! };
}

// ============================================================================
// BACKUP OPERATIONS
// ============================================================================

interface BackupSnapshot {
  date: string;
  namespace: string;
  fileCount: number;
  totalSizeBytes?: number;
  files: string[];
}

/**
 * List all backup snapshots for a namespace
 */
function listBackupSnapshots(
  namespace: string,
  options?: { startDate?: string; endDate?: string; limit?: number }
): BackupSnapshot[] {
  const namespacePath = parseNamespaceToPath(namespace);
  const walPrefix = `${namespacePath}/_wal/`;

  log(`Scanning R2 prefix: ${walPrefix}`);

  const allKeys = r2List(walPrefix);
  const cdcFiles = allKeys.filter((key) => key.endsWith('.gcol'));

  if (cdcFiles.length === 0) {
    return [];
  }

  // Group by date
  const filesByDate = new Map<string, string[]>();

  for (const file of cdcFiles) {
    const parsed = parseCDCPath(file);
    if (!parsed) continue;

    const { date } = parsed;

    // Apply date filters
    if (options?.startDate && date < options.startDate) continue;
    if (options?.endDate && date > options.endDate) continue;

    if (!filesByDate.has(date)) {
      filesByDate.set(date, []);
    }
    filesByDate.get(date)!.push(file);
  }

  // Convert to snapshots
  const snapshots: BackupSnapshot[] = [];

  for (const [date, dateFiles] of filesByDate) {
    dateFiles.sort();
    snapshots.push({
      date,
      namespace,
      fileCount: dateFiles.length,
      files: dateFiles,
    });
  }

  // Sort by date (oldest first)
  snapshots.sort((a, b) => a.date.localeCompare(b.date));

  // Apply limit
  if (options?.limit && snapshots.length > options.limit) {
    return snapshots.slice(-options.limit); // Return most recent
  }

  return snapshots;
}

/**
 * Get detailed info about a snapshot
 */
function getSnapshotInfo(snapshot: BackupSnapshot): BackupSnapshot & { totalSizeBytes: number } {
  let totalSize = 0;

  for (const file of snapshot.files) {
    const meta = r2Head(file);
    if (meta) {
      totalSize += meta.size;
    }
  }

  return {
    ...snapshot,
    totalSizeBytes: totalSize,
  };
}

/**
 * Validate a backup snapshot
 */
function validateSnapshot(snapshot: BackupSnapshot): { valid: boolean; missingFiles: string[]; corruptedFiles: string[] } {
  const missingFiles: string[] = [];
  const corruptedFiles: string[] = [];

  for (const file of snapshot.files) {
    const meta = r2Head(file);
    if (!meta) {
      missingFiles.push(file);
      continue;
    }

    // Try to download and validate (basic check)
    const data = r2Get(file);
    if (!data || data.length === 0) {
      corruptedFiles.push(file);
    }
  }

  return {
    valid: missingFiles.length === 0 && corruptedFiles.length === 0,
    missingFiles,
    corruptedFiles,
  };
}

// ============================================================================
// COMMAND HANDLERS
// ============================================================================

function handleList(args: CLIArgs): void {
  if (!args.namespace) {
    logError('--namespace is required');
    process.exit(1);
  }

  log(`\nListing backups for namespace: ${args.namespace}\n`);

  const snapshots = listBackupSnapshots(args.namespace, {
    startDate: args.startDate,
    endDate: args.endDate,
    limit: args.limit,
  });

  if (snapshots.length === 0) {
    log('No backups found.');
    return;
  }

  log(`Found ${snapshots.length} backup snapshot(s):\n`);
  log('  DATE        | FILES | NAMESPACE');
  log('  ------------|-------|' + '-'.repeat(50));

  for (const snapshot of snapshots) {
    log(`  ${snapshot.date} | ${String(snapshot.fileCount).padStart(5)} | ${snapshot.namespace}`);
  }

  log('');
}

function handleInfo(args: CLIArgs): void {
  if (!args.namespace) {
    logError('--namespace is required');
    process.exit(1);
  }

  if (!args.date) {
    logError('--date is required (format: YYYY-MM-DD)');
    process.exit(1);
  }

  log(`\nGetting backup info for ${args.namespace} on ${args.date}\n`);

  const snapshots = listBackupSnapshots(args.namespace, {
    startDate: args.date,
    endDate: args.date,
  });

  if (snapshots.length === 0) {
    log('No backup found for the specified date.');
    return;
  }

  const snapshot = snapshots[0]!;
  log('Calculating backup size...');
  const info = getSnapshotInfo(snapshot);

  log('');
  log('Backup Snapshot Information');
  log('===========================');
  log(`  Date:       ${info.date}`);
  log(`  Namespace:  ${info.namespace}`);
  log(`  Files:      ${info.fileCount}`);
  log(`  Total Size: ${formatBytes(info.totalSizeBytes)}`);
  log('');

  if (args.verbose && info.files.length <= 20) {
    log('Files:');
    for (const file of info.files) {
      log(`  - ${file}`);
    }
    log('');
  } else if (info.files.length > 20) {
    log(`Files: (showing first 5 and last 5 of ${info.files.length})`);
    for (const file of info.files.slice(0, 5)) {
      log(`  - ${file}`);
    }
    log('  ...');
    for (const file of info.files.slice(-5)) {
      log(`  - ${file}`);
    }
    log('');
  }
}

function handleValidate(args: CLIArgs): void {
  if (!args.namespace) {
    logError('--namespace is required');
    process.exit(1);
  }

  if (!args.date) {
    logError('--date is required (format: YYYY-MM-DD)');
    process.exit(1);
  }

  log(`\nValidating backup for ${args.namespace} on ${args.date}\n`);

  const snapshots = listBackupSnapshots(args.namespace, {
    startDate: args.date,
    endDate: args.date,
  });

  if (snapshots.length === 0) {
    log('No backup found for the specified date.');
    return;
  }

  const snapshot = snapshots[0]!;
  log(`Validating ${snapshot.fileCount} files...`);

  const validation = validateSnapshot(snapshot);

  log('');
  if (validation.valid) {
    log('VALIDATION PASSED: All files are present and readable.');
  } else {
    log('VALIDATION FAILED:');
    if (validation.missingFiles.length > 0) {
      log(`  Missing files: ${validation.missingFiles.length}`);
      for (const file of validation.missingFiles.slice(0, 5)) {
        log(`    - ${file}`);
      }
      if (validation.missingFiles.length > 5) {
        log(`    ... and ${validation.missingFiles.length - 5} more`);
      }
    }
    if (validation.corruptedFiles.length > 0) {
      log(`  Corrupted files: ${validation.corruptedFiles.length}`);
      for (const file of validation.corruptedFiles.slice(0, 5)) {
        log(`    - ${file}`);
      }
      if (validation.corruptedFiles.length > 5) {
        log(`    ... and ${validation.corruptedFiles.length - 5} more`);
      }
    }
  }
  log('');
}

function handleRestore(args: CLIArgs): void {
  if (!args.namespace) {
    logError('--namespace is required');
    process.exit(1);
  }

  const isDryRun = args.dryRun;

  log(`\nRestore Operation ${isDryRun ? '(DRY RUN)' : '(LIVE)'}`);
  log('='.repeat(50));
  log(`  Namespace:      ${args.namespace}`);

  if (args.timestamp) {
    log(`  Target Time:    ${args.timestamp}`);
  }
  if (args.date) {
    log(`  Filter Date:    ${args.date}`);
  }
  log(`  Include Deletes: ${args.includeDeletes}`);
  log(`  Batch Size:     ${args.batchSize || 1000}`);
  log('');

  if (isDryRun) {
    log('This is a DRY RUN. No changes will be made.');
    log('Use --execute or -x flag to perform actual restore.');
    log('');
  }

  // List snapshots to restore
  const snapshots = listBackupSnapshots(args.namespace, {
    startDate: args.startDate,
    endDate: args.endDate || args.date,
  });

  if (snapshots.length === 0) {
    log('No backups found to restore.');
    return;
  }

  // Calculate totals
  let totalFiles = 0;
  let totalSize = 0;

  for (const snapshot of snapshots) {
    totalFiles += snapshot.fileCount;
    const info = getSnapshotInfo(snapshot);
    totalSize += info.totalSizeBytes;
  }

  log(`Found ${snapshots.length} snapshot(s) with ${totalFiles} files (${formatBytes(totalSize)})`);
  log('');

  if (isDryRun) {
    log('Snapshot dates that would be restored:');
    for (const snapshot of snapshots) {
      log(`  - ${snapshot.date} (${snapshot.fileCount} files)`);
    }
    log('');
    log('To execute the restore, run with --execute flag.');
    return;
  }

  // Actual restore would happen here
  log('');
  log('RESTORE NOT IMPLEMENTED IN CLI');
  log('');
  log('To restore programmatically, use the restoreFromBackup function:');
  log('');
  log('  import { restoreFromBackup } from "@dotdo/graphdb/storage";');
  log('');
  log('  await restoreFromBackup(bucket, namespace, async (events) => {');
  log('    // Process events');
  log('    await tripleStore.insertBatch(events.map(e => e.triple));');
  log('  }, {');
  log(`    targetTimestamp: BigInt(Date.parse("${args.timestamp || new Date().toISOString()}")),`);
  log('    onProgress: (p) => console.log(`${p.percentComplete}% complete`),');
  log('  });');
  log('');
}

function handleExport(args: CLIArgs): void {
  if (!args.namespace) {
    logError('--namespace is required');
    process.exit(1);
  }

  if (!args.output) {
    logError('--output is required');
    process.exit(1);
  }

  log(`\nExporting backup for ${args.namespace}\n`);

  const snapshots = listBackupSnapshots(args.namespace, {
    startDate: args.startDate || args.date,
    endDate: args.endDate || args.date,
  });

  if (snapshots.length === 0) {
    log('No backups found to export.');
    return;
  }

  // Ensure output directory exists
  const outputDir = dirname(args.output);
  if (outputDir && !existsSync(outputDir)) {
    mkdirSync(outputDir, { recursive: true });
  }

  log(`Exporting ${snapshots.length} snapshot(s) to ${args.output}`);

  const exportData = {
    exportedAt: new Date().toISOString(),
    namespace: args.namespace,
    snapshots: snapshots.map((s) => ({
      date: s.date,
      fileCount: s.fileCount,
      files: s.files,
    })),
  };

  writeFileSync(args.output, JSON.stringify(exportData, null, 2));

  log(`Export complete: ${args.output}`);
  log('');
}

function showHelp(): void {
  log(`
Backup/Restore CLI for R2 CDC Lakehouse

USAGE:
  npx tsx scripts/backup.ts <command> [options]

COMMANDS:
  list        List available backup snapshots
  info        Get detailed information about a specific backup
  validate    Validate backup integrity
  restore     Restore from a backup (dry-run by default)
  export      Export backup metadata to a local file
  help        Show this help message

OPTIONS:
  --namespace, -n <url>    Namespace URL (required for most commands)
  --date, -d <date>        Specific date filter (YYYY-MM-DD)
  --start-date <date>      Start date for range filter
  --end-date <date>        End date for range filter
  --timestamp, -t <iso>    Target timestamp for point-in-time recovery
  --output, -o <file>      Output file path (for export command)
  --limit, -l <n>          Limit number of results
  --batch-size, -b <n>     Batch size for restore operations (default: 1000)
  --execute, -x            Execute restore (default is dry-run)
  --no-deletes             Exclude delete events from restore
  --verbose, -v            Enable verbose output
  --help, -h               Show this help message

EXAMPLES:
  # List all backups for a namespace
  npx tsx scripts/backup.ts list --namespace https://example.com/crm

  # List backups from the last week
  npx tsx scripts/backup.ts list -n https://example.com/crm --start-date 2024-01-08

  # Get info about a specific day's backup
  npx tsx scripts/backup.ts info -n https://example.com/crm --date 2024-01-15

  # Validate backup integrity
  npx tsx scripts/backup.ts validate -n https://example.com/crm --date 2024-01-15

  # Dry-run restore to a specific point in time
  npx tsx scripts/backup.ts restore -n https://example.com/crm -t 2024-01-15T12:00:00Z

  # Execute actual restore (caution!)
  npx tsx scripts/backup.ts restore -n https://example.com/crm -t 2024-01-15T12:00:00Z --execute

  # Export backup metadata
  npx tsx scripts/backup.ts export -n https://example.com/crm --date 2024-01-15 -o backup-meta.json

ENVIRONMENT VARIABLES:
  R2_BUCKET_NAME         R2 bucket name (default: graphdb-lakehouse-prod)
  CLOUDFLARE_API_TOKEN   Cloudflare API token (for wrangler)
  CLOUDFLARE_ACCOUNT_ID  Cloudflare account ID (for wrangler)

NOTE: This CLI uses wrangler for R2 access. Ensure you are logged in:
  wrangler login
`);
}

// ============================================================================
// MAIN
// ============================================================================

async function main(): Promise<void> {
  const args = parseArgs();

  if (args.help || args.command === 'help') {
    showHelp();
    return;
  }

  switch (args.command) {
    case 'list':
      handleList(args);
      break;
    case 'info':
      handleInfo(args);
      break;
    case 'validate':
      handleValidate(args);
      break;
    case 'restore':
      handleRestore(args);
      break;
    case 'export':
      handleExport(args);
      break;
    default:
      logError(`Unknown command: ${args.command}`);
      log('');
      showHelp();
      process.exit(1);
  }
}

main().catch((error) => {
  logError(error.message);
  process.exit(1);
});
