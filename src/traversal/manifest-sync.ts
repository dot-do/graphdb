/**
 * ManifestSync - Sync protocol between R2 and DO storage
 *
 * Keeps DO storage consistent with R2 (source of truth).
 *
 * Sync Protocol:
 * 1. On DO cold start: check DO storage version vs R2 manifest
 * 2. If stale or empty: fetch R2 manifest, populate DO storage
 * 3. On write: update DO storage first, then R2 manifest
 * 4. On conflict: R2 wins (source of truth)
 *
 * Concurrency Safety:
 * - Sync operations are serialized using a mutex to prevent race conditions
 * - R2 fetches may fail; errors are handled gracefully with proper reporting
 * - State updates (lastSyncTime, cachedR2Version) happen atomically with sync
 *
 * @packageDocumentation
 */

import type { ManifestStore, R2Manifest } from './manifest-store.js';
import { namespaceToR2Path } from './graph-lookup.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Configuration for ManifestSync
 */
export interface ManifestSyncConfig {
  /** R2 bucket for persistent storage */
  r2: R2Bucket;
  /** DO-local manifest store */
  store: ManifestStore;
  /** Namespace to sync */
  namespace: string;
}

/**
 * Error codes for sync operations
 */
export type SyncErrorCode =
  | 'R2_FETCH_FAILED'
  | 'R2_WRITE_FAILED'
  | 'INVALID_MANIFEST'
  | 'IMPORT_FAILED'
  | 'EXPORT_FAILED'
  | 'UNKNOWN';

/**
 * Result of a sync operation
 */
export interface SyncResult {
  /** Whether the sync succeeded */
  success: boolean;
  /** Direction of the sync */
  direction: 'from_r2' | 'to_r2' | 'bidirectional' | 'none';
  /** Number of entries updated */
  entriesUpdated: number;
  /** Number of conflicts resolved */
  conflicts: number;
  /** Error message if sync failed */
  error?: string;
  /** Structured error code for programmatic handling */
  errorCode?: SyncErrorCode;
}

/**
 * Current sync status
 */
export interface SyncStatus {
  /** Timestamp of last successful sync */
  lastSyncTime: number | null;
  /** Version in DO storage */
  doVersion: string | null;
  /** Version in R2 storage */
  r2Version: string | null;
  /** Whether DO storage is stale compared to R2 */
  isStale: boolean;
  /** Whether a sync operation is currently in progress */
  syncInProgress: boolean;
}

// ============================================================================
// ManifestSync Class
// ============================================================================

/**
 * ManifestSync - Handles sync between R2 and DO storage
 *
 * R2 is the source of truth. DO storage provides low-latency access
 * by caching the manifest locally.
 *
 * Thread Safety:
 * - Uses a mutex to serialize sync operations
 * - Prevents race conditions when multiple callers trigger sync
 */
export class ManifestSync {
  private readonly r2: R2Bucket;
  private readonly store: ManifestStore;
  private readonly namespace: string;

  // Sync status tracking
  private lastSyncTime: number | null = null;
  private cachedR2Version: string | null = null;

  // Mutex for serializing sync operations
  private syncMutex: Promise<void> = Promise.resolve();
  private syncInProgress = false;

  constructor(config: ManifestSyncConfig) {
    this.r2 = config.r2;
    this.store = config.store;
    this.namespace = config.namespace;
  }

  /**
   * Acquire the sync mutex to serialize operations
   */
  private async withSyncLock<T>(operation: () => Promise<T>): Promise<T> {
    // Wait for any pending sync to complete
    await this.syncMutex;

    // Create a new lock
    let release: () => void;
    this.syncMutex = new Promise((resolve) => {
      release = resolve;
    });
    this.syncInProgress = true;

    try {
      return await operation();
    } finally {
      this.syncInProgress = false;
      release!();
    }
  }

  // --------------------------------------------------------------------------
  // R2 Path Helpers
  // --------------------------------------------------------------------------

  /**
   * Get the R2 path for the manifest file
   */
  private getR2ManifestPath(): string {
    const r2Path = namespaceToR2Path(this.namespace);
    return `${r2Path}/_manifest.json`;
  }

  // --------------------------------------------------------------------------
  // R2 Operations
  // --------------------------------------------------------------------------

  /**
   * Fetch manifest from R2
   *
   * @throws Error with code 'R2_FETCH_FAILED' if R2 request fails
   * @throws Error with code 'INVALID_MANIFEST' if JSON parsing fails
   */
  private async fetchR2Manifest(): Promise<R2Manifest | null> {
    const path = this.getR2ManifestPath();

    let obj: R2ObjectBody | null;
    try {
      obj = await this.r2.get(path);
    } catch (error) {
      const err = new Error(
        `R2 fetch failed for ${path}: ${error instanceof Error ? error.message : String(error)}`
      );
      (err as Error & { code: SyncErrorCode }).code = 'R2_FETCH_FAILED';
      throw err;
    }

    if (!obj) {
      return null;
    }

    let json: string;
    try {
      json = await obj.text();
    } catch (error) {
      const err = new Error(
        `Failed to read R2 response body: ${error instanceof Error ? error.message : String(error)}`
      );
      (err as Error & { code: SyncErrorCode }).code = 'R2_FETCH_FAILED';
      throw err;
    }

    try {
      const manifest = JSON.parse(json) as R2Manifest;
      // Basic validation
      if (!manifest.namespace || !manifest.version || !Array.isArray(manifest.entityIndex)) {
        const err = new Error('Invalid manifest structure: missing required fields');
        (err as Error & { code: SyncErrorCode }).code = 'INVALID_MANIFEST';
        throw err;
      }
      return manifest;
    } catch (error) {
      if ((error as Error & { code?: SyncErrorCode }).code === 'INVALID_MANIFEST') {
        throw error;
      }
      const err = new Error(
        `Failed to parse R2 manifest JSON: ${error instanceof Error ? error.message : String(error)}`
      );
      (err as Error & { code: SyncErrorCode }).code = 'INVALID_MANIFEST';
      throw err;
    }
  }

  /**
   * Write manifest to R2
   *
   * @throws Error with code 'R2_WRITE_FAILED' if R2 write fails
   */
  private async writeR2Manifest(manifest: R2Manifest): Promise<void> {
    const path = this.getR2ManifestPath();
    const json = JSON.stringify(manifest, null, 2);

    try {
      await this.r2.put(path, json, {
        httpMetadata: {
          contentType: 'application/json',
        },
      });
    } catch (error) {
      const err = new Error(
        `R2 write failed for ${path}: ${error instanceof Error ? error.message : String(error)}`
      );
      (err as Error & { code: SyncErrorCode }).code = 'R2_WRITE_FAILED';
      throw err;
    }
  }

  /**
   * Get just the version from R2 manifest (lightweight check)
   *
   * Note: This fetches the full manifest. For production, consider using
   * R2 object metadata (custom metadata) to store version separately.
   */
  private async getR2Version(): Promise<string | null> {
    const manifest = await this.fetchR2Manifest();
    if (!manifest) {
      return null;
    }
    this.cachedR2Version = manifest.version;
    return manifest.version;
  }

  // --------------------------------------------------------------------------
  // Sync Operations
  // --------------------------------------------------------------------------

  /**
   * Extract error code from an error object
   */
  private getErrorCode(error: unknown): SyncErrorCode {
    if (error && typeof error === 'object' && 'code' in error) {
      return (error as { code: SyncErrorCode }).code;
    }
    return 'UNKNOWN';
  }

  /**
   * Check if DO storage needs sync from R2
   *
   * Returns true if:
   * - DO storage is empty (no manifest for namespace)
   * - DO version differs from R2 version
   *
   * @throws Error if R2 fetch fails (network error, invalid JSON)
   */
  async needsSync(): Promise<boolean> {
    // Get DO version
    const doManifest = await this.store.getManifest(this.namespace);
    const doVersion = doManifest?.version ?? null;

    // Get R2 version - may throw on network error
    const r2Version = await this.getR2Version();

    // If no R2 manifest, nothing to sync
    if (!r2Version) {
      return false;
    }

    // If no DO manifest, needs sync
    if (!doVersion) {
      return true;
    }

    // Compare versions
    return doVersion !== r2Version;
  }

  /**
   * Sync from R2 to DO storage (cold start)
   *
   * Fetches the full R2 manifest and populates DO storage.
   * Uses a mutex to prevent concurrent sync operations.
   */
  async syncFromR2(): Promise<SyncResult> {
    return this.withSyncLock(async () => {
      try {
        // Fetch R2 manifest
        const r2Manifest = await this.fetchR2Manifest();

        if (!r2Manifest) {
          return {
            success: true,
            direction: 'from_r2' as const,
            entriesUpdated: 0,
            conflicts: 0,
          };
        }

        // Import into DO storage
        try {
          await this.store.importFromR2Manifest(r2Manifest);
        } catch (importError) {
          const err = new Error(
            `Failed to import manifest: ${importError instanceof Error ? importError.message : String(importError)}`
          );
          (err as Error & { code: SyncErrorCode }).code = 'IMPORT_FAILED';
          throw err;
        }

        // Update sync status atomically after successful sync
        this.lastSyncTime = Date.now();
        this.cachedR2Version = r2Manifest.version;

        return {
          success: true,
          direction: 'from_r2' as const,
          entriesUpdated: r2Manifest.entityIndex.length,
          conflicts: 0,
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const errorCode = this.getErrorCode(error);
        return {
          success: false,
          direction: 'from_r2' as const,
          entriesUpdated: 0,
          conflicts: 0,
          error: message,
          errorCode,
        };
      }
    });
  }

  /**
   * Sync to R2 after local write
   *
   * Exports DO storage to R2 manifest format and writes to R2.
   * Uses a mutex to prevent concurrent sync operations.
   *
   * Note: While this method returns a Promise, for true fire-and-forget
   * behavior in a Durable Object, wrap the call with ctx.waitUntil():
   *
   *   ctx.waitUntil(manifestSync.syncToR2());
   *
   * This ensures the sync completes even if the DO hibernates.
   */
  async syncToR2(): Promise<SyncResult> {
    return this.withSyncLock(async () => {
      try {
        // Export from DO storage
        let manifest: R2Manifest;
        try {
          manifest = await this.store.exportToR2Manifest();
        } catch (exportError) {
          const err = new Error(
            `Failed to export manifest: ${exportError instanceof Error ? exportError.message : String(exportError)}`
          );
          (err as Error & { code: SyncErrorCode }).code = 'EXPORT_FAILED';
          throw err;
        }

        // Nothing to sync if export is empty
        if (!manifest.namespace || manifest.files.length === 0) {
          return {
            success: true,
            direction: 'to_r2' as const,
            entriesUpdated: 0,
            conflicts: 0,
          };
        }

        // Write to R2
        await this.writeR2Manifest(manifest);

        // Update sync status atomically after successful sync
        this.lastSyncTime = Date.now();
        this.cachedR2Version = manifest.version;

        return {
          success: true,
          direction: 'to_r2' as const,
          entriesUpdated: manifest.entityIndex.length,
          conflicts: 0,
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const errorCode = this.getErrorCode(error);
        // Log error for observability
        console.error(`[ManifestSync] Error syncing to R2: ${message} (code: ${errorCode})`);
        return {
          success: false,
          direction: 'to_r2' as const,
          entriesUpdated: 0,
          conflicts: 0,
          error: message,
          errorCode,
        };
      }
    });
  }

  /**
   * Full bidirectional sync with conflict resolution
   *
   * Compares DO and R2 versions:
   * - If R2 is newer (or DO is empty): sync from R2 (R2 wins)
   * - If DO has data but R2 is empty: sync to R2
   * - If both empty: no sync needed
   * - If equal: no sync needed
   *
   * Conflict resolution: R2 always wins (source of truth)
   *
   * Note: This method fetches the R2 manifest once and reuses it,
   * avoiding the double-fetch that would occur if calling syncFromR2 directly.
   */
  async fullSync(): Promise<SyncResult> {
    return this.withSyncLock(async () => {
      try {
        // Get both versions - fetch R2 manifest once for reuse
        const doManifest = await this.store.getManifest(this.namespace);
        const doVersion = doManifest?.version ?? null;
        const r2Manifest = await this.fetchR2Manifest();
        const r2Version = r2Manifest?.version ?? null;

        // Case 1: Neither has data - nothing to sync
        if (!r2Version && !doVersion) {
          return {
            success: true,
            direction: 'none' as const,
            entriesUpdated: 0,
            conflicts: 0,
          };
        }

        // Case 2: R2 is empty but DO has data - sync to R2
        if (!r2Version && doVersion) {
          // Note: We cannot call syncToR2() here as we already hold the lock
          // Instead, inline the export/write logic
          let manifest: R2Manifest;
          try {
            manifest = await this.store.exportToR2Manifest();
          } catch (exportError) {
            const err = new Error(
              `Failed to export manifest: ${exportError instanceof Error ? exportError.message : String(exportError)}`
            );
            (err as Error & { code: SyncErrorCode }).code = 'EXPORT_FAILED';
            throw err;
          }

          if (!manifest.namespace || manifest.files.length === 0) {
            return {
              success: true,
              direction: 'to_r2' as const,
              entriesUpdated: 0,
              conflicts: 0,
            };
          }

          await this.writeR2Manifest(manifest);
          this.lastSyncTime = Date.now();
          this.cachedR2Version = manifest.version;

          return {
            success: true,
            direction: 'to_r2' as const,
            entriesUpdated: manifest.entityIndex.length,
            conflicts: 0,
          };
        }

        // Case 3: DO is empty but R2 has data - sync from R2
        // Case 4: Both have data but versions differ - R2 wins
        if (r2Manifest && (!doVersion || doVersion !== r2Version)) {
          // Import the already-fetched manifest (no double-fetch)
          try {
            await this.store.importFromR2Manifest(r2Manifest);
          } catch (importError) {
            const err = new Error(
              `Failed to import manifest: ${importError instanceof Error ? importError.message : String(importError)}`
            );
            (err as Error & { code: SyncErrorCode }).code = 'IMPORT_FAILED';
            throw err;
          }

          this.lastSyncTime = Date.now();
          this.cachedR2Version = r2Manifest.version;

          const hadConflict = doVersion !== null && doVersion !== r2Version;
          return {
            success: true,
            direction: hadConflict ? 'bidirectional' as const : 'from_r2' as const,
            entriesUpdated: r2Manifest.entityIndex.length,
            conflicts: hadConflict ? 1 : 0,
          };
        }

        // Case 5: Versions match - no sync needed
        this.lastSyncTime = Date.now();
        return {
          success: true,
          direction: 'bidirectional' as const,
          entriesUpdated: 0,
          conflicts: 0,
        };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        const errorCode = this.getErrorCode(error);
        return {
          success: false,
          direction: 'bidirectional' as const,
          entriesUpdated: 0,
          conflicts: 0,
          error: message,
          errorCode,
        };
      }
    });
  }

  /**
   * Get current sync status (synchronous, uses cached values)
   *
   * Note: doVersion and isStale require async fetch to be accurate.
   * Use getSyncStatusAsync() for fresh data.
   */
  getSyncStatus(): SyncStatus {
    return {
      lastSyncTime: this.lastSyncTime,
      doVersion: null, // Will be populated by async call
      r2Version: this.cachedR2Version,
      isStale: false, // Will be populated by async call
      syncInProgress: this.syncInProgress,
    };
  }

  /**
   * Get current sync status with fresh version data
   *
   * @throws Error if R2 fetch fails when r2Version is not cached
   */
  async getSyncStatusAsync(): Promise<SyncStatus> {
    const doManifest = await this.store.getManifest(this.namespace);
    const doVersion = doManifest?.version ?? null;
    const r2Version = this.cachedR2Version ?? (await this.getR2Version());

    const isStale = r2Version !== null && (doVersion === null || doVersion !== r2Version);

    return {
      lastSyncTime: this.lastSyncTime,
      doVersion,
      r2Version,
      isStale,
      syncInProgress: this.syncInProgress,
    };
  }

  /**
   * Check if a sync operation is currently in progress
   */
  isSyncing(): boolean {
    return this.syncInProgress;
  }
}
