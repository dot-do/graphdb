/**
 * ManifestStore - DO-local manifest storage for zero-RTT lookups
 *
 * Stores manifest and entity index locally in DO SQLite storage,
 * eliminating R2 round trips for metadata during graph traversals.
 *
 * Storage Keys:
 * - manifest:${namespace}:${path} - ManifestFile data (unique per file)
 * - entity:${entityId} - EntityIndexEntry for quick lookup
 * - entityIndex:${namespace} - Array of all entity IDs in namespace (for loading)
 *
 * Key Design:
 * - Entity IDs are URL-based (e.g., https://imdb.com/title/tt0000001)
 * - URLs may contain : and / but these are safe for DO storage keys
 * - Manifest keys include path to support multi-file namespaces
 *
 * @packageDocumentation
 */

import { extractNamespaceFromEntityId } from './graph-lookup.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Manifest file metadata stored in DO
 */
export interface ManifestFile {
  /** Primary key - namespace URL (e.g., https://imdb.com/title/) */
  namespace: string;
  /** R2 path to data file */
  path: string;
  /** Byte offset where footer starts */
  footerOffset: number;
  /** Footer size in bytes */
  footerSize: number;
  /** Number of entities in file */
  entityCount: number;
  /** Version string for staleness check */
  version: string;
  /** Timestamp when updated */
  updatedAt: number;
}

/**
 * Entity index entry for direct lookup
 */
export interface EntityIndexEntry {
  /** Primary key - entity ID (URL) */
  entityId: string;
  /** Which file contains this entity */
  filePath: string;
  /** Offset in file */
  byteOffset: number;
  /** Length of entity data */
  byteLength: number;
}

/**
 * R2 manifest structure (external format)
 */
export interface R2Manifest {
  /** Namespace for this manifest */
  namespace: string;
  /** Version string */
  version: string;
  /** Files in this manifest */
  files: Array<{
    path: string;
    footerOffset: number;
    footerSize: number;
    entityCount: number;
  }>;
  /** Entity index entries */
  entityIndex: EntityIndexEntry[];
  /** Creation timestamp */
  createdAt: number;
}

/**
 * Configuration options for ManifestStore
 */
export interface ManifestStoreConfig {
  /** Maximum number of entities to cache per namespace (default: 100000) */
  maxEntitiesPerNamespace?: number;
  /** Maximum number of namespaces to cache (default: 10) */
  maxCachedNamespaces?: number;
}

/**
 * Cache statistics for monitoring
 */
export interface CacheStats {
  /** Number of cached namespaces */
  cachedNamespaces: number;
  /** Total entities in cache across all namespaces */
  totalCachedEntities: number;
  /** Entities per namespace */
  entitiesPerNamespace: Record<string, number>;
}

/**
 * Error thrown by ManifestStore operations
 */
export class ManifestStoreError extends Error {
  constructor(
    message: string,
    public readonly code: ManifestStoreErrorCode,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'ManifestStoreError';
  }
}

export enum ManifestStoreErrorCode {
  STORAGE_READ_FAILED = 'STORAGE_READ_FAILED',
  STORAGE_WRITE_FAILED = 'STORAGE_WRITE_FAILED',
  INVALID_DATA = 'INVALID_DATA',
  CACHE_LIMIT_EXCEEDED = 'CACHE_LIMIT_EXCEEDED',
}

// ============================================================================
// Constants
// ============================================================================

const DEFAULT_MAX_ENTITIES_PER_NAMESPACE = 100_000;
const DEFAULT_MAX_CACHED_NAMESPACES = 10;

// ============================================================================
// ManifestStore Class
// ============================================================================

/**
 * ManifestStore - DO storage abstraction for manifest and entity index
 *
 * Design principles:
 * 1. Use prefixed keys for type safety: manifest:, entity:, entityIndex:
 * 2. Keep entity index in memory after first load for zero-RTT lookups
 * 3. Support import/export for R2 manifest sync
 * 4. Batch storage operations for efficiency
 * 5. Enforce memory limits with LRU eviction
 */
export class ManifestStore {
  private storage: DurableObjectStorage;
  private config: Required<ManifestStoreConfig>;

  // In-memory cache of entity indexes (loaded on first access)
  private entityIndexCache = new Map<string, Map<string, EntityIndexEntry>>();

  // Track which namespaces have been loaded (in access order for LRU)
  private namespaceAccessOrder: string[] = [];

  constructor(storage: DurableObjectStorage, config: ManifestStoreConfig = {}) {
    this.storage = storage;
    this.config = {
      maxEntitiesPerNamespace:
        config.maxEntitiesPerNamespace ?? DEFAULT_MAX_ENTITIES_PER_NAMESPACE,
      maxCachedNamespaces: config.maxCachedNamespaces ?? DEFAULT_MAX_CACHED_NAMESPACES,
    };
  }

  // --------------------------------------------------------------------------
  // File Manifest Operations
  // --------------------------------------------------------------------------

  /**
   * Get manifest for a namespace (returns first manifest file)
   * For multi-file namespaces, use listManifestsForNamespace
   */
  async getManifest(namespace: string): Promise<ManifestFile | null> {
    try {
      const prefix = `manifest:${namespace}:`;
      const entries = await this.storage.list<ManifestFile>({ prefix, limit: 1 });

      for (const [, value] of entries) {
        if (this.isValidManifestFile(value)) {
          return value;
        }
      }
      return null;
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to get manifest for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_READ_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Get all manifest files for a namespace (for multi-file namespaces)
   */
  async listManifestsForNamespace(namespace: string): Promise<ManifestFile[]> {
    try {
      const prefix = `manifest:${namespace}:`;
      const entries = await this.storage.list<ManifestFile>({ prefix });

      const manifests: ManifestFile[] = [];
      for (const [, value] of entries) {
        if (this.isValidManifestFile(value)) {
          manifests.push(value);
        }
      }
      return manifests;
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to list manifests for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_READ_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Store manifest for a namespace
   */
  async putManifest(manifest: ManifestFile): Promise<void> {
    // Include path in key to support multi-file namespaces
    const key = `manifest:${manifest.namespace}:${manifest.path}`;
    try {
      await this.storage.put(key, manifest);
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to store manifest for namespace: ${manifest.namespace}`,
        ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Delete manifest for a namespace and path
   */
  async deleteManifest(namespace: string, path: string): Promise<boolean> {
    const key = `manifest:${namespace}:${path}`;
    try {
      return await this.storage.delete(key);
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to delete manifest for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * List all stored manifests across all namespaces
   */
  async listManifests(): Promise<ManifestFile[]> {
    try {
      const prefix = 'manifest:';
      const entries = await this.storage.list<ManifestFile>({ prefix });

      const manifests: ManifestFile[] = [];
      for (const [, value] of entries) {
        if (this.isValidManifestFile(value)) {
          manifests.push(value);
        }
      }

      return manifests;
    } catch (error) {
      throw new ManifestStoreError(
        'Failed to list manifests',
        ManifestStoreErrorCode.STORAGE_READ_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  // --------------------------------------------------------------------------
  // Entity Index Operations
  // --------------------------------------------------------------------------

  /**
   * Load entity index for a namespace into memory
   *
   * Returns a Map for O(1) lookups. The index is cached in memory
   * for subsequent calls with LRU eviction when limits are reached.
   */
  async loadEntityIndex(namespace: string): Promise<Map<string, EntityIndexEntry>> {
    // Check memory cache first
    if (this.entityIndexCache.has(namespace)) {
      this.touchNamespace(namespace);
      return this.entityIndexCache.get(namespace)!;
    }

    try {
      // Load from storage
      const indexKey = `entityIndex:${namespace}`;
      const entityIds = await this.storage.get<string[]>(indexKey);

      const index = new Map<string, EntityIndexEntry>();

      if (entityIds && entityIds.length > 0) {
        // Batch load all entity entries for efficiency
        const entryKeys = entityIds.map((id) => `entity:${id}`);
        const entries = await this.storage.get<EntityIndexEntry>(entryKeys);

        for (const [_key, entry] of entries) {
          if (entry && this.isValidEntityIndexEntry(entry)) {
            index.set(entry.entityId, entry);
          }
        }
      }

      // Cache in memory with LRU eviction
      this.cacheNamespaceIndex(namespace, index);

      return index;
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to load entity index for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_READ_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Store entity index entries for a namespace
   *
   * Uses batch operations for efficiency.
   */
  async putEntityIndex(namespace: string, entries: EntityIndexEntry[]): Promise<void> {
    try {
      // Batch store all entity entries
      const entityMap = new Map<string, EntityIndexEntry>();
      const entityIds: string[] = [];

      for (const entry of entries) {
        const key = `entity:${entry.entityId}`;
        entityMap.set(key, entry);
        entityIds.push(entry.entityId);
      }

      // Single batch put for all entities
      if (entityMap.size > 0) {
        await this.storage.put(Object.fromEntries(entityMap));
      }

      // Store the list of entity IDs for this namespace
      const indexKey = `entityIndex:${namespace}`;
      await this.storage.put(indexKey, entityIds);

      // Update in-memory cache
      const index = new Map<string, EntityIndexEntry>();
      for (const entry of entries) {
        index.set(entry.entityId, entry);
      }
      this.cacheNamespaceIndex(namespace, index);
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to store entity index for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Delete all entity index entries for a namespace
   */
  async deleteEntityIndex(namespace: string): Promise<void> {
    try {
      // Get current entity IDs
      const indexKey = `entityIndex:${namespace}`;
      const entityIds = await this.storage.get<string[]>(indexKey);

      if (entityIds && entityIds.length > 0) {
        // Batch delete all entity entries
        const keysToDelete = entityIds.map((id) => `entity:${id}`);
        keysToDelete.push(indexKey);
        await this.storage.delete(keysToDelete);
      } else {
        await this.storage.delete(indexKey);
      }

      // Clear from memory cache
      this.entityIndexCache.delete(namespace);
      this.namespaceAccessOrder = this.namespaceAccessOrder.filter((ns) => ns !== namespace);
    } catch (error) {
      throw new ManifestStoreError(
        `Failed to delete entity index for namespace: ${namespace}`,
        ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Lookup a single entity by ID
   *
   * This method:
   * 1. Extracts namespace from entity ID
   * 2. Loads the namespace's index if not cached
   * 3. Returns the entry from memory cache
   */
  async lookupEntity(entityId: string): Promise<EntityIndexEntry | null> {
    // Extract namespace from entity ID
    const namespace = extractNamespaceFromEntityId(entityId);

    // Ensure index is loaded
    if (!this.entityIndexCache.has(namespace)) {
      await this.loadEntityIndex(namespace);
    } else {
      this.touchNamespace(namespace);
    }

    // Look up in memory cache
    const index = this.entityIndexCache.get(namespace);
    if (!index) {
      return null;
    }

    return index.get(entityId) ?? null;
  }

  // --------------------------------------------------------------------------
  // Bulk Operations
  // --------------------------------------------------------------------------

  /**
   * Import from R2 manifest format
   *
   * Populates DO storage with manifest metadata and entity index
   * from an R2-stored manifest.
   *
   * Note: This properly handles multi-file manifests by using unique keys
   * per file path.
   */
  async importFromR2Manifest(r2Manifest: R2Manifest): Promise<void> {
    const now = Date.now();

    try {
      // Batch all manifest storage operations
      const manifestPuts = new Map<string, ManifestFile>();

      for (const file of r2Manifest.files) {
        const manifest: ManifestFile = {
          namespace: r2Manifest.namespace,
          path: file.path,
          footerOffset: file.footerOffset,
          footerSize: file.footerSize,
          entityCount: file.entityCount,
          version: r2Manifest.version,
          updatedAt: now,
        };

        // Use namespace + path as unique key for multi-file namespaces
        const key = `manifest:${r2Manifest.namespace}:${file.path}`;
        manifestPuts.set(key, manifest);
      }

      // Batch put all manifests
      if (manifestPuts.size > 0) {
        await this.storage.put(Object.fromEntries(manifestPuts));
      }

      // Import entity index
      await this.putEntityIndex(r2Manifest.namespace, r2Manifest.entityIndex);
    } catch (error) {
      if (error instanceof ManifestStoreError) {
        throw error;
      }
      throw new ManifestStoreError(
        `Failed to import R2 manifest for namespace: ${r2Manifest.namespace}`,
        ManifestStoreErrorCode.STORAGE_WRITE_FAILED,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Export to R2 manifest format
   *
   * Reconstructs an R2Manifest from DO storage.
   * For multi-namespace stores, exports only the first namespace found.
   */
  async exportToR2Manifest(namespace?: string): Promise<R2Manifest> {
    const manifests = namespace
      ? await this.listManifestsForNamespace(namespace)
      : await this.listManifests();

    if (manifests.length === 0) {
      return {
        namespace: namespace ?? '',
        version: '',
        files: [],
        entityIndex: [],
        createdAt: Date.now(),
      };
    }

    // Use provided namespace or first found
    const targetNamespace = namespace ?? manifests[0]!.namespace;
    const version = manifests.find((m) => m.namespace === targetNamespace)!.version ?? '';

    // Collect files for target namespace
    const files = manifests
      .filter((m) => m.namespace === targetNamespace)
      .map((m) => ({
        path: m.path,
        footerOffset: m.footerOffset,
        footerSize: m.footerSize,
        entityCount: m.entityCount,
      }));

    // Collect entity index
    const index = await this.loadEntityIndex(targetNamespace);
    const entityIndex: EntityIndexEntry[] = Array.from(index.values());

    return {
      namespace: targetNamespace,
      version,
      files,
      entityIndex,
      createdAt: Date.now(),
    };
  }

  // --------------------------------------------------------------------------
  // Version Check
  // --------------------------------------------------------------------------

  /**
   * Check if local manifest is stale compared to R2 version
   */
  async isStale(namespace: string, r2Version: string): Promise<boolean> {
    const manifest = await this.getManifest(namespace);

    // No local manifest = stale
    if (!manifest) {
      return true;
    }

    // Compare versions
    return manifest.version !== r2Version;
  }

  // --------------------------------------------------------------------------
  // Cache Management
  // --------------------------------------------------------------------------

  /**
   * Clear in-memory caches (storage is unaffected)
   */
  clearMemoryCache(): void {
    this.entityIndexCache.clear();
    this.namespaceAccessOrder = [];
  }

  /**
   * Check if a namespace's entity index is loaded in memory
   */
  isNamespaceLoaded(namespace: string): boolean {
    return this.entityIndexCache.has(namespace);
  }

  /**
   * Get cache statistics for monitoring
   */
  getCacheStats(): CacheStats {
    const entitiesPerNamespace: Record<string, number> = {};
    let totalCachedEntities = 0;

    for (const [namespace, index] of this.entityIndexCache) {
      entitiesPerNamespace[namespace] = index.size;
      totalCachedEntities += index.size;
    }

    return {
      cachedNamespaces: this.entityIndexCache.size,
      totalCachedEntities,
      entitiesPerNamespace,
    };
  }

  // --------------------------------------------------------------------------
  // Private Helpers
  // --------------------------------------------------------------------------

  /**
   * Type guard for ManifestFile
   */
  private isValidManifestFile(value: unknown): value is ManifestFile {
    if (!value || typeof value !== 'object') return false;
    const v = value as Record<string, unknown>;
    return (
      typeof v['namespace'] === 'string' &&
      typeof v['path'] === 'string' &&
      typeof v['footerOffset'] === 'number' &&
      typeof v['footerSize'] === 'number' &&
      typeof v['entityCount'] === 'number' &&
      typeof v['version'] === 'string' &&
      typeof v['updatedAt'] === 'number'
    );
  }

  /**
   * Type guard for EntityIndexEntry
   */
  private isValidEntityIndexEntry(value: unknown): value is EntityIndexEntry {
    if (!value || typeof value !== 'object') return false;
    const v = value as Record<string, unknown>;
    return (
      typeof v['entityId'] === 'string' &&
      typeof v['filePath'] === 'string' &&
      typeof v['byteOffset'] === 'number' &&
      typeof v['byteLength'] === 'number'
    );
  }

  /**
   * Update LRU access order for a namespace
   */
  private touchNamespace(namespace: string): void {
    const idx = this.namespaceAccessOrder.indexOf(namespace);
    if (idx !== -1) {
      this.namespaceAccessOrder.splice(idx, 1);
    }
    this.namespaceAccessOrder.push(namespace);
  }

  /**
   * Cache namespace index with LRU eviction
   */
  private cacheNamespaceIndex(namespace: string, index: Map<string, EntityIndexEntry>): void {
    // Check if we're at the limit
    if (
      this.entityIndexCache.size >= this.config.maxCachedNamespaces &&
      !this.entityIndexCache.has(namespace)
    ) {
      // Evict least recently used namespace
      const lruNamespace = this.namespaceAccessOrder.shift();
      if (lruNamespace) {
        this.entityIndexCache.delete(lruNamespace);
      }
    }

    // Check if index exceeds per-namespace limit
    if (index.size > this.config.maxEntitiesPerNamespace) {
      // Truncate to limit (keep first N entries)
      const truncatedIndex = new Map<string, EntityIndexEntry>();
      let count = 0;
      for (const [entityId, entry] of index) {
        if (count >= this.config.maxEntitiesPerNamespace) break;
        truncatedIndex.set(entityId, entry);
        count++;
      }
      this.entityIndexCache.set(namespace, truncatedIndex);
    } else {
      this.entityIndexCache.set(namespace, index);
    }

    this.touchNamespace(namespace);
  }
}
