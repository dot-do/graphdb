/**
 * Environment bindings for GraphDB Worker
 *
 * This interface defines the Cloudflare Workers environment bindings
 * required by GraphDB Durable Objects and the main worker.
 *
 * Centralized here to avoid circular dependencies between modules.
 */
export interface Env {
  // Durable Object namespaces
  BROKER: DurableObjectNamespace;
  SHARD: DurableObjectNamespace;
  COORDINATOR: DurableObjectNamespace;
  CDC_COORDINATOR: DurableObjectNamespace;
  TRAVERSAL_DO: DurableObjectNamespace;

  // R2 bucket for lakehouse storage
  LAKEHOUSE: R2Bucket;

  // KV for edge cache metadata
  CACHE_META: KVNamespace;
}
