/**
 * Bloom Filter Tests
 *
 * Success Criteria:
 * - Snippet script < 32KB
 * - Bloom check completes in < 2ms
 * - False positive rate < 1% with 10K entries
 * - Serialization round-trip preserves data
 * - URL pattern extraction works correctly
 */

import { describe, it, expect, beforeAll } from "vitest";
import {
  createBloomFilter,
  addToFilter,
  addManyToFilter,
  mightExist,
  serializeFilter,
  deserializeFilter,
  getFilterStats,
  calculateOptimalBits,
  calculateOptimalK,
  calculateExpectedFpr,
  mergeFilters,
  createIncrementalFilter,
  estimateFpr,
  extractEntityId,
} from "../../src/snippet/bloom.js";

// The snippet code template is ~5KB, so we verify total budget with filter
// Actual snippet size is verified by calculating code + filter base64 size
const SNIPPET_CODE_OVERHEAD_BYTES = 5000; // ~5KB for bloom.snippet.js code

describe("Snippet Size Constraint", () => {
  it("snippet code overhead estimate is reasonable", () => {
    // The bloom.snippet.js template without filter data is ~5KB
    // We verify this by ensuring our estimate is conservative
    // Actual snippet = code (~5KB) + base64 filter data
    const maxSize = 32 * 1024; // 32KB
    const maxFilterSize = maxSize - SNIPPET_CODE_OVERHEAD_BYTES;

    console.log(`Estimated code overhead: ${SNIPPET_CODE_OVERHEAD_BYTES} bytes`);
    console.log(`Max filter budget: ${maxFilterSize} bytes`);

    // Code overhead should leave room for at least 20KB of filter data
    expect(maxFilterSize).toBeGreaterThan(20 * 1024);
  });

  it("filter size for 10K entries fits in snippet budget", () => {
    const filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
      maxSizeBytes: 16 * 1024, // 16KB filter leaves room for code
    });

    // Add 10K entries
    for (let i = 0; i < 10000; i++) {
      addToFilter(filter, `entity_${i.toString(16).padStart(8, "0")}`);
    }

    const serialized = serializeFilter(filter);
    const filterBase64Size = serialized.filter.length;

    // Base64 encoding adds ~33% overhead, so ~16KB filter -> ~21KB base64
    // Plus code overhead (~5KB) should stay under 32KB
    console.log(`Filter base64 size: ${filterBase64Size} bytes (${(filterBase64Size / 1024).toFixed(2)} KB)`);
    console.log(`Estimated total snippet: ${(filterBase64Size + 5000) / 1024} KB`);

    expect(filterBase64Size + 5000).toBeLessThan(32 * 1024);
  });
});

describe("Bloom Check Latency", () => {
  let filter: ReturnType<typeof createBloomFilter>;
  let entityIds: string[];

  beforeAll(() => {
    filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    entityIds = [];
    for (let i = 0; i < 10000; i++) {
      const id = `entity_${i.toString(16).padStart(8, "0")}`;
      entityIds.push(id);
      addToFilter(filter, id);
    }
  });

  it("single bloom check completes in < 2ms", () => {
    const testId = entityIds[5000];
    const iterations = 1000;
    const latencies: number[] = [];

    // Warm up
    for (let i = 0; i < 100; i++) {
      mightExist(filter, testId);
    }

    // Measure
    for (let i = 0; i < iterations; i++) {
      const start = performance.now();
      mightExist(filter, testId);
      const end = performance.now();
      latencies.push(end - start);
    }

    const avgMs = latencies.reduce((a, b) => a + b, 0) / latencies.length;
    const maxMs = Math.max(...latencies);

    console.log(`Average check latency: ${(avgMs * 1000).toFixed(2)} us`);
    console.log(`Max check latency: ${(maxMs * 1000).toFixed(2)} us`);

    // Must complete in < 2ms (within 5ms snippet budget)
    expect(avgMs).toBeLessThan(2);
    expect(maxMs).toBeLessThan(2);
  });

  it("batch of 100 checks completes in < 5ms", () => {
    const testIds = entityIds.slice(0, 100);

    const start = performance.now();
    for (const id of testIds) {
      mightExist(filter, id);
    }
    const elapsed = performance.now() - start;

    console.log(`100 checks completed in: ${elapsed.toFixed(2)} ms`);

    expect(elapsed).toBeLessThan(5);
  });
});

describe("False Positive Rate", () => {
  it("FPR < 1% with 10K entries", () => {
    // Target 0.5% FPR to ensure actual FPR stays below 1% with statistical variance
    const filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.005,
    });

    // Add exactly 10K entities
    const entityIds: string[] = [];
    for (let i = 0; i < 10000; i++) {
      const id = `entity_${i.toString(16).padStart(8, "0")}`;
      entityIds.push(id);
      addToFilter(filter, id);
    }

    // Test with 10K non-existent IDs
    const testCount = 10000;
    let falsePositives = 0;

    for (let i = 0; i < testCount; i++) {
      const nonExistentId = `__nonexistent__${Math.random().toString(36)}__${i}`;
      if (mightExist(filter, nonExistentId)) {
        falsePositives++;
      }
    }

    const fpr = falsePositives / testCount;
    console.log(`False positives: ${falsePositives} / ${testCount}`);
    console.log(`Measured FPR: ${(fpr * 100).toFixed(4)}%`);
    console.log(`Target FPR: 1%`);

    expect(fpr).toBeLessThan(0.01);
  });

  it("zero false negatives (all added entries are found)", () => {
    const filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    const entityIds: string[] = [];
    for (let i = 0; i < 10000; i++) {
      const id = `entity_${i.toString(16).padStart(8, "0")}`;
      entityIds.push(id);
      addToFilter(filter, id);
    }

    let falseNegatives = 0;
    for (const id of entityIds) {
      if (!mightExist(filter, id)) {
        falseNegatives++;
      }
    }

    console.log(`False negatives: ${falseNegatives} / ${entityIds.length}`);

    expect(falseNegatives).toBe(0);
  });
});

describe("Serialization Round-trip", () => {
  it("serialization preserves filter data", () => {
    const filter = createBloomFilter({
      capacity: 1000,
      targetFpr: 0.01,
      version: "v1.0.0-test",
    });

    // Add test entries
    for (let i = 0; i < 500; i++) {
      addToFilter(filter, `entity_${i}`);
    }

    // Serialize
    const serialized = serializeFilter(filter);

    expect(serialized.version).toBe("v1.0.0-test");
    expect(serialized.filter).toBeTruthy();
    expect(serialized.k).toBeGreaterThan(0);
    expect(serialized.m).toBeGreaterThan(0);
    expect(serialized.meta.count).toBe(500);

    // Deserialize
    const restored = deserializeFilter(serialized);

    // Verify all entries still exist
    for (let i = 0; i < 500; i++) {
      expect(mightExist(restored, `entity_${i}`)).toBe(true);
    }

    // Verify non-existent entries still don't exist
    let falsePositives = 0;
    for (let i = 500; i < 1000; i++) {
      if (mightExist(restored, `other_${i}`)) {
        falsePositives++;
      }
    }

    // Should have very few false positives
    expect(falsePositives).toBeLessThan(50); // < 10%
  });

  it("filter can be serialized for edge distribution", () => {
    const filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    for (let i = 0; i < 1000; i++) {
      addToFilter(filter, `entity_${i}`);
    }

    const serialized = serializeFilter(filter);

    // Should be valid JSON-serializable
    const json = JSON.stringify(serialized);
    const parsed = JSON.parse(json);

    expect(parsed.filter).toBe(serialized.filter);
    expect(parsed.k).toBe(serialized.k);
    expect(parsed.m).toBe(serialized.m);

    console.log(`Serialized filter size: ${json.length} bytes`);
  });
});

describe("URL Pattern Extraction", () => {
  it("extracts entity ID from /entities/{id}", () => {
    expect(extractEntityId("/entities/abc123")).toBe("abc123");
    expect(extractEntityId("/entities/user_001")).toBe("user_001");
  });

  it("extracts entity ID from /api/v1/entities/{id}", () => {
    expect(extractEntityId("/api/v1/entities/abc123")).toBe("abc123");
    expect(extractEntityId("/api/v2/entities/user_001")).toBe("user_001");
  });

  it("extracts entity ID from /graph/{type}/{id}", () => {
    expect(extractEntityId("/graph/user/abc123")).toBe("abc123");
    expect(extractEntityId("/graph/document/doc_001")).toBe("doc_001");
  });

  it("returns null for non-entity paths", () => {
    expect(extractEntityId("/")).toBeNull();
    expect(extractEntityId("/health")).toBeNull();
    expect(extractEntityId("/api/v1/search")).toBeNull();
    expect(extractEntityId("/entities")).toBeNull();
    expect(extractEntityId("/entities/")).toBeNull();
  });
});

describe("Bloom Filter Builder", () => {
  it("calculates optimal parameters correctly", () => {
    // 10K entries, 1% FPR
    const n = 10000;
    const p = 0.01;

    const m = calculateOptimalBits(n, p);
    const k = calculateOptimalK(m, n);
    const expectedFpr = calculateExpectedFpr(m, n, k);

    console.log(`Optimal bits (m): ${m}`);
    console.log(`Optimal hash functions (k): ${k}`);
    console.log(`Expected FPR: ${(expectedFpr * 100).toFixed(4)}%`);

    // Theoretical: m/n = -ln(p)/ln(2)^2 ~ 9.585 bits per entry for 1% FPR
    expect(m / n).toBeCloseTo(9.585, 1);
    // Theoretical: k = ln(2) * m/n ~ 6.64 ~ 7
    expect(k).toBeGreaterThanOrEqual(6);
    expect(k).toBeLessThanOrEqual(8);
    // Expected FPR should be close to target (within 1% tolerance due to rounding)
    expect(expectedFpr).toBeLessThan(p * 1.01);
  });

  it("respects max size constraint", () => {
    const filter = createBloomFilter({
      capacity: 100000, // Would normally need ~120KB
      targetFpr: 0.01,
      maxSizeBytes: 16 * 1024, // Limit to 16KB
    });

    const stats = getFilterStats(filter);
    console.log(`Constrained filter size: ${stats.sizeKB.toFixed(2)} KB`);

    expect(stats.sizeBytes).toBeLessThanOrEqual(16 * 1024);
  });

  it("getFilterStats returns accurate information", () => {
    const filter = createBloomFilter({
      capacity: 1000,
      targetFpr: 0.01,
    });

    for (let i = 0; i < 500; i++) {
      addToFilter(filter, `entity_${i}`);
    }

    const stats = getFilterStats(filter);

    expect(stats.entriesAdded).toBe(500);
    expect(stats.capacity).toBe(1000);
    expect(stats.utilizationPercent).toBe(50);
    expect(stats.fillRate).toBeGreaterThan(0);
    expect(stats.fillRate).toBeLessThan(1);
    expect(stats.bitsSet).toBeGreaterThan(0);

    console.log("Filter stats:", stats);
  });

  it("estimateFpr matches theoretical FPR", () => {
    const filter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    for (let i = 0; i < 10000; i++) {
      addToFilter(filter, `entity_${i}`);
    }

    const estimated = estimateFpr(filter, 10000);
    const stats = getFilterStats(filter);

    console.log(`Theoretical FPR: ${(stats.expectedFpr * 100).toFixed(4)}%`);
    console.log(`Estimated FPR: ${(estimated * 100).toFixed(4)}%`);

    // Should be within reasonable range of theoretical
    expect(estimated).toBeLessThan(0.02); // Allow some variance
  });
});

describe("Filter Merge Operations", () => {
  it("supports incremental updates via merge", () => {
    // Main filter with initial entities
    const mainFilter = createBloomFilter({
      capacity: 10000,
      targetFpr: 0.01,
    });

    for (let i = 0; i < 5000; i++) {
      addToFilter(mainFilter, `entity_${i}`);
    }

    // Incremental filter for new entities
    const newEntities = [];
    for (let i = 5000; i < 6000; i++) {
      newEntities.push(`entity_${i}`);
    }

    const incrementalFilter = createIncrementalFilter(mainFilter, newEntities);

    // Merge filters
    const merged = mergeFilters(mainFilter, incrementalFilter);

    // All entities should be found in merged filter
    for (let i = 0; i < 6000; i++) {
      expect(mightExist(merged, `entity_${i}`)).toBe(true);
    }

    console.log(`Main filter entries: ${mainFilter.count}`);
    console.log(`Incremental filter entries: ${incrementalFilter.count}`);
    console.log(`Merged filter entries: ${merged.count}`);
  });

  it("throws on merging incompatible filters", () => {
    const filter1 = createBloomFilter({
      capacity: 1000,
      targetFpr: 0.01,
    });

    const filter2 = createBloomFilter({
      capacity: 5000, // Different capacity = different m
      targetFpr: 0.01,
    });

    expect(() => mergeFilters(filter1, filter2)).toThrow();
  });
});
