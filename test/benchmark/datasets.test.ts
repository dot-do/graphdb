/**
 * Dataset Generator Tests
 *
 * Tests for benchmark dataset generation functionality:
 * - ULID generation
 * - Dataset configurations
 * - O*NET dataset generator
 * - IMDB dataset generator
 * - Factory functions and utilities
 */

import { describe, it, expect } from 'vitest';
import {
  generateULID,
  DATASETS,
  generateONETDataset,
  generateIMDBDataset,
  getDatasetGenerator,
  estimateTripleCount,
  randomEntityId,
  type DatasetConfig,
} from '../../src/benchmark/datasets.js';
import { ObjectType } from '../../src/core/types.js';

describe('Benchmark Datasets', () => {
  // ============================================================================
  // ULID Generation Tests
  // ============================================================================

  describe('generateULID', () => {
    it('should generate a 26-character ULID', () => {
      const ulid = generateULID();
      expect(ulid).toHaveLength(26);
    });

    it('should generate ULIDs with valid Crockford Base32 characters', () => {
      const ulid = generateULID();
      // Crockford Base32 excludes I, L, O, U
      const validChars = /^[0-9A-HJKMNP-TV-Z]+$/;
      expect(ulid).toMatch(validChars);
    });

    it('should generate unique ULIDs', () => {
      const ulids = new Set<string>();
      for (let i = 0; i < 100; i++) {
        ulids.add(generateULID());
      }
      expect(ulids.size).toBe(100);
    });

    it('should generate ULIDs that sort chronologically', () => {
      const ulid1 = generateULID();
      // Small delay to ensure different timestamp
      const ulid2 = generateULID();
      // The first 10 characters are the timestamp portion
      // Due to the short time between calls, they may be the same
      // but should never be out of order
      expect(ulid1.slice(0, 10) <= ulid2.slice(0, 10)).toBe(true);
    });
  });

  // ============================================================================
  // Dataset Configuration Tests
  // ============================================================================

  describe('DATASETS', () => {
    it('should have all predefined datasets', () => {
      expect(DATASETS).toHaveProperty('tiny');
      expect(DATASETS).toHaveProperty('small');
      expect(DATASETS).toHaveProperty('medium');
      expect(DATASETS).toHaveProperty('onet');
      expect(DATASETS).toHaveProperty('imdb');
    });

    it('should have valid configuration for tiny dataset', () => {
      const tiny = DATASETS['tiny'];
      expect(tiny).toBeDefined();
      expect(tiny!.name).toBe('tiny');
      expect(tiny!.entityCount).toBe(100);
      expect(tiny!.avgTriplesPerEntity).toBe(5);
      expect(tiny!.relationshipDensity).toBeGreaterThanOrEqual(0);
      expect(tiny!.relationshipDensity).toBeLessThanOrEqual(1);
      expect(tiny!.estimatedSizeMB).toBeGreaterThan(0);
    });

    it('should have valid configuration for small dataset', () => {
      const small = DATASETS['small'];
      expect(small).toBeDefined();
      expect(small!.name).toBe('small');
      expect(small!.entityCount).toBe(1_000);
    });

    it('should have valid configuration for medium dataset', () => {
      const medium = DATASETS['medium'];
      expect(medium).toBeDefined();
      expect(medium!.name).toBe('medium');
      expect(medium!.entityCount).toBe(10_000);
    });

    it('should have valid configuration for onet dataset', () => {
      const onet = DATASETS['onet'];
      expect(onet).toBeDefined();
      expect(onet!.name).toBe('onet');
      expect(onet!.entityCount).toBe(100_000);
      expect(onet!.estimatedSizeMB).toBe(100);
    });

    it('should have valid configuration for imdb dataset', () => {
      const imdb = DATASETS['imdb'];
      expect(imdb).toBeDefined();
      expect(imdb!.name).toBe('imdb');
      expect(imdb!.entityCount).toBe(1_000_000);
      expect(imdb!.estimatedSizeMB).toBe(1000);
    });

    it('should have increasing entity counts across dataset sizes', () => {
      expect(DATASETS['tiny']!.entityCount).toBeLessThan(DATASETS['small']!.entityCount);
      expect(DATASETS['small']!.entityCount).toBeLessThan(DATASETS['medium']!.entityCount);
      expect(DATASETS['medium']!.entityCount).toBeLessThan(DATASETS['onet']!.entityCount);
      expect(DATASETS['onet']!.entityCount).toBeLessThan(DATASETS['imdb']!.entityCount);
    });
  });

  // ============================================================================
  // O*NET Dataset Generator Tests
  // ============================================================================

  describe('generateONETDataset', () => {
    const tinyConfig: DatasetConfig = DATASETS['tiny']!;

    it('should generate triples in batches', () => {
      const generator = generateONETDataset(tinyConfig, 10);
      const firstBatch = generator.next();

      expect(firstBatch.done).toBe(false);
      expect(firstBatch.value).toBeInstanceOf(Array);
      expect(firstBatch.value!.length).toBeLessThanOrEqual(10);
    });

    it('should generate valid triple structure', () => {
      const generator = generateONETDataset(tinyConfig, 100);
      const batch = generator.next().value;

      expect(batch).toBeDefined();
      expect(batch!.length).toBeGreaterThan(0);

      const triple = batch![0];
      expect(triple).toHaveProperty('subject');
      expect(triple).toHaveProperty('predicate');
      expect(triple).toHaveProperty('object');
      expect(triple).toHaveProperty('timestamp');
      expect(triple).toHaveProperty('txId');
    });

    it('should generate occupation entities', () => {
      const generator = generateONETDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find occupation type triples
      const occupationTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Occupation'
      );

      expect(occupationTriples.length).toBeGreaterThan(0);
    });

    it('should generate skill entities', () => {
      const generator = generateONETDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find skill type triples
      const skillTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Skill'
      );

      expect(skillTriples.length).toBeGreaterThan(0);
    });

    it('should generate worker entities with relationships', () => {
      const generator = generateONETDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find worker type triples
      const workerTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Person'
      );

      expect(workerTriples.length).toBeGreaterThan(0);

      // Find hasOccupation relationships
      const hasOccupationTriples = allTriples.filter(
        (t) => t.predicate === 'hasOccupation' && t.object.type === ObjectType.REF
      );

      expect(hasOccupationTriples.length).toBeGreaterThan(0);
    });

    it('should generate geo point data for workers', () => {
      const generator = generateONETDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find location triples
      const locationTriples = allTriples.filter(
        (t) => t.predicate === 'location' && t.object.type === ObjectType.GEO_POINT
      );

      expect(locationTriples.length).toBeGreaterThan(0);
    });

    it('should generate numeric data (salary, years experience)', () => {
      const generator = generateONETDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find salary triples
      const salaryTriples = allTriples.filter(
        (t) =>
          (t.predicate === 'salaryMin' || t.predicate === 'salaryMax') &&
          t.object.type === ObjectType.INT64
      );

      expect(salaryTriples.length).toBeGreaterThan(0);

      // Find years experience triples
      const yearsExpTriples = allTriples.filter(
        (t) => t.predicate === 'yearsExperience' && t.object.type === ObjectType.INT32
      );

      expect(yearsExpTriples.length).toBeGreaterThan(0);
    });
  });

  // ============================================================================
  // IMDB Dataset Generator Tests
  // ============================================================================

  describe('generateIMDBDataset', () => {
    const tinyConfig: DatasetConfig = DATASETS['tiny']!;

    it('should generate triples in batches', () => {
      const generator = generateIMDBDataset(tinyConfig, 10);
      const firstBatch = generator.next();

      expect(firstBatch.done).toBe(false);
      expect(firstBatch.value).toBeInstanceOf(Array);
    });

    it('should generate movie entities', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find movie type triples
      const movieTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Movie'
      );

      expect(movieTriples.length).toBeGreaterThan(0);
    });

    it('should generate actor entities', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find actor type triples
      const actorTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Actor'
      );

      expect(actorTriples.length).toBeGreaterThan(0);
    });

    it('should generate director entities', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find director type triples
      const directorTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Director'
      );

      expect(directorTriples.length).toBeGreaterThan(0);
    });

    it('should generate studio entities', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number; value?: string } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find studio type triples
      const studioTriples = allTriples.filter(
        (t) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Studio'
      );

      expect(studioTriples.length).toBeGreaterThan(0);
    });

    it('should generate movie relationships (directedBy, starring)', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find directedBy relationships
      const directedByTriples = allTriples.filter(
        (t) => t.predicate === 'directedBy' && t.object.type === ObjectType.REF
      );

      expect(directedByTriples.length).toBeGreaterThan(0);

      // Find starring relationships
      const starringTriples = allTriples.filter(
        (t) => t.predicate === 'starring' && t.object.type === ObjectType.REF
      );

      expect(starringTriples.length).toBeGreaterThan(0);
    });

    it('should generate reverse relationships (actedIn)', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      // Find actedIn reverse relationships
      const actedInTriples = allTriples.filter(
        (t) => t.predicate === 'actedIn' && t.object.type === ObjectType.REF
      );

      expect(actedInTriples.length).toBeGreaterThan(0);
    });

    it('should generate movie metadata (year, genre, rating, runtime)', () => {
      const generator = generateIMDBDataset(tinyConfig, 1000);
      const allTriples: Array<{ subject: string; predicate: string; object: { type: number } }> = [];

      for (const batch of generator) {
        allTriples.push(...batch);
      }

      expect(allTriples.filter((t) => t.predicate === 'year').length).toBeGreaterThan(0);
      expect(allTriples.filter((t) => t.predicate === 'genre').length).toBeGreaterThan(0);
      expect(allTriples.filter((t) => t.predicate === 'rating').length).toBeGreaterThan(0);
      expect(allTriples.filter((t) => t.predicate === 'runtime').length).toBeGreaterThan(0);
    });
  });

  // ============================================================================
  // Factory Function Tests
  // ============================================================================

  describe('getDatasetGenerator', () => {
    it('should return ONET generator for tiny dataset', () => {
      const generator = getDatasetGenerator('tiny');
      const batch = generator.next().value;

      expect(batch).toBeDefined();
      expect(batch!.length).toBeGreaterThan(0);
    });

    it('should return ONET generator for small dataset', () => {
      const generator = getDatasetGenerator('small', 100);
      const batch = generator.next().value;

      expect(batch).toBeDefined();
    });

    it('should return IMDB generator for imdb dataset', () => {
      // Create a small imdb-like config for testing
      const generator = getDatasetGenerator('imdb', 10);
      const batch = generator.next().value;

      expect(batch).toBeDefined();
      // IMDB dataset generates studios first, check for studio URL
      const hasStudioType = batch!.some(
        (t: { object: { type: number; value?: string } }) =>
          t.object.type === ObjectType.URL &&
          t.object.value === 'https://schema.workers.do/Studio'
      );
      expect(hasStudioType).toBe(true);
    });

    it('should throw error for unknown dataset', () => {
      expect(() => getDatasetGenerator('nonexistent')).toThrow('Unknown dataset: nonexistent');
    });

    it('should include available datasets in error message', () => {
      try {
        getDatasetGenerator('invalid');
      } catch (e) {
        expect((e as Error).message).toContain('tiny');
        expect((e as Error).message).toContain('small');
        expect((e as Error).message).toContain('medium');
      }
    });
  });

  // ============================================================================
  // Utility Function Tests
  // ============================================================================

  describe('estimateTripleCount', () => {
    it('should return positive count for valid datasets', () => {
      expect(estimateTripleCount('tiny')).toBeGreaterThan(0);
      expect(estimateTripleCount('small')).toBeGreaterThan(0);
      expect(estimateTripleCount('medium')).toBeGreaterThan(0);
    });

    it('should return 0 for unknown dataset', () => {
      expect(estimateTripleCount('nonexistent')).toBe(0);
    });

    it('should calculate based on entityCount, avgTriplesPerEntity, and relationshipDensity', () => {
      const tiny = DATASETS['tiny']!;
      const expected = Math.floor(
        tiny.entityCount * tiny.avgTriplesPerEntity * (1 + tiny.relationshipDensity)
      );
      expect(estimateTripleCount('tiny')).toBe(expected);
    });

    it('should return larger counts for larger datasets', () => {
      expect(estimateTripleCount('tiny')).toBeLessThan(estimateTripleCount('small'));
      expect(estimateTripleCount('small')).toBeLessThan(estimateTripleCount('medium'));
      expect(estimateTripleCount('medium')).toBeLessThan(estimateTripleCount('onet'));
    });
  });

  describe('randomEntityId', () => {
    it('should generate valid occupation entity ID', () => {
      const id = randomEntityId('tiny', 'occupation');
      expect(id).toContain('https://graph.workers.do/occupation/');
    });

    it('should generate valid skill entity ID', () => {
      const id = randomEntityId('tiny', 'skill');
      expect(id).toContain('https://graph.workers.do/skill/');
    });

    it('should generate valid worker entity ID', () => {
      const id = randomEntityId('tiny', 'worker');
      expect(id).toContain('https://graph.workers.do/worker/');
    });

    it('should generate valid movie entity ID', () => {
      const id = randomEntityId('tiny', 'movie');
      expect(id).toContain('https://graph.workers.do/movie/');
    });

    it('should generate valid actor entity ID', () => {
      const id = randomEntityId('tiny', 'actor');
      expect(id).toContain('https://graph.workers.do/actor/');
    });

    it('should generate valid director entity ID', () => {
      const id = randomEntityId('tiny', 'director');
      expect(id).toContain('https://graph.workers.do/director/');
    });

    it('should generate valid studio entity ID', () => {
      const id = randomEntityId('tiny', 'studio');
      expect(id).toContain('https://graph.workers.do/studio/');
    });

    it('should throw error for unknown dataset', () => {
      expect(() => randomEntityId('nonexistent', 'worker')).toThrow('Unknown dataset: nonexistent');
    });

    it('should generate IDs within valid range based on entity distribution', () => {
      // For tiny dataset with 100 entities:
      // - occupations: 30% = 30
      // - skills: 20% = 20
      // - workers: 50% = 50
      const occupationIds = new Set<string>();
      for (let i = 0; i < 100; i++) {
        occupationIds.add(randomEntityId('tiny', 'occupation'));
      }

      // All generated IDs should have indices less than 30
      for (const id of occupationIds) {
        const index = parseInt(id.split('/').pop()!);
        expect(index).toBeLessThan(30);
      }
    });
  });
});
