/**
 * Benchmark Dataset Generators for GraphDB
 *
 * Generates realistic test data at various scales:
 * - O*NET scale (~100MB): ~1M triples, occupation/skill knowledge graph
 * - IMDB scale (~1GB): ~10M triples, movie/actor/director relationships
 *
 * Data model uses URL-based identifiers and JS-native field names.
 */

import type { EntityId, TransactionId } from '../core/types.js';
import { ObjectType, createEntityId, createPredicate } from '../core/types.js';
import type { Triple } from '../core/triple.js';

// ============================================================================
// ULID Generator (simplified for benchmarks)
// ============================================================================

const ENCODING = '0123456789ABCDEFGHJKMNPQRSTVWXYZ';

function encodeTime(now: number): string {
  let str = '';
  for (let i = 0; i < 10; i++) {
    str = ENCODING[now % 32] + str;
    now = Math.floor(now / 32);
  }
  return str;
}

function encodeRandom(): string {
  let str = '';
  for (let i = 0; i < 16; i++) {
    str += ENCODING[Math.floor(Math.random() * 32)];
  }
  return str;
}

export function generateULID(): TransactionId {
  return (encodeTime(Date.now()) + encodeRandom()) as TransactionId;
}

// ============================================================================
// Dataset Types
// ============================================================================

export interface DatasetConfig {
  name: string;
  entityCount: number;
  avgTriplesPerEntity: number;
  relationshipDensity: number; // 0-1, how interconnected entities are
  estimatedSizeMB: number;
}

export interface GeneratorProgress {
  totalEntities: number;
  generatedEntities: number;
  totalTriples: number;
  generatedTriples: number;
  percentComplete: number;
  elapsedMs: number;
}

export type ProgressCallback = (progress: GeneratorProgress) => void;

// ============================================================================
// Predefined Dataset Configurations
// ============================================================================

export const DATASETS: Record<string, DatasetConfig> = {
  // Small test dataset
  tiny: {
    name: 'tiny',
    entityCount: 100,
    avgTriplesPerEntity: 5,
    relationshipDensity: 0.1,
    estimatedSizeMB: 0.05,
  },

  // Small scale for quick tests
  small: {
    name: 'small',
    entityCount: 1_000,
    avgTriplesPerEntity: 8,
    relationshipDensity: 0.15,
    estimatedSizeMB: 1,
  },

  // Medium scale (~10MB)
  medium: {
    name: 'medium',
    entityCount: 10_000,
    avgTriplesPerEntity: 10,
    relationshipDensity: 0.2,
    estimatedSizeMB: 10,
  },

  // O*NET scale (~100MB) - occupation/skill knowledge graph
  onet: {
    name: 'onet',
    entityCount: 100_000,
    avgTriplesPerEntity: 10,
    relationshipDensity: 0.25,
    estimatedSizeMB: 100,
  },

  // IMDB scale (~1GB) - movie/actor/director graph
  imdb: {
    name: 'imdb',
    entityCount: 1_000_000,
    avgTriplesPerEntity: 10,
    relationshipDensity: 0.3,
    estimatedSizeMB: 1000,
  },
};

// ============================================================================
// O*NET-like Dataset Generator (Occupations & Skills)
// ============================================================================

const ONET_OCCUPATIONS = [
  'Software Developer',
  'Data Scientist',
  'DevOps Engineer',
  'Product Manager',
  'UX Designer',
  'Database Administrator',
  'Security Analyst',
  'Cloud Architect',
  'Machine Learning Engineer',
  'Backend Developer',
  'Frontend Developer',
  'Full Stack Developer',
  'Site Reliability Engineer',
  'Systems Administrator',
  'Network Engineer',
  'Technical Writer',
  'QA Engineer',
  'Scrum Master',
  'Technical Lead',
  'Engineering Manager',
];

const ONET_SKILLS = [
  'JavaScript',
  'TypeScript',
  'Python',
  'Go',
  'Rust',
  'SQL',
  'GraphQL',
  'REST APIs',
  'Docker',
  'Kubernetes',
  'AWS',
  'GCP',
  'Azure',
  'PostgreSQL',
  'MongoDB',
  'Redis',
  'Kafka',
  'Git',
  'CI/CD',
  'Terraform',
  'Linux',
  'Networking',
  'Security',
  'Agile',
  'Communication',
];

const ONET_INDUSTRIES = [
  'Technology',
  'Finance',
  'Healthcare',
  'E-commerce',
  'Gaming',
  'Education',
  'Government',
  'Consulting',
  'Startup',
  'Enterprise',
];

/**
 * Generate O*NET-like occupation/skill knowledge graph
 */
export function* generateONETDataset(
  config: DatasetConfig,
  batchSize: number = 1000
): Generator<Triple[], void, void> {
  const txId = generateULID();
  const timestamp = BigInt(Date.now());
  let batch: Triple[] = [];
  let tripleCount = 0;

  // Generate occupations
  const occupationCount = Math.floor(config.entityCount * 0.3);
  for (let i = 0; i < occupationCount; i++) {
    const occupationName = ONET_OCCUPATIONS[i % ONET_OCCUPATIONS.length];
    const occupationId = createEntityId(`https://graph.workers.do/occupation/${i}`);

    // Name triple
    batch.push({
      subject: occupationId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `${occupationName} ${i}` },
      timestamp,
      txId,
    });
    tripleCount++;

    // Type triple
    batch.push({
      subject: occupationId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Occupation' },
      timestamp,
      txId,
    });
    tripleCount++;

    // Industry triple
    batch.push({
      subject: occupationId,
      predicate: createPredicate('industry'),
      object: { type: ObjectType.STRING, value: ONET_INDUSTRIES[i % ONET_INDUSTRIES.length]! },
      timestamp,
      txId,
    });
    tripleCount++;

    // Salary range
    batch.push({
      subject: occupationId,
      predicate: createPredicate('salaryMin'),
      object: { type: ObjectType.INT64, value: BigInt(50000 + (i % 100) * 1000) },
      timestamp,
      txId,
    });
    tripleCount++;

    batch.push({
      subject: occupationId,
      predicate: createPredicate('salaryMax'),
      object: { type: ObjectType.INT64, value: BigInt(100000 + (i % 100) * 2000) },
      timestamp,
      txId,
    });
    tripleCount++;

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate skills
  const skillCount = Math.floor(config.entityCount * 0.2);
  for (let i = 0; i < skillCount; i++) {
    const skillName = ONET_SKILLS[i % ONET_SKILLS.length];
    const skillId = createEntityId(`https://graph.workers.do/skill/${i}`);

    batch.push({
      subject: skillId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `${skillName} ${Math.floor(i / ONET_SKILLS.length)}` },
      timestamp,
      txId,
    });
    tripleCount++;

    batch.push({
      subject: skillId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Skill' },
      timestamp,
      txId,
    });
    tripleCount++;

    batch.push({
      subject: skillId,
      predicate: createPredicate('category'),
      object: { type: ObjectType.STRING, value: i % 2 === 0 ? 'Technical' : 'Soft' },
      timestamp,
      txId,
    });
    tripleCount++;

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate workers (people)
  const workerCount = Math.floor(config.entityCount * 0.5);
  for (let i = 0; i < workerCount; i++) {
    const workerId = createEntityId(`https://graph.workers.do/worker/${i}`);

    batch.push({
      subject: workerId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `Worker ${i}` },
      timestamp,
      txId,
    });
    tripleCount++;

    batch.push({
      subject: workerId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Person' },
      timestamp,
      txId,
    });
    tripleCount++;

    batch.push({
      subject: workerId,
      predicate: createPredicate('email'),
      object: { type: ObjectType.STRING, value: `worker${i}@example.com` },
      timestamp,
      txId,
    });
    tripleCount++;

    // hasOccupation relationship
    const occIndex = i % occupationCount;
    batch.push({
      subject: workerId,
      predicate: createPredicate('hasOccupation'),
      object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/occupation/${occIndex}`) },
      timestamp,
      txId,
    });
    tripleCount++;

    // hasSkill relationships (2-5 skills per worker based on density)
    const numSkills = 2 + Math.floor(config.relationshipDensity * 5);
    for (let s = 0; s < numSkills; s++) {
      const skillIndex = (i + s * 7) % skillCount; // Distribute skills
      batch.push({
        subject: workerId,
        predicate: createPredicate('hasSkill'),
        object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/skill/${skillIndex}`) },
        timestamp,
        txId,
      });
      tripleCount++;
    }

    // worksAt relationship (company)
    batch.push({
      subject: workerId,
      predicate: createPredicate('worksAt'),
      object: { type: ObjectType.STRING, value: `Company ${i % 100}` },
      timestamp,
      txId,
    });
    tripleCount++;

    // yearsExperience
    batch.push({
      subject: workerId,
      predicate: createPredicate('yearsExperience'),
      object: { type: ObjectType.INT32, value: BigInt(1 + (i % 20)) },
      timestamp,
      txId,
    });
    tripleCount++;

    // location (geo point)
    batch.push({
      subject: workerId,
      predicate: createPredicate('location'),
      object: {
        type: ObjectType.GEO_POINT,
        value: {
          lat: 37.7749 + (i % 100) * 0.01,
          lng: -122.4194 + (i % 100) * 0.01,
        },
      },
      timestamp,
      txId,
    });
    tripleCount++;

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate occupation-skill relationships
  for (let o = 0; o < occupationCount; o++) {
    const occupationId = createEntityId(`https://graph.workers.do/occupation/${o}`);
    const numSkills = 3 + Math.floor(config.relationshipDensity * 8);

    for (let s = 0; s < numSkills; s++) {
      const skillIndex = (o * 3 + s) % skillCount;
      batch.push({
        subject: occupationId,
        predicate: createPredicate('requiresSkill'),
        object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/skill/${skillIndex}`) },
        timestamp,
        txId,
      });
      tripleCount++;

      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }
  }

  // Yield remaining
  if (batch.length > 0) {
    yield batch;
  }
}

// ============================================================================
// IMDB-like Dataset Generator (Movies, Actors, Directors)
// ============================================================================

const GENRES = [
  'Action',
  'Comedy',
  'Drama',
  'Horror',
  'Sci-Fi',
  'Romance',
  'Thriller',
  'Documentary',
  'Animation',
  'Adventure',
];

const FIRST_NAMES = [
  'James',
  'Mary',
  'John',
  'Patricia',
  'Robert',
  'Jennifer',
  'Michael',
  'Linda',
  'William',
  'Elizabeth',
  'David',
  'Barbara',
  'Richard',
  'Susan',
  'Joseph',
  'Jessica',
  'Thomas',
  'Sarah',
  'Charles',
  'Karen',
];

const LAST_NAMES = [
  'Smith',
  'Johnson',
  'Williams',
  'Brown',
  'Jones',
  'Garcia',
  'Miller',
  'Davis',
  'Rodriguez',
  'Martinez',
  'Hernandez',
  'Lopez',
  'Gonzalez',
  'Wilson',
  'Anderson',
  'Thomas',
  'Taylor',
  'Moore',
  'Jackson',
  'Martin',
];

/**
 * Generate IMDB-like movie/actor/director graph
 */
export function* generateIMDBDataset(
  config: DatasetConfig,
  batchSize: number = 1000
): Generator<Triple[], void, void> {
  const txId = generateULID();
  const timestamp = BigInt(Date.now());
  let batch: Triple[] = [];

  // Entity distribution: 20% movies, 60% actors, 10% directors, 10% studios
  const movieCount = Math.floor(config.entityCount * 0.2);
  const actorCount = Math.floor(config.entityCount * 0.6);
  const directorCount = Math.floor(config.entityCount * 0.1);
  const studioCount = Math.floor(config.entityCount * 0.1);

  // Generate studios
  for (let i = 0; i < studioCount; i++) {
    const studioId = createEntityId(`https://graph.workers.do/studio/${i}`);

    batch.push({
      subject: studioId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `Studio ${i}` },
      timestamp,
      txId,
    });

    batch.push({
      subject: studioId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Studio' },
      timestamp,
      txId,
    });

    batch.push({
      subject: studioId,
      predicate: createPredicate('founded'),
      object: { type: ObjectType.INT32, value: BigInt(1920 + (i % 100)) },
      timestamp,
      txId,
    });

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate directors
  for (let i = 0; i < directorCount; i++) {
    const directorId = createEntityId(`https://graph.workers.do/director/${i}`);
    const firstName = FIRST_NAMES[i % FIRST_NAMES.length];
    const lastName = LAST_NAMES[(i * 7) % LAST_NAMES.length];

    batch.push({
      subject: directorId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `${firstName} ${lastName}` },
      timestamp,
      txId,
    });

    batch.push({
      subject: directorId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Director' },
      timestamp,
      txId,
    });

    batch.push({
      subject: directorId,
      predicate: createPredicate('birthYear'),
      object: { type: ObjectType.INT32, value: BigInt(1940 + (i % 60)) },
      timestamp,
      txId,
    });

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate actors
  for (let i = 0; i < actorCount; i++) {
    const actorId = createEntityId(`https://graph.workers.do/actor/${i}`);
    const firstName = FIRST_NAMES[i % FIRST_NAMES.length];
    const lastName = LAST_NAMES[(i * 3) % LAST_NAMES.length];

    batch.push({
      subject: actorId,
      predicate: createPredicate('name'),
      object: { type: ObjectType.STRING, value: `${firstName} ${lastName} ${Math.floor(i / 400)}` },
      timestamp,
      txId,
    });

    batch.push({
      subject: actorId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Actor' },
      timestamp,
      txId,
    });

    batch.push({
      subject: actorId,
      predicate: createPredicate('birthYear'),
      object: { type: ObjectType.INT32, value: BigInt(1950 + (i % 50)) },
      timestamp,
      txId,
    });

    batch.push({
      subject: actorId,
      predicate: createPredicate('nationality'),
      object: { type: ObjectType.STRING, value: ['USA', 'UK', 'France', 'Germany', 'Japan'][i % 5]! },
      timestamp,
      txId,
    });

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Generate movies with relationships
  for (let i = 0; i < movieCount; i++) {
    const movieId = createEntityId(`https://graph.workers.do/movie/${i}`);

    batch.push({
      subject: movieId,
      predicate: createPredicate('title'),
      object: { type: ObjectType.STRING, value: `Movie Title ${i}` },
      timestamp,
      txId,
    });

    batch.push({
      subject: movieId,
      predicate: createPredicate('$type'),
      object: { type: ObjectType.URL, value: 'https://schema.workers.do/Movie' },
      timestamp,
      txId,
    });

    batch.push({
      subject: movieId,
      predicate: createPredicate('year'),
      object: { type: ObjectType.INT32, value: BigInt(1970 + (i % 55)) },
      timestamp,
      txId,
    });

    batch.push({
      subject: movieId,
      predicate: createPredicate('genre'),
      object: { type: ObjectType.STRING, value: GENRES[i % GENRES.length]! },
      timestamp,
      txId,
    });

    batch.push({
      subject: movieId,
      predicate: createPredicate('rating'),
      object: { type: ObjectType.FLOAT64, value: 5.0 + (i % 50) * 0.1 },
      timestamp,
      txId,
    });

    batch.push({
      subject: movieId,
      predicate: createPredicate('runtime'),
      object: { type: ObjectType.INT32, value: BigInt(80 + (i % 100)) },
      timestamp,
      txId,
    });

    // Director relationship
    const directorIndex = i % directorCount;
    batch.push({
      subject: movieId,
      predicate: createPredicate('directedBy'),
      object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/director/${directorIndex}`) },
      timestamp,
      txId,
    });

    // Studio relationship
    const studioIndex = i % studioCount;
    batch.push({
      subject: movieId,
      predicate: createPredicate('producedBy'),
      object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/studio/${studioIndex}`) },
      timestamp,
      txId,
    });

    // Cast relationships (3-10 actors per movie based on density)
    const numActors = 3 + Math.floor(config.relationshipDensity * 10);
    for (let a = 0; a < numActors; a++) {
      const actorIndex = (i * 5 + a * 17) % actorCount;
      batch.push({
        subject: movieId,
        predicate: createPredicate('starring'),
        object: { type: ObjectType.REF, value: createEntityId(`https://graph.workers.do/actor/${actorIndex}`) },
        timestamp,
        txId,
      });
    }

    if (batch.length >= batchSize) {
      yield batch;
      batch = [];
    }
  }

  // Add reverse relationships: actor -> actedIn -> movie
  for (let m = 0; m < movieCount; m++) {
    const numActors = 3 + Math.floor(config.relationshipDensity * 10);
    for (let a = 0; a < numActors; a++) {
      const actorIndex = (m * 5 + a * 17) % actorCount;
      const actorId = createEntityId(`https://graph.workers.do/actor/${actorIndex}`);
      const movieId = createEntityId(`https://graph.workers.do/movie/${m}`);

      batch.push({
        subject: actorId,
        predicate: createPredicate('actedIn'),
        object: { type: ObjectType.REF, value: movieId },
        timestamp,
        txId,
      });

      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }
  }

  // Yield remaining
  if (batch.length > 0) {
    yield batch;
  }
}

// ============================================================================
// Generic Dataset Generator Factory
// ============================================================================

export function getDatasetGenerator(
  datasetName: string,
  batchSize: number = 1000
): Generator<Triple[], void, void> {
  const config = DATASETS[datasetName];
  if (!config) {
    throw new Error(`Unknown dataset: ${datasetName}. Available: ${Object.keys(DATASETS).join(', ')}`);
  }

  if (datasetName === 'imdb') {
    return generateIMDBDataset(config, batchSize);
  }

  // Default to O*NET-style for all other datasets
  return generateONETDataset(config, batchSize);
}

/**
 * Get estimated triple count for a dataset
 */
export function estimateTripleCount(datasetName: string): number {
  const config = DATASETS[datasetName];
  if (!config) {
    return 0;
  }
  return Math.floor(config.entityCount * config.avgTriplesPerEntity * (1 + config.relationshipDensity));
}

/**
 * Generate a random entity ID for benchmark lookups
 */
export function randomEntityId(datasetName: string, entityType: 'occupation' | 'skill' | 'worker' | 'movie' | 'actor' | 'director' | 'studio'): EntityId {
  const config = DATASETS[datasetName];
  if (!config) {
    throw new Error(`Unknown dataset: ${datasetName}`);
  }

  let maxIndex: number;
  switch (entityType) {
    case 'occupation':
      maxIndex = Math.floor(config.entityCount * 0.3);
      break;
    case 'skill':
      maxIndex = Math.floor(config.entityCount * 0.2);
      break;
    case 'worker':
      maxIndex = Math.floor(config.entityCount * 0.5);
      break;
    case 'movie':
      maxIndex = Math.floor(config.entityCount * 0.2);
      break;
    case 'actor':
      maxIndex = Math.floor(config.entityCount * 0.6);
      break;
    case 'director':
      maxIndex = Math.floor(config.entityCount * 0.1);
      break;
    case 'studio':
      maxIndex = Math.floor(config.entityCount * 0.1);
      break;
    default:
      maxIndex = config.entityCount;
  }

  const index = Math.floor(Math.random() * maxIndex);
  return createEntityId(`https://graph.workers.do/${entityType}/${index}`);
}
