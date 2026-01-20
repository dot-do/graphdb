# Data Loaders

Cloudflare Worker scripts for loading external datasets into GraphDB.

## O*NET Loader

Loads occupational data from the U.S. Department of Labor's O*NET database.

### Data Source

- URL: https://www.onetcenter.org/dl_files/database/db_29_1_text.zip
- Size: ~20MB compressed
- Format: Tab-separated values (TSV)

### Entity Model

```
Occupation (https://onet.org/occupation/{code})
  - code: STRING (O*NET-SOC code, e.g., "11-1011.00")
  - title: STRING (e.g., "Chief Executives")
  - description: STRING

Skill (https://onet.org/skill/{elementId})
  - elementId: STRING
  - name: STRING

Ability (https://onet.org/ability/{elementId})
  - elementId: STRING
  - name: STRING

Knowledge (https://onet.org/knowledge/{elementId})
  - elementId: STRING
  - name: STRING

SkillRating (https://onet.org/relation/{occupationCode}_skill_{elementId}_{scaleId})
  - occupation: REF -> Occupation
  - element: REF -> Skill
  - scaleId: STRING (IM = Importance, LV = Level)
  - dataValue: FLOAT64 (0-7 scale)
```

### Relations

Occupations have relations to skills, abilities, and knowledge:

- `requiresSkill`: REF from Occupation to Skill
- `requiresAbility`: REF from Occupation to Ability
- `requiresKnowledge`: REF from Occupation to Knowledge

### R2 Output Structure

```
datasets/onet/
  chunks/
    chunk_0000.gcol
    chunk_0001.gcol
    ...
  bloom/
    filter.json
  index.json
```

### Usage

#### Deploy the Loader

```bash
# From packages/graphdb directory
cd scripts/loaders
wrangler deploy --config wrangler.onet.jsonc
```

#### Trigger the Load

```bash
# Check current status
curl https://onet-loader.<your-subdomain>.workers.dev/status

# Trigger a fresh load (POST required)
curl -X POST https://onet-loader.<your-subdomain>.workers.dev/load
```

#### Local Development

```bash
# Run locally with R2 binding
wrangler dev --config wrangler.onet.jsonc
```

### Expected Output

After a successful load:

- ~1000 occupations
- ~35 skills
- ~52 abilities
- ~33 knowledge areas
- ~200,000+ triples
- ~20 GraphCol chunks

### Index Schema

The `index.json` file contains:

```typescript
interface LoaderIndex {
  version: string;        // O*NET version (e.g., "29.1")
  source: string;         // Source URL
  loadedAt: string;       // ISO timestamp
  namespace: string;      // "https://onet.org"
  stats: {
    occupations: number;
    skills: number;
    abilities: number;
    knowledge: number;
    totalTriples: number;
    totalChunks: number;
  };
  chunks: Array<{
    path: string;         // R2 key path
    tripleCount: number;
    sizeBytes: number;
  }>;
  bloom: {
    path: string;         // R2 key for bloom filter
    entityCount: number;
  };
}
```

### Query Examples

After loading, you can query the data through GraphDB:

```graphql
# Find an occupation by code
{
  occupation(id: "https://onet.org/occupation/11-1011.00") {
    title
    description
    requiresSkill {
      name
    }
  }
}

# Find occupations requiring a specific skill
{
  skillRatings(filter: { element: "https://onet.org/skill/2.A.1.a" }) {
    occupation {
      title
    }
    dataValue
  }
}
```

### Implementation Notes

1. **ZIP Extraction**: Uses native `DecompressionStream` for deflate decompression
2. **TSV Parsing**: Custom parser handling quoted fields and UTF-8 encoding
3. **Chunking**: 10,000 triples per GraphCol chunk for efficient streaming
4. **Bloom Filter**: 1% false positive rate, sized for all entity IDs
5. **Transaction ID**: Single ULID-style ID for the entire load operation

---

## IMDB Loader

Streams IMDB movie database directly from source to R2 as GraphCol chunks.

### Data Sources

| Dataset | URL | Description |
|---------|-----|-------------|
| title.basics.tsv.gz | https://datasets.imdbws.com/title.basics.tsv.gz | Movie/TV metadata |
| title.ratings.tsv.gz | https://datasets.imdbws.com/title.ratings.tsv.gz | Ratings and votes |
| name.basics.tsv.gz | https://datasets.imdbws.com/name.basics.tsv.gz | Person metadata |
| title.principals.tsv.gz | https://datasets.imdbws.com/title.principals.tsv.gz | Cast/crew relations |

**Total size**: ~2GB compressed, ~7GB uncompressed

### Entity Model

```
Movie (https://imdb.com/title/{tconst})
  - $type: STRING ("Movie")
  - title: STRING (e.g., "The Shawshank Redemption")
  - year: INT32 (release year)
  - runtime: INT32 (minutes)
  - genres: STRING (comma-separated)
  - rating: FLOAT64 (0-10 scale)

Person (https://imdb.com/name/{nconst})
  - $type: STRING ("Person")
  - name: STRING (e.g., "Morgan Freeman")
  - birthYear: INT32
  - profession: STRING (comma-separated)
```

### Relations

- `starring`: REF from Movie to Person (actors/actresses)
- `directedBy`: REF from Movie to Person (directors)

### R2 Output Structure

```
datasets/imdb/
  chunks/
    chunk_000000.graphcol
    chunk_000001.graphcol
    ...
  bloom/
    chunk_000000.bloom
    chunk_000001.bloom
    master.bloom
  index.json
```

### Usage

#### Local Development

```bash
# From packages/graphdb directory
wrangler dev scripts/loaders/imdb-loader.ts -c scripts/loaders/wrangler.toml
```

#### Deploy the Loader

```bash
wrangler deploy -c scripts/loaders/wrangler.toml --env production
```

#### Trigger Loads

```bash
# Get available endpoints
curl http://localhost:8787/

# Load all datasets (takes several minutes)
curl http://localhost:8787/load

# Load individual datasets
curl http://localhost:8787/load/title-basics
curl http://localhost:8787/load/title-ratings
curl http://localhost:8787/load/name-basics
curl http://localhost:8787/load/title-principals

# Check current status/manifest
curl http://localhost:8787/status
```

### Expected Output

After a successful full load:

- ~10M movies/TV shows (filtered to movies/tvMovie/tvSeries)
- ~13M people (actors, directors, etc.)
- ~60M+ cast/crew relations
- ~100M+ triples total
- ~2000+ GraphCol chunks (~2MB each)

### Index Schema

```typescript
interface ChunkManifest {
  version: 1;
  namespace: "https://imdb.com/";
  datasets: Array<{
    name: string;
    url: string;
    status: "pending" | "processing" | "completed" | "error";
    chunks: string[];
    tripleCount: number;
    entityCount: number;
    error?: string;
    startedAt?: string;
    completedAt?: string;
  }>;
  chunks: Array<{
    id: string;
    path: string;
    bloomPath: string;
    tripleCount: number;
    sizeBytes: number;
    predicates: string[];
    createdAt: string;
  }>;
  stats: {
    totalTriples: number;
    totalChunks: number;
    totalEntities: number;
    totalSizeBytes: number;
  };
  createdAt: string;
  updatedAt: string;
}
```

### Query Examples

After loading, query through GraphDB:

```graphql
# Find a movie by ID
{
  movie(id: "https://imdb.com/title/tt0111161") {
    title
    year
    rating
    genres
    starring {
      name
    }
    directedBy {
      name
    }
  }
}

# Find movies with an actor
{
  movies(filter: { starring: "https://imdb.com/name/nm0000151" }) {
    title
    year
    rating
  }
}
```

### Implementation Notes

1. **Streaming Pipeline**: gzip -> text -> TSV parse -> triple generation -> chunking
2. **DecompressionStream**: Native browser/Workers API for gzip decompression
3. **TransformStream**: Custom TSV parser as web stream transform
4. **Chunking**: 50,000 triples per GraphCol chunk (~2MB target size)
5. **Bloom Filter**: Per-chunk filters + master filter for entity lookup
6. **Progress Logging**: Every 100K lines processed
7. **Memory Efficiency**: Never buffers entire dataset - true streaming

---

## Wiktionary Loader

Streams pre-parsed Wiktionary dictionary data from kaikki.org to R2 as GraphCol chunks.

### Data Source

- URL: https://kaikki.org/dictionary/English/by-pos-all.json
- Size: ~300MB (NDJSON format)
- Format: Newline-delimited JSON (one word entry per line)
- Content: Pre-parsed English Wiktionary with senses, etymologies, pronunciations

### Entity Model

```
WordEntry (https://wiktionary.org/word/{word}/{pos}/{idx})
  - word: STRING (the word itself, e.g., "dictionary")
  - partOfSpeech: STRING (noun, verb, adj, adv, etc.)
  - senses: JSON (array of definitions with glosses and tags)
  - sounds: JSON (IPA pronunciations, audio file refs)
  - etymology: STRING (word origin text)
  - forms: JSON (inflections - plural, past tense, etc.)
  - categories: JSON (Wiktionary categories)
  - relatedWords: JSON (array of related word strings)
  - derivedWords: JSON (array of derived word strings)
```

### R2 Output Structure

```
datasets/wiktionary/
  chunks/
    chunk_000000.gcol
    chunk_000001.gcol
    ...
  bloom/
    filter.json
  index.json
```

### Usage

#### Deploy the Loader

```bash
# From packages/graphdb directory
cd scripts/loaders
wrangler deploy --config wrangler.wiktionary.jsonc
```

#### Trigger the Load

```bash
# Check current status
curl https://wiktionary-loader.<your-subdomain>.workers.dev/status

# Trigger a fresh load (POST required)
curl -X POST https://wiktionary-loader.<your-subdomain>.workers.dev/load
```

#### Local Development

```bash
# Run locally with R2 binding
wrangler dev --config wrangler.wiktionary.jsonc
```

### Expected Output

After a successful load:

- ~1M word entries
- ~5M triples
- ~100 GraphCol chunks (~2MB each)
- ~2M entities in bloom filter

### Index Schema

```typescript
interface LoaderIndex {
  version: string;           // "1.0"
  source: string;            // Source URL (kaikki.org)
  loadedAt: string;          // ISO timestamp
  namespace: string;         // "https://wiktionary.org"
  stats: {
    totalEntries: number;    // Word entries processed
    totalTriples: number;    // Total triples generated
    totalChunks: number;     // GraphCol chunks
    byPartOfSpeech: Record<string, number>;  // Counts per POS
  };
  chunks: Array<{
    path: string;            // R2 key path
    tripleCount: number;
    sizeBytes: number;
  }>;
  bloom: {
    path: string;            // R2 key for bloom filter
    entityCount: number;
  };
}
```

### Query Examples

After loading, you can query the data through GraphDB:

```graphql
# Find a word entry
{
  word(id: "https://wiktionary.org/word/dictionary/noun/0") {
    word
    partOfSpeech
    senses
    etymology
    sounds
  }
}

# Search for words by prefix (requires FTS index)
{
  words(filter: { word_prefix: "dict" }) {
    word
    partOfSpeech
    senses
  }
}
```

### Sample Data

Example word entry from kaikki.org:

```json
{
  "word": "dictionary",
  "pos": "noun",
  "senses": [
    {
      "glosses": ["A reference work with a list of words from one or more languages..."],
      "tags": ["countable"]
    }
  ],
  "sounds": [
    { "ipa": "/\u02c8d\u026ak.\u0283\u0259n.\u0259\u0279i/" }
  ],
  "etymology_text": "From Medieval Latin dictionarium, from Latin dictio ('speaking')...",
  "forms": [
    { "form": "dictionaries", "tags": ["plural"] }
  ]
}
```

### Implementation Notes

1. **NDJSON Streaming**: Custom TransformStream for newline-delimited JSON parsing
2. **Memory Efficient**: Never buffers entire 300MB dataset - true streaming
3. **Unique Entity IDs**: Format `{word}/{pos}/{idx}` handles homonyms and multiple POS
4. **JSON Storage**: Senses, sounds, forms stored as JSON for flexibility
5. **Progress Logging**: Every 50K entries processed
6. **Bloom Filter**: 1% FPR, sized for ~2M entities
7. **Chunking**: 50,000 triples per GraphCol chunk

### Comparison with Raw Wiktionary

| Approach | Size | Format | Parsing |
|----------|------|--------|---------|
| kaikki.org JSON | ~300MB | Pre-parsed NDJSON | Simple JSON.parse |
| Wiktionary XML dump | ~7GB compressed | Wikitext markup | Complex wiki parser |
| Wiktionary API | Unlimited | JSON | Rate limited |

We use kaikki.org for simplicity - the data is already extracted and structured.

---

## Common Crawl Host Graph Loader

Streams the Common Crawl host-level web graph to R2 as GraphCol chunks.

### Data Source

- **Release**: cc-main-2024-aug-sep-oct (August/September/October 2024)
- **URL**: https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/
- **Full Size**: ~300 million hosts, 2.6 billion edges
- **Format**: Tab-separated values (gzip compressed, split across 16 vertex files and 32 edge files)

### File Format

**Vertices**: `vertex_id TAB reversed_hostname`
```
0	com.example.www
1	org.wikipedia.en
2	net.cloudflare
```

**Edges**: `from_vertex_id TAB to_vertex_id`
```
0	1
0	2
1	2
```

Hostnames are stored in reverse order (e.g., "com.example.www" for "www.example.com").

### Entity Model

```
Host (https://cc.org/host/{hostname})
  - $type: STRING ("Host")
  - hostname: STRING (normalized hostname, e.g., "www.example.com")
  - vertexId: INT64 (original Common Crawl vertex ID)
```

### Relations

- `linksTo`: REF from source Host to target Host

### Limits (Initial Load)

Due to the massive size of the full graph, the initial load is limited:

| Resource | Limit | Full Graph |
|----------|-------|------------|
| Hosts | 1,000,000 | 300 million |
| Edges | 10,000,000 | 2.6 billion |

### R2 Output Structure

```
datasets/cc-hostgraph/
  chunks/
    chunk_000000.graphcol
    chunk_000001.graphcol
    ...
  bloom/
    filter.json
  index.json
```

### Usage

#### Deploy the Loader

```bash
# From packages/graphdb directory
cd scripts/loaders
wrangler deploy --config wrangler.cc-hostgraph.jsonc
```

#### Trigger the Load

```bash
# Get info about the loader
curl https://cc-hostgraph-loader.<your-subdomain>.workers.dev/

# Check current status
curl https://cc-hostgraph-loader.<your-subdomain>.workers.dev/status

# Trigger a load (POST required)
curl -X POST https://cc-hostgraph-loader.<your-subdomain>.workers.dev/load
```

#### Local Development

```bash
# Run locally with R2 binding
wrangler dev --config wrangler.cc-hostgraph.jsonc
```

### Expected Output

After a successful limited load:

- 1,000,000 hosts
- ~10,000,000 edges (actual count depends on edges between loaded hosts)
- ~13,000,000 triples (3 per host + 1 per edge)
- ~260 GraphCol chunks (~50MB each)
- ~200MB total storage

### Index Schema

```typescript
interface LoaderIndex {
  version: string;         // "1.0.0"
  source: string;          // "Common Crawl Web Graph"
  release: string;         // "cc-main-2024-aug-sep-oct"
  loadedAt: string;        // ISO timestamp
  namespace: string;       // "https://cc.org/"
  limits: {
    maxHosts: number;      // 1,000,000
    maxEdges: number;      // 10,000,000
  };
  stats: {
    hosts: number;         // Actual hosts loaded
    edges: number;         // Actual edges loaded
    totalTriples: number;
    totalChunks: number;
    totalSizeBytes: number;
  };
  chunks: Array<{
    path: string;          // R2 key path
    tripleCount: number;
    sizeBytes: number;
  }>;
  bloom: {
    path: string;          // R2 key for bloom filter
    entityCount: number;
  };
}
```

### Query Examples

After loading, query through GraphDB:

```graphql
# Find a host by hostname
{
  host(id: "https://cc.org/host/www.example.com") {
    hostname
    vertexId
    linksTo {
      hostname
    }
  }
}

# Find hosts linking to a domain
{
  hosts(filter: { linksTo: "https://cc.org/host/www.wikipedia.org" }) {
    hostname
  }
}

# Get link count (outbound)
{
  host(id: "https://cc.org/host/news.ycombinator.com") {
    hostname
    linksTo @count
  }
}
```

### Implementation Notes

1. **Two-Phase Load**: First loads vertices to build ID->hostname mapping, then loads edges
2. **Streaming**: Uses AsyncGenerator for memory-efficient line-by-line processing
3. **Paths Files**: Downloads .paths.gz index files first to get list of data files
4. **Reversed Hostnames**: Common Crawl stores hostnames in reverse (com.example.www)
5. **Edge Filtering**: Edges are skipped if either endpoint wasn't loaded (due to limit)
6. **Chunking**: 50,000 triples per GraphCol chunk
7. **Bloom Filter**: 1% FPR for all host entity IDs
8. **Memory Optimization**: ID->hostname map kept in memory (~100MB for 1M hosts)

### Scaling Beyond Limits

To load the full graph (300M hosts, 2.6B edges):

1. **Queue-Based Processing**: Use Cloudflare Queues to process files in parallel
2. **Durable Object Sharding**: Distribute vertex map across multiple DOs
3. **Incremental Loading**: Load vertices first, persist to R2, then process edges
4. **Time Budget**: Full load would take ~10+ hours with streaming

### Full Graph Statistics (cc-main-2024-aug-sep-oct)

| Metric | Value |
|--------|-------|
| Host Nodes | 299.9 million |
| Host Edges | 2.6 billion |
| Dangling Nodes | ~81% |
| Largest SCC | ~15% of nodes |
