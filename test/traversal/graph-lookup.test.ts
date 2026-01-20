/**
 * Graph Lookup Tests
 *
 * Tests for the bloom filter routing and chunk-based entity lookup.
 */

import { describe, it, expect } from 'vitest';
import {
  namespaceToR2Path,
  extractNamespaceFromEntityId,
} from '../../src/traversal/graph-lookup.js';

describe('namespaceToR2Path', () => {
  it('converts simple namespace to reversed domain path', () => {
    const result = namespaceToR2Path('https://imdb.com/title/');
    expect(result).toBe('.com/.imdb/title');
  });

  it('handles namespace without path', () => {
    const result = namespaceToR2Path('https://example.com/');
    expect(result).toBe('.com/.example');
  });

  it('handles nested paths', () => {
    const result = namespaceToR2Path('https://api.example.com/v1/users/');
    expect(result).toBe('.com/.example/.api/v1/users');
  });

  it('handles subdomains', () => {
    const result = namespaceToR2Path('https://www.example.co.uk/data/');
    expect(result).toBe('.uk/.co/.example/.www/data');
  });

  it('returns input for invalid URLs', () => {
    const result = namespaceToR2Path('not-a-url');
    expect(result).toBe('not-a-url');
  });
});

describe('extractNamespaceFromEntityId', () => {
  it('extracts namespace from entity with multiple path segments', () => {
    const result = extractNamespaceFromEntityId('https://imdb.com/title/tt0000001');
    expect(result).toBe('https://imdb.com/title/');
  });

  it('extracts namespace from entity with single path segment', () => {
    const result = extractNamespaceFromEntityId('https://example.com/users/123');
    expect(result).toBe('https://example.com/users/');
  });

  it('uses origin for entity without path', () => {
    const result = extractNamespaceFromEntityId('https://example.com/123');
    expect(result).toBe('https://example.com/');
  });

  it('handles deeply nested paths', () => {
    const result = extractNamespaceFromEntityId('https://api.example.com/v1/users/123/profile');
    expect(result).toBe('https://api.example.com/v1/');
  });

  it('returns input for invalid URLs', () => {
    const result = extractNamespaceFromEntityId('not-a-url');
    expect(result).toBe('not-a-url');
  });
});

describe('Integration: namespace to R2 path round-trip', () => {
  it('correctly maps namespace to manifest path', () => {
    const entityId = 'https://imdb.com/title/tt0000001';
    const namespace = extractNamespaceFromEntityId(entityId);
    const r2Path = namespaceToR2Path(namespace);
    const manifestPath = `${r2Path}/_manifest.json`;

    expect(namespace).toBe('https://imdb.com/title/');
    expect(manifestPath).toBe('.com/.imdb/title/_manifest.json');
  });

  it('correctly maps chunk path', () => {
    const namespace = 'https://wiktionary.org/entries/';
    const chunkId = 'chunk-001';
    const r2Path = namespaceToR2Path(namespace);
    const chunkPath = `${r2Path}/_chunks/${chunkId}.gcol`;

    expect(chunkPath).toBe('.org/.wiktionary/entries/_chunks/chunk-001.gcol');
  });
});
