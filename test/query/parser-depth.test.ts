/**
 * GraphDB Query Parser Depth Limit Tests
 *
 * Tests for parser recursion depth limiting to prevent stack overflow
 * on deeply nested or malicious queries.
 */

import { describe, it, expect } from 'vitest';
import { parse, ParseError, MAX_PARSER_DEPTH } from '../../src/query/parser';

// ============================================================================
// Parser Depth Limit Tests
// ============================================================================

describe('Parser Depth Limit', () => {
  describe('MAX_PARSER_DEPTH constant', () => {
    it('should export MAX_PARSER_DEPTH constant', () => {
      expect(MAX_PARSER_DEPTH).toBeDefined();
      expect(typeof MAX_PARSER_DEPTH).toBe('number');
    });

    it('should have MAX_PARSER_DEPTH set to 50', () => {
      expect(MAX_PARSER_DEPTH).toBe(50);
    });
  });

  // ============================================================================
  // Queries within depth limit
  // ============================================================================

  describe('Should parse queries up to MAX_PARSER_DEPTH', () => {
    it('should parse deeply nested expansions up to limit', () => {
      // Create nested expansion just under limit
      // Each nested expansion adds one level of depth
      const depth = 20; // Well within limit
      let query = 'user:123';
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `level${i} { ${nested} }`;
      }
      query = `${query} { ${nested} }`;

      const result = parse(query);
      expect(result.type).toBe('expand');
    });

    it('should parse deeply chained property accesses', () => {
      // Chain many property accesses - these should work fine
      // Property accesses don't stack recursion in the same way
      const query = 'user:123.a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t';
      const result = parse(query);
      expect(result.type).toBe('property');
    });

    it('should parse complex nested filters within limit', () => {
      // Nested logical conditions in filter
      const query = 'user:123.friends[?(a > 1 and (b > 2 and (c > 3 and (d > 4 and e > 5))))]';
      const result = parse(query);
      expect(result.type).toBe('filter');
    });

    it('should parse mix of expansions and filters within limit', () => {
      const query = 'user:123.friends[?age > 30] { posts { title, comments { author { name } } } }';
      const result = parse(query);
      expect(result.type).toBe('expand');
    });
  });

  // ============================================================================
  // Queries exceeding depth limit
  // ============================================================================

  describe('Should reject queries exceeding depth limit', () => {
    it('should reject extremely deeply nested expansions', () => {
      // Create expansion deeper than MAX_PARSER_DEPTH
      const depth = MAX_PARSER_DEPTH + 10;
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `level${i} { ${nested} }`;
      }
      const query = `user:123 { ${nested} }`;

      expect(() => parse(query)).toThrow(ParseError);
    });

    it('should include depth-related message in error', () => {
      const depth = MAX_PARSER_DEPTH + 10;
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `level${i} { ${nested} }`;
      }
      const query = `user:123 { ${nested} }`;

      try {
        parse(query);
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const message = (error as ParseError).message.toLowerCase();
        expect(
          message.includes('depth') ||
          message.includes('nested') ||
          message.includes('recursion')
        ).toBe(true);
      }
    });

    it('should reject deeply nested logical expressions in filters', () => {
      // Create deeply nested parenthesized conditions
      const depth = MAX_PARSER_DEPTH + 10;
      let condition = 'x > 1';
      for (let i = 0; i < depth; i++) {
        condition = `(${condition} and y${i} > ${i})`;
      }
      const query = `user:123.friends[?${condition}]`;

      expect(() => parse(query)).toThrow(ParseError);
    });
  });

  // ============================================================================
  // Depth tracking through nested expressions
  // ============================================================================

  describe('Should track depth through nested expressions', () => {
    it('should track depth through expansion fields', () => {
      // Build exactly at the limit - should work
      const depth = MAX_PARSER_DEPTH - 5; // Leave some margin for initial parsing
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `f${i} { ${nested} }`;
      }
      const query = `user:123 { ${nested} }`;

      // This should either parse successfully or throw depth error
      // depending on exact depth accounting
      try {
        const result = parse(query);
        expect(result).toBeDefined();
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        expect((error as ParseError).message.toLowerCase()).toMatch(/depth|nested|recursion/);
      }
    });

    it('should track depth through filter conditions', () => {
      // Deeply nested parenthesized conditions
      const depth = 30;
      let condition = 'a > 1';
      for (let i = 0; i < depth; i++) {
        condition = `(${condition})`;
      }
      const query = `user:123.friends[?${condition}]`;

      // Should parse - 30 levels is within limit
      const result = parse(query);
      expect(result.type).toBe('filter');
    });

    it('should accumulate depth across mixed nesting', () => {
      // Deeply nested expansions - valid syntax
      const query = `user:123 {
        friends {
          posts {
            comments {
              author {
                profile {
                  details {
                    info { name }
                  }
                }
              }
            }
          }
        }
      }`;

      const result = parse(query);
      expect(result.type).toBe('expand');
    });
  });

  // ============================================================================
  // Stack overflow protection (malicious input)
  // ============================================================================

  describe('Should not stack overflow on malicious input', () => {
    it('should handle query with 1000 nested braces without stack overflow', () => {
      const depth = 1000;
      const openBraces = '{ a '.repeat(depth);
      const closeBraces = ' }'.repeat(depth);
      const query = `user:123 ${openBraces}name${closeBraces}`;

      // Should throw ParseError for depth exceeded, NOT stack overflow
      expect(() => parse(query)).toThrow(ParseError);
    });

    it('should handle query with 1000 nested parentheses without stack overflow', () => {
      const depth = 1000;
      const openParens = '('.repeat(depth);
      const closeParens = ')'.repeat(depth);
      const query = `user:123.friends[?${openParens}age > 30${closeParens}]`;

      // Should throw ParseError for depth exceeded, NOT stack overflow
      expect(() => parse(query)).toThrow(ParseError);
    });

    it('should handle alternating nesting patterns without stack overflow', () => {
      // Alternating between expansions and filters
      const depth = 500;
      let query = 'user:123';
      for (let i = 0; i < depth; i++) {
        if (i % 2 === 0) {
          query = `${query} { field${i}`;
        } else {
          query = `${query}[?x > ${i}]`;
        }
      }
      // Close all braces
      for (let i = 0; i < depth / 2; i++) {
        query = `${query} }`;
      }

      // Should throw ParseError, not cause stack overflow
      expect(() => parse(query)).toThrow();
    });

    it('should gracefully reject pathological queries', () => {
      // Very long chain of operations that could cause issues
      const query = 'user:123' + '.x'.repeat(10000);

      // Should complete (either successfully or with error) without hanging
      const startTime = Date.now();
      try {
        parse(query);
      } catch {
        // Expected to potentially throw
      }
      const elapsed = Date.now() - startTime;

      // Should complete in reasonable time (< 5 seconds)
      expect(elapsed).toBeLessThan(5000);
    });
  });

  // ============================================================================
  // Error reporting for depth violations
  // ============================================================================

  describe('Error reporting for depth violations', () => {
    it('should provide ParseError with position information', () => {
      const depth = MAX_PARSER_DEPTH + 10;
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `level${i} { ${nested} }`;
      }
      const query = `user:123 { ${nested} }`;

      try {
        parse(query);
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.position).toBeDefined();
        expect(typeof parseError.position).toBe('number');
      }
    });

    it('should have line and column in depth error', () => {
      const depth = MAX_PARSER_DEPTH + 10;
      let nested = 'name';
      for (let i = 0; i < depth; i++) {
        nested = `level${i} { ${nested} }`;
      }
      const query = `user:123 { ${nested} }`;

      try {
        parse(query);
        expect.fail('Expected ParseError to be thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(ParseError);
        const parseError = error as ParseError;
        expect(parseError.line).toBeDefined();
        expect(parseError.column).toBeDefined();
      }
    });
  });
});
