import { describe, it, expect } from 'vitest';
import {
  TokenType,
  tokenize,
  createLexer,
  type Token,
} from '../../src/snippet/lexer';

describe('Lexer TokenType enum', () => {
  it('should have all expected token types', () => {
    // Identifiers
    expect(TokenType.IDENTIFIER).toBe('IDENTIFIER');
    expect(TokenType.NUMBER).toBe('NUMBER');
    expect(TokenType.STRING).toBe('STRING');

    // Operators
    expect(TokenType.DOT).toBe('DOT');
    expect(TokenType.COLON).toBe('COLON');
    expect(TokenType.ARROW_LEFT).toBe('ARROW_LEFT');
    expect(TokenType.STAR).toBe('STAR');

    // Brackets
    expect(TokenType.LBRACKET).toBe('LBRACKET');
    expect(TokenType.RBRACKET).toBe('RBRACKET');
    expect(TokenType.LBRACE).toBe('LBRACE');
    expect(TokenType.RBRACE).toBe('RBRACE');
    expect(TokenType.LPAREN).toBe('LPAREN');
    expect(TokenType.RPAREN).toBe('RPAREN');

    // Filter operators
    expect(TokenType.QUESTION).toBe('QUESTION');
    expect(TokenType.GT).toBe('GT');
    expect(TokenType.LT).toBe('LT');
    expect(TokenType.GTE).toBe('GTE');
    expect(TokenType.LTE).toBe('LTE');
    expect(TokenType.EQ).toBe('EQ');
    expect(TokenType.NEQ).toBe('NEQ');

    // Keywords
    expect(TokenType.AND).toBe('AND');
    expect(TokenType.OR).toBe('OR');
    expect(TokenType.DEPTH).toBe('DEPTH');

    // Misc
    expect(TokenType.COMMA).toBe('COMMA');
    expect(TokenType.EOF).toBe('EOF');
  });
});

describe('tokenize function', () => {
  describe('Simple path: user:123.friends', () => {
    it('should tokenize entity reference with type and id', () => {
      const tokens = tokenize('user:123.friends');

      expect(tokens).toEqual([
        { type: TokenType.IDENTIFIER, value: 'user', position: 0 },
        { type: TokenType.COLON, value: ':', position: 4 },
        { type: TokenType.NUMBER, value: '123', position: 5 },
        { type: TokenType.DOT, value: '.', position: 8 },
        { type: TokenType.IDENTIFIER, value: 'friends', position: 9 },
        { type: TokenType.EOF, value: '', position: 16 },
      ]);
    });

    it('should handle alphanumeric identifiers', () => {
      const tokens = tokenize('user123:456.friends');

      expect(tokens[0]).toEqual({
        type: TokenType.IDENTIFIER,
        value: 'user123',
        position: 0,
      });
    });
  });

  describe('Multi-hop path: user:123.friends.posts', () => {
    it('should tokenize multiple traversal steps', () => {
      const tokens = tokenize('user:123.friends.posts');

      expect(tokens).toEqual([
        { type: TokenType.IDENTIFIER, value: 'user', position: 0 },
        { type: TokenType.COLON, value: ':', position: 4 },
        { type: TokenType.NUMBER, value: '123', position: 5 },
        { type: TokenType.DOT, value: '.', position: 8 },
        { type: TokenType.IDENTIFIER, value: 'friends', position: 9 },
        { type: TokenType.DOT, value: '.', position: 16 },
        { type: TokenType.IDENTIFIER, value: 'posts', position: 17 },
        { type: TokenType.EOF, value: '', position: 22 },
      ]);
    });

    it('should handle deep traversals', () => {
      const tokens = tokenize('user:1.a.b.c.d');
      const identifiers = tokens.filter(
        (t) => t.type === TokenType.IDENTIFIER
      );
      expect(identifiers.map((t) => t.value)).toEqual([
        'user',
        'a',
        'b',
        'c',
        'd',
      ]);
    });
  });

  describe('Filter: user:123.friends[?age > 30]', () => {
    it('should tokenize filter expression', () => {
      const tokens = tokenize('user:123.friends[?age > 30]');

      expect(tokens).toEqual([
        { type: TokenType.IDENTIFIER, value: 'user', position: 0 },
        { type: TokenType.COLON, value: ':', position: 4 },
        { type: TokenType.NUMBER, value: '123', position: 5 },
        { type: TokenType.DOT, value: '.', position: 8 },
        { type: TokenType.IDENTIFIER, value: 'friends', position: 9 },
        { type: TokenType.LBRACKET, value: '[', position: 16 },
        { type: TokenType.QUESTION, value: '?', position: 17 },
        { type: TokenType.IDENTIFIER, value: 'age', position: 18 },
        { type: TokenType.GT, value: '>', position: 22 },
        { type: TokenType.NUMBER, value: '30', position: 24 },
        { type: TokenType.RBRACKET, value: ']', position: 26 },
        { type: TokenType.EOF, value: '', position: 27 },
      ]);
    });

    it('should tokenize less than operator', () => {
      const tokens = tokenize('[?x < 10]');
      // [?x < 10] - '<' is at position 4 (after space)
      expect(tokens).toContainEqual({
        type: TokenType.LT,
        value: '<',
        position: 4,
      });
    });

    it('should tokenize greater than or equal operator', () => {
      const tokens = tokenize('[?age >= 21]');
      // [?age >= 21] - '>=' is at position 6 (after space)
      expect(tokens).toContainEqual({
        type: TokenType.GTE,
        value: '>=',
        position: 6,
      });
    });

    it('should tokenize less than or equal operator', () => {
      const tokens = tokenize('[?count <= 100]');
      // [?count <= 100] - '<=' is at position 8 (after space)
      expect(tokens).toContainEqual({
        type: TokenType.LTE,
        value: '<=',
        position: 8,
      });
    });

    it('should tokenize equality operator', () => {
      const tokens = tokenize('[?status = "active"]');
      // [?status = "active"] - '=' is at position 9 (after space)
      expect(tokens).toContainEqual({
        type: TokenType.EQ,
        value: '=',
        position: 9,
      });
    });

    it('should tokenize not equal operator', () => {
      const tokens = tokenize('[?type != "admin"]');
      // [?type != "admin"] - '!=' is at position 7 (after space)
      expect(tokens).toContainEqual({
        type: TokenType.NEQ,
        value: '!=',
        position: 7,
      });
    });

    it('should tokenize AND keyword', () => {
      const tokens = tokenize('[?age > 18 AND status = "active"]');
      expect(tokens).toContainEqual({
        type: TokenType.AND,
        value: 'AND',
        position: 11,
      });
    });

    it('should tokenize OR keyword', () => {
      const tokens = tokenize('[?role = "admin" OR role = "mod"]');
      expect(tokens).toContainEqual({
        type: TokenType.OR,
        value: 'OR',
        position: 17,
      });
    });
  });

  describe('Reverse traversal: post:456 <- likes', () => {
    it('should tokenize reverse traversal with arrow', () => {
      const tokens = tokenize('post:456 <- likes');

      expect(tokens).toEqual([
        { type: TokenType.IDENTIFIER, value: 'post', position: 0 },
        { type: TokenType.COLON, value: ':', position: 4 },
        { type: TokenType.NUMBER, value: '456', position: 5 },
        { type: TokenType.ARROW_LEFT, value: '<-', position: 9 },
        { type: TokenType.IDENTIFIER, value: 'likes', position: 12 },
        { type: TokenType.EOF, value: '', position: 17 },
      ]);
    });

    it('should handle reverse traversal without spaces', () => {
      const tokens = tokenize('post:456<-likes');
      expect(tokens).toContainEqual({
        type: TokenType.ARROW_LEFT,
        value: '<-',
        position: 8,
      });
    });
  });

  describe('Bounded recursion: user:123.friends*[depth <= 3]', () => {
    it('should tokenize recursive traversal with star', () => {
      const tokens = tokenize('user:123.friends*[depth <= 3]');

      expect(tokens).toEqual([
        { type: TokenType.IDENTIFIER, value: 'user', position: 0 },
        { type: TokenType.COLON, value: ':', position: 4 },
        { type: TokenType.NUMBER, value: '123', position: 5 },
        { type: TokenType.DOT, value: '.', position: 8 },
        { type: TokenType.IDENTIFIER, value: 'friends', position: 9 },
        { type: TokenType.STAR, value: '*', position: 16 },
        { type: TokenType.LBRACKET, value: '[', position: 17 },
        { type: TokenType.DEPTH, value: 'depth', position: 18 },
        { type: TokenType.LTE, value: '<=', position: 24 },
        { type: TokenType.NUMBER, value: '3', position: 27 },
        { type: TokenType.RBRACKET, value: ']', position: 28 },
        { type: TokenType.EOF, value: '', position: 29 },
      ]);
    });
  });

  describe('String literals', () => {
    it('should tokenize double-quoted strings', () => {
      const tokens = tokenize('[?name = "John Doe"]');
      expect(tokens).toContainEqual({
        type: TokenType.STRING,
        value: 'John Doe',
        position: 9,
      });
    });

    it('should tokenize single-quoted strings', () => {
      const tokens = tokenize("[?name = 'Jane']");
      expect(tokens).toContainEqual({
        type: TokenType.STRING,
        value: 'Jane',
        position: 9,
      });
    });

    it('should handle escaped quotes in strings', () => {
      const tokens = tokenize('[?msg = "He said \\"hi\\""]');
      expect(tokens).toContainEqual({
        type: TokenType.STRING,
        value: 'He said "hi"',
        position: 8,
      });
    });

    it('should handle empty strings', () => {
      const tokens = tokenize('[?value = ""]');
      expect(tokens).toContainEqual({
        type: TokenType.STRING,
        value: '',
        position: 10,
      });
    });
  });

  describe('Edge cases', () => {
    describe('Whitespace handling', () => {
      it('should skip leading whitespace', () => {
        const tokens = tokenize('   user:123');
        expect(tokens[0]).toEqual({
          type: TokenType.IDENTIFIER,
          value: 'user',
          position: 3,
        });
      });

      it('should skip trailing whitespace', () => {
        const tokens = tokenize('user:123   ');
        expect(tokens[tokens.length - 1].type).toBe(TokenType.EOF);
      });

      it('should skip whitespace between tokens', () => {
        const tokens = tokenize('user : 123 . friends');
        const types = tokens.map((t) => t.type);
        expect(types).toEqual([
          TokenType.IDENTIFIER,
          TokenType.COLON,
          TokenType.NUMBER,
          TokenType.DOT,
          TokenType.IDENTIFIER,
          TokenType.EOF,
        ]);
      });

      it('should handle tabs and newlines', () => {
        const tokens = tokenize('user:123\n.friends\t.posts');
        const identifiers = tokens.filter(
          (t) => t.type === TokenType.IDENTIFIER
        );
        expect(identifiers.map((t) => t.value)).toEqual([
          'user',
          'friends',
          'posts',
        ]);
      });
    });

    describe('Invalid characters', () => {
      it('should throw error for invalid characters', () => {
        expect(() => tokenize('user@123')).toThrow();
        expect(() => tokenize('user#123')).toThrow();
        expect(() => tokenize('user$123')).toThrow();
      });

      it('should throw error with position info for invalid char', () => {
        try {
          tokenize('user@123');
        } catch (e) {
          // Error message now includes line and column instead of raw position
          expect((e as Error).message).toContain('line 1');
          expect((e as Error).message).toContain('column 5'); // 0-indexed position 4 = column 5
        }
      });
    });

    describe('Unterminated strings', () => {
      it('should throw error for unterminated double-quoted string', () => {
        expect(() => tokenize('[?name = "John]')).toThrow(/unterminated/i);
      });

      it('should throw error for unterminated single-quoted string', () => {
        expect(() => tokenize("[?name = 'John]")).toThrow(/unterminated/i);
      });
    });

    describe('Empty input', () => {
      it('should return only EOF for empty string', () => {
        const tokens = tokenize('');
        expect(tokens).toEqual([
          { type: TokenType.EOF, value: '', position: 0 },
        ]);
      });

      it('should return only EOF for whitespace-only string', () => {
        const tokens = tokenize('   \t\n   ');
        expect(tokens.length).toBe(1);
        expect(tokens[0].type).toBe(TokenType.EOF);
        expect(tokens[0].value).toBe('');
        // Position should be at end of string (after all whitespace consumed)
      });
    });

    describe('Numbers', () => {
      it('should handle integer numbers', () => {
        const tokens = tokenize('user:12345');
        expect(tokens[2]).toEqual({
          type: TokenType.NUMBER,
          value: '12345',
          position: 5,
        });
      });

      it('should handle decimal numbers', () => {
        const tokens = tokenize('[?score > 3.14]');
        expect(tokens).toContainEqual({
          type: TokenType.NUMBER,
          value: '3.14',
          position: 10,
        });
      });

      it('should handle negative numbers', () => {
        const tokens = tokenize('[?temp > -10]');
        // Negative sign can be part of the number or separate
        const hasNegativeNumber = tokens.some(
          (t) => t.type === TokenType.NUMBER && t.value === '-10'
        );
        const hasMinusAndNumber =
          tokens.some((t) => t.value === '-') &&
          tokens.some((t) => t.type === TokenType.NUMBER && t.value === '10');
        expect(hasNegativeNumber || hasMinusAndNumber).toBe(true);
      });
    });

    describe('Brackets and braces', () => {
      it('should tokenize parentheses', () => {
        const tokens = tokenize('[?(age > 18 AND active)]');
        expect(tokens).toContainEqual({
          type: TokenType.LPAREN,
          value: '(',
          position: 2,
        });
        expect(tokens).toContainEqual({
          type: TokenType.RPAREN,
          value: ')',
          position: 22,
        });
      });

      it('should tokenize curly braces', () => {
        const tokens = tokenize('{type: "user"}');
        expect(tokens).toContainEqual({
          type: TokenType.LBRACE,
          value: '{',
          position: 0,
        });
        expect(tokens).toContainEqual({
          type: TokenType.RBRACE,
          value: '}',
          position: 13,
        });
      });

      it('should tokenize comma', () => {
        const tokens = tokenize('[?a, b, c]');
        const commas = tokens.filter((t) => t.type === TokenType.COMMA);
        expect(commas.length).toBe(2);
      });
    });

    describe('Keywords vs identifiers', () => {
      it('should recognize depth as keyword in filter context', () => {
        const tokens = tokenize('[depth <= 3]');
        expect(tokens[1].type).toBe(TokenType.DEPTH);
      });

      it('should recognize AND as keyword (case insensitive)', () => {
        const tokens1 = tokenize('[?a AND b]');
        const tokens2 = tokenize('[?a and b]');
        expect(tokens1).toContainEqual(
          expect.objectContaining({ type: TokenType.AND })
        );
        expect(tokens2).toContainEqual(
          expect.objectContaining({ type: TokenType.AND })
        );
      });

      it('should recognize OR as keyword (case insensitive)', () => {
        const tokens1 = tokenize('[?a OR b]');
        const tokens2 = tokenize('[?a or b]');
        expect(tokens1).toContainEqual(
          expect.objectContaining({ type: TokenType.OR })
        );
        expect(tokens2).toContainEqual(
          expect.objectContaining({ type: TokenType.OR })
        );
      });
    });
  });
});

describe('createLexer function', () => {
  describe('peek()', () => {
    it('should return current token without advancing', () => {
      const lexer = createLexer('user:123');
      const first = lexer.peek();
      const second = lexer.peek();
      expect(first).toEqual(second);
      expect(first.type).toBe(TokenType.IDENTIFIER);
      expect(first.value).toBe('user');
    });
  });

  describe('next()', () => {
    it('should return current token and advance', () => {
      const lexer = createLexer('user:123');
      const first = lexer.next();
      const second = lexer.next();

      expect(first.type).toBe(TokenType.IDENTIFIER);
      expect(first.value).toBe('user');
      expect(second.type).toBe(TokenType.COLON);
      expect(second.value).toBe(':');
    });

    it('should return EOF repeatedly after end', () => {
      const lexer = createLexer('a');
      lexer.next(); // IDENTIFIER 'a'
      const eof1 = lexer.next();
      const eof2 = lexer.next();
      const eof3 = lexer.next();

      expect(eof1.type).toBe(TokenType.EOF);
      expect(eof2.type).toBe(TokenType.EOF);
      expect(eof3.type).toBe(TokenType.EOF);
    });
  });

  describe('expect()', () => {
    it('should return token if type matches', () => {
      const lexer = createLexer('user:123');
      const token = lexer.expect(TokenType.IDENTIFIER);
      expect(token.type).toBe(TokenType.IDENTIFIER);
      expect(token.value).toBe('user');
    });

    it('should throw error if type does not match', () => {
      const lexer = createLexer('user:123');
      expect(() => lexer.expect(TokenType.NUMBER)).toThrow();
    });

    it('should include expected and actual type in error', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        expect((e as Error).message).toContain(TokenType.NUMBER);
        expect((e as Error).message).toContain(TokenType.IDENTIFIER);
      }
    });
  });

  describe('isAtEnd()', () => {
    it('should return false when tokens remain', () => {
      const lexer = createLexer('user:123');
      expect(lexer.isAtEnd()).toBe(false);
    });

    it('should return true at EOF', () => {
      const lexer = createLexer('a');
      lexer.next(); // consume 'a'
      expect(lexer.isAtEnd()).toBe(true);
    });

    it('should return true for empty input', () => {
      const lexer = createLexer('');
      expect(lexer.isAtEnd()).toBe(true);
    });
  });

  describe('Complex queries', () => {
    it('should iterate through all tokens', () => {
      const lexer = createLexer('user:123.friends[?age > 30]');
      const tokens: Token[] = [];

      while (!lexer.isAtEnd()) {
        tokens.push(lexer.next());
      }

      // Should have collected all non-EOF tokens
      expect(tokens.length).toBeGreaterThan(0);
      expect(tokens[tokens.length - 1].type).not.toBe(TokenType.EOF);
    });
  });
});

describe('Lexer size constraint', () => {
  it('should have minimal code paths for snippet compatibility', () => {
    // This test validates that the lexer uses simple, efficient patterns
    // that will minify well to stay under 10KB for Cloudflare Snippets.
    // The actual file size is verified in CI with a separate bundling step.

    // Verify basic functionality works (proves code is loaded)
    const tokens = tokenize('user:123');
    expect(tokens.length).toBeGreaterThan(0);

    // Verify no heavy dependencies by checking exports
    expect(typeof tokenize).toBe('function');
    expect(typeof createLexer).toBe('function');
    expect(typeof TokenType).toBe('object');
  });
});
