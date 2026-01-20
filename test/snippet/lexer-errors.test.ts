/**
 * Lexer Error Consistency Tests
 *
 * Verifies that all lexer errors:
 * - Use the LexerError class
 * - Include consistent error codes
 * - Include position information (line, column, offset)
 * - Include source context
 * - Have consistent message format
 */

import { describe, it, expect } from 'vitest';
import {
  tokenize,
  createLexer,
  TokenType,
  LexerError,
  LexerErrorCode,
  type LexerPosition,
} from '../../src/snippet/lexer';

describe('LexerError consistency', () => {
  describe('Error class properties', () => {
    it('should be instanceof LexerError', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect(e).toBeInstanceOf(LexerError);
        expect(e).toBeInstanceOf(Error);
      }
    });

    it('should have name "LexerError"', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).name).toBe('LexerError');
      }
    });

    it('should have all required properties', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        expect(err.code).toBeDefined();
        expect(err.message).toBeDefined();
        expect(err.position).toBeDefined();
        expect(err.source).toBeDefined();
      }
    });
  });

  describe('Position information', () => {
    it('should include offset', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.offset).toBe(4);
      }
    });

    it('should include line number (1-indexed)', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.line).toBe(1);
      }
    });

    it('should include column number (1-indexed)', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.column).toBe(5);
      }
    });

    it('should track line correctly for multiline input', () => {
      try {
        tokenize('user:123\n.friends\n@invalid');
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.line).toBe(3);
        expect(err.position.column).toBe(1);
      }
    });

    it('should track column correctly after newline', () => {
      try {
        tokenize('user:123\nfoo@bar');
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.line).toBe(2);
        expect(err.position.column).toBe(4);
      }
    });
  });

  describe('Error codes', () => {
    it('should use UNEXPECTED_CHARACTER for invalid characters', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).code).toBe(LexerErrorCode.UNEXPECTED_CHARACTER);
      }
    });

    it('should use UNTERMINATED_STRING for unclosed strings', () => {
      try {
        tokenize('[?name = "John]');
      } catch (e) {
        expect((e as LexerError).code).toBe(LexerErrorCode.UNTERMINATED_STRING);
      }
    });

    it('should use UNEXPECTED_TOKEN for expect() failures', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        expect((e as LexerError).code).toBe(LexerErrorCode.UNEXPECTED_TOKEN);
      }
    });
  });

  describe('Message format consistency', () => {
    it('should include error code in message', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).message).toContain('[UNEXPECTED_CHARACTER]');
      }
    });

    it('should include line and column in message', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const msg = (e as LexerError).message;
        expect(msg).toContain('line 1');
        expect(msg).toContain('column 5');
      }
    });

    it('should include "found" value in message when available', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).message).toContain("found: '@'");
      }
    });

    it('should include "expected" value in message when available', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).message).toContain('expected:');
      }
    });

    it('should include source snippet in message', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).message).toContain('Source:');
        expect((e as LexerError).message).toContain('user@123');
      }
    });

    it('should include caret pointing to error position', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        // The caret should appear in the message
        expect((e as LexerError).message).toContain('^');
      }
    });
  });

  describe('Source context', () => {
    it('should capture the error line as source', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        expect((e as LexerError).source).toBe('user@123');
      }
    });

    it('should capture only the error line for multiline input', () => {
      try {
        tokenize('user:123\nfoo@bar\nbaz');
      } catch (e) {
        expect((e as LexerError).source).toBe('foo@bar');
      }
    });

    it('should truncate very long lines with ellipsis', () => {
      const longInput = 'a'.repeat(100) + '@' + 'b'.repeat(100);
      try {
        tokenize(longInput);
      } catch (e) {
        const source = (e as LexerError).source;
        expect(source.length).toBeLessThan(longInput.length);
        expect(source).toContain('...');
      }
    });
  });

  describe('JSON serialization', () => {
    it('should serialize to JSON with toJSON()', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        const json = err.toJSON();
        expect(json.code).toBe(LexerErrorCode.UNEXPECTED_CHARACTER);
        expect(json.position.offset).toBe(4);
        expect(json.position.line).toBe(1);
        expect(json.position.column).toBe(5);
        expect(json.source).toBe('user@123');
      }
    });

    it('should include found and expected in JSON', () => {
      try {
        tokenize('user@123');
      } catch (e) {
        const err = e as LexerError;
        const json = err.toJSON();
        expect(json.found).toBe('@');
        expect(json.expected).toBeDefined();
      }
    });
  });

  describe('All error paths use LexerError', () => {
    const invalidInputs = [
      { input: '@start', desc: 'invalid at start' },
      { input: 'mid@dle', desc: 'invalid in middle' },
      { input: 'end@', desc: 'invalid at end' },
      { input: '#hash', desc: 'hash character' },
      { input: '$dollar', desc: 'dollar sign' },
      { input: 'user%123', desc: 'percent sign' },
      { input: 'path^caret', desc: 'caret character' },
      { input: 'back\\slash', desc: 'backslash outside string' },
      { input: 'tilde~test', desc: 'tilde character' },
      { input: 'grave`mark', desc: 'backtick character' },
    ];

    for (const { input, desc } of invalidInputs) {
      it(`should throw LexerError for ${desc}`, () => {
        try {
          tokenize(input);
          expect.fail(`Expected error for input: ${input}`);
        } catch (e) {
          expect(e).toBeInstanceOf(LexerError);
          expect((e as LexerError).code).toBe(LexerErrorCode.UNEXPECTED_CHARACTER);
        }
      });
    }

    const unterminatedStrings = [
      { input: '"unterminated', desc: 'unterminated double quote' },
      { input: "'unterminated", desc: 'unterminated single quote' },
      { input: '[?name = "test', desc: 'unterminated in filter' },
      { input: "user:'incomplete", desc: 'unterminated after colon' },
    ];

    for (const { input, desc } of unterminatedStrings) {
      it(`should throw LexerError for ${desc}`, () => {
        try {
          tokenize(input);
          expect.fail(`Expected error for input: ${input}`);
        } catch (e) {
          expect(e).toBeInstanceOf(LexerError);
          expect((e as LexerError).code).toBe(LexerErrorCode.UNTERMINATED_STRING);
        }
      });
    }
  });

  describe('expect() errors', () => {
    it('should throw LexerError with UNEXPECTED_TOKEN', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        expect(e).toBeInstanceOf(LexerError);
        expect((e as LexerError).code).toBe(LexerErrorCode.UNEXPECTED_TOKEN);
      }
    });

    it('should include found token type', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        expect((e as LexerError).found).toBe(TokenType.IDENTIFIER);
      }
    });

    it('should include expected token type', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        expect((e as LexerError).expected).toBe(TokenType.NUMBER);
      }
    });

    it('should include position information', () => {
      const lexer = createLexer('user:123');
      try {
        lexer.expect(TokenType.NUMBER);
      } catch (e) {
        const err = e as LexerError;
        expect(err.position.offset).toBe(0);
        expect(err.position.line).toBe(1);
        expect(err.position.column).toBe(1);
      }
    });
  });
});
