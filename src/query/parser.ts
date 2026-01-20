/**
 * GraphDB Path Query Parser
 *
 * Pratt parser for graph query expressions.
 * Adapted from path-query-parser spike for GraphDB integration.
 *
 * Query Syntax:
 * - user:123.friends                     # Entity lookup + property access
 * - user:123.friends.posts               # Multi-hop traversal
 * - user:123 { friends { name, posts } } # JSON expansion
 * - user:123.friends[?age > 30]          # Filtered traversal
 * - post:456 <- likes                    # Reverse traversal
 * - user:123.friends*[depth <= 3]        # Bounded recursion
 */

import type { Lexer, Token } from '../snippet/lexer';
import { createLexer, TokenType } from '../snippet/lexer';

// ============================================================================
// AST Types
// ============================================================================

/**
 * Union type for all query AST nodes.
 *
 * Represents all possible node types in a parsed query AST.
 * Use type narrowing on the `type` field to access specific node properties.
 *
 * @example
 * ```typescript
 * function processNode(node: QueryNode) {
 *   switch (node.type) {
 *     case 'entity':
 *       console.log(`Looking up ${node.namespace}:${node.id}`);
 *       break;
 *     case 'property':
 *       console.log(`Traversing ${node.name}`);
 *       break;
 *   }
 * }
 * ```
 */
export type QueryNode =
  | EntityLookup
  | PropertyAccess
  | ReverseTraversal
  | Filter
  | Expansion
  | Recursion;

/**
 * Entity lookup: user:123
 *
 * The starting point for most queries. Identifies a specific entity
 * by namespace and ID.
 *
 * @example
 * ```typescript
 * // Parsed from "user:123"
 * const node: EntityLookup = {
 *   type: 'entity',
 *   namespace: 'user',
 *   id: '123'
 * };
 * ```
 */
export interface EntityLookup {
  type: 'entity';
  /** Namespace/type of the entity (e.g., "user", "post") */
  namespace: string;
  /** ID of the entity (e.g., "123", "abc-def") */
  id: string;
}

/**
 * Property/edge access: .friends, .posts
 *
 * Represents a single-hop traversal following an edge from the source node.
 *
 * @example
 * ```typescript
 * // Parsed from "user:123.friends"
 * const node: PropertyAccess = {
 *   type: 'property',
 *   name: 'friends',
 *   source: { type: 'entity', namespace: 'user', id: '123' }
 * };
 * ```
 */
export interface PropertyAccess {
  type: 'property';
  /** Name of the property or edge to traverse */
  name: string;
  /** Source node to traverse from */
  source: QueryNode;
}

/**
 * Reverse edge traversal: <- likes
 *
 * Find entities that have an edge pointing TO the current node.
 * Used for traversing relationships in the reverse direction.
 *
 * @example
 * ```typescript
 * // Parsed from "post:456 <- likes" (find who liked this post)
 * const node: ReverseTraversal = {
 *   type: 'reverse',
 *   predicate: 'likes',
 *   source: { type: 'entity', namespace: 'post', id: '456' }
 * };
 * ```
 */
export interface ReverseTraversal {
  type: 'reverse';
  /** Name of the predicate/edge to traverse in reverse */
  predicate: string;
  /** Source node to find incoming edges for */
  source: QueryNode;
}

/**
 * Filtered traversal: [?age > 30]
 *
 * Apply conditions to filter results from the source node.
 * Supports comparison operators and logical combinations.
 *
 * @example
 * ```typescript
 * // Parsed from "user:123.friends[?age > 30]"
 * const node: Filter = {
 *   type: 'filter',
 *   condition: { type: 'comparison', field: 'age', operator: '>', value: 30 },
 *   source: { ... } // PropertyAccess node
 * };
 * ```
 */
export interface Filter {
  type: 'filter';
  /** Filter condition to apply */
  condition: FilterCondition;
  /** Source node to filter */
  source: QueryNode;
}

/**
 * JSON-style expansion: { friends { name, posts } }
 *
 * Specify which fields to expand inline in the result.
 * Similar to GraphQL field selection.
 *
 * @example
 * ```typescript
 * // Parsed from "user:123 { name, friends { name } }"
 * const node: Expansion = {
 *   type: 'expand',
 *   fields: [
 *     { name: 'name' },
 *     { name: 'friends', nested: [{ name: 'name' }] }
 *   ],
 *   source: { type: 'entity', namespace: 'user', id: '123' }
 * };
 * ```
 */
export interface Expansion {
  type: 'expand';
  /** Fields to expand */
  fields: ExpansionField[];
  /** Source node to expand */
  source: QueryNode;
}

/**
 * Bounded recursion: *[depth <= 3]
 *
 * Repeat the previous traversal up to a maximum depth.
 * Useful for finding all connected nodes within N hops.
 *
 * @example
 * ```typescript
 * // Parsed from "user:123.friends*[depth <= 3]"
 * const node: Recursion = {
 *   type: 'recurse',
 *   maxDepth: 3,
 *   source: { ... } // PropertyAccess node
 * };
 * ```
 */
export interface Recursion {
  type: 'recurse';
  /** Maximum recursion depth (optional, defaults to unlimited) */
  maxDepth?: number;
  /** Source node to recurse from */
  source: QueryNode;
}

// ============================================================================
// Filter Conditions
// ============================================================================

/**
 * Filter condition types
 */
export type FilterCondition =
  | ComparisonCondition
  | LogicalCondition
  | DepthCondition;

/**
 * Comparison operators
 */
export type ComparisonOperator = '=' | '!=' | '>' | '<' | '>=' | '<=';

/**
 * Comparison condition: age > 30, name = "Alice"
 */
export interface ComparisonCondition {
  type: 'comparison';
  /** Field to compare */
  field: string;
  /** Comparison operator */
  operator: ComparisonOperator;
  /** Value to compare against */
  value: FilterValue;
}

/**
 * Logical operators
 */
export type LogicalOperator = 'and' | 'or';

/**
 * Logical condition: (age > 30 and status = "active")
 */
export interface LogicalCondition {
  type: 'logical';
  /** Logical operator */
  operator: LogicalOperator;
  /** Left operand */
  left: FilterCondition;
  /** Right operand */
  right: FilterCondition;
}

/**
 * Depth condition for recursion: depth <= 3
 */
export interface DepthCondition {
  type: 'depth';
  /** Comparison operator (usually <= or <) */
  operator: ComparisonOperator;
  /** Maximum depth value */
  value: number;
}

/**
 * Filter value types
 */
export type FilterValue = string | number | boolean;

// ============================================================================
// Expansion Fields
// ============================================================================

/**
 * Field in an expansion block
 */
export interface ExpansionField {
  /** Field name to include */
  name: string;
  /** Nested expansion (optional) */
  nested?: ExpansionField[];
  /** Alias for the field (optional) */
  alias?: string;
}

// ============================================================================
// Parser Error
// ============================================================================

/**
 * Calculate line and column from position in source string
 */
function calculateLineColumn(source: string, position: number): { line: number; column: number } {
  let line = 1;
  let column = 1;
  for (let i = 0; i < position && i < source.length; i++) {
    if (source[i] === '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
  }
  return { line, column };
}

export class ParseError extends Error {
  public line: number;
  public column: number;

  constructor(
    message: string,
    public position: number,
    public token?: Token,
    source?: string
  ) {
    // Calculate line/column if source is provided
    const lineCol = source ? calculateLineColumn(source, position) : { line: 1, column: position + 1 };

    super(`Parse error at position ${position} (line ${lineCol.line}, column ${lineCol.column}): ${message}`);
    this.name = 'ParseError';
    this.line = lineCol.line;
    this.column = lineCol.column;
  }
}

// ============================================================================
// Parser Depth Limit
// ============================================================================

/**
 * Maximum recursion depth for parser to prevent stack overflow
 * on deeply nested or malicious queries.
 */
export const MAX_PARSER_DEPTH = 50;

// ============================================================================
// Parser Context
// ============================================================================

/**
 * Parser context to carry source string for error reporting
 * and track recursion depth
 */
interface ParserContext {
  lexer: Lexer;
  source: string;
  depth: number;
}

/**
 * Helper to create ParseError with source for line/column calculation
 */
function createParseError(ctx: ParserContext, message: string, position: number, token?: Token): ParseError {
  return new ParseError(message, position, token, ctx.source);
}

/**
 * Check and increment parser depth, throwing if limit exceeded
 */
function checkDepth(ctx: ParserContext): void {
  ctx.depth++;
  if (ctx.depth > MAX_PARSER_DEPTH) {
    const token = ctx.lexer.peek();
    throw createParseError(
      ctx,
      `Maximum nesting depth (${MAX_PARSER_DEPTH}) exceeded. Query is too deeply nested.`,
      token.position,
      token
    );
  }
}

/**
 * Decrement parser depth after leaving a recursive context
 */
function decrementDepth(ctx: ParserContext): void {
  ctx.depth--;
}

// ============================================================================
// Main Parser
// ============================================================================

/**
 * Parse a graph query string into an AST.
 *
 * Supports the GraphDB path query syntax:
 * - Entity lookup: `user:123`
 * - Property access: `user:123.friends`
 * - Multi-hop traversal: `user:123.friends.posts`
 * - Reverse traversal: `post:456 <- likes`
 * - Filtering: `user:123.friends[?age > 30]`
 * - Expansion: `user:123 { friends { name } }`
 * - Recursion: `user:123.friends*[depth <= 3]`
 *
 * @param query - The query string to parse
 * @returns The parsed AST root node
 * @throws ParseError if the query is invalid or malformed
 * @example
 * ```typescript
 * // Simple entity lookup
 * const ast1 = parse("user:123");
 * // ast1 = { type: 'entity', namespace: 'user', id: '123' }
 *
 * // Multi-hop traversal
 * const ast2 = parse("user:123.friends.posts");
 * // ast2 = PropertyAccess { name: 'posts', source: PropertyAccess { ... } }
 *
 * // Filtered traversal
 * const ast3 = parse("user:123.friends[?age > 30]");
 * ```
 */
export function parse(query: string): QueryNode {
  // Handle empty or whitespace-only queries
  const trimmed = query.trim();
  if (trimmed === '') {
    throw new ParseError('Query is empty', 0, undefined, query);
  }

  const lexer = createLexer(query);
  const ctx: ParserContext = { lexer, source: query, depth: 0 };
  const result = parseQueryWithContext(ctx);

  if (!lexer.isAtEnd()) {
    const token = lexer.peek();
    throw createParseError(ctx, `Unexpected token ${token.type}`, token.position, token);
  }

  return result;
}

/**
 * Parse a query expression with context
 */
function parseQueryWithContext(ctx: ParserContext): QueryNode {
  const { lexer } = ctx;
  // Start with primary (entity lookup)
  let node = parsePrimaryWithContext(ctx);

  // Parse postfix operators: .property, <-predicate, [?filter], {expansion}, *[recursion]
  while (!lexer.isAtEnd()) {
    const token = lexer.peek();

    switch (token.type) {
      case TokenType.DOT:
        lexer.next();
        node = parsePropertyAccessWithContext(ctx, node);
        break;

      case TokenType.ARROW_LEFT:
        lexer.next();
        node = parseReverseTraversalWithContext(ctx, node);
        break;

      case TokenType.LBRACKET:
        node = parseFilterOrRecursionBoundsWithContext(ctx, node);
        break;

      case TokenType.LBRACE:
        node = parseExpansionWithContext(ctx, node);
        break;

      case TokenType.STAR:
        lexer.next();
        node = parseRecursionWithContext(ctx, node);
        break;

      default:
        // No more postfix operators
        return node;
    }
  }

  return node;
}

/**
 * Parse primary expression (entity lookup or identifier)
 */
function parsePrimaryWithContext(ctx: ParserContext): QueryNode {
  const { lexer } = ctx;
  const token = lexer.peek();

  if (token.type !== TokenType.IDENTIFIER) {
    throw createParseError(ctx, `Expected identifier, got ${token.type}`, token.position, token);
  }

  const namespace = lexer.next().value;

  // Check for entity lookup (namespace:id)
  if (lexer.match(TokenType.COLON)) {
    const idToken = lexer.peek();
    let id: string;

    if (idToken.type === TokenType.IDENTIFIER || idToken.type === TokenType.NUMBER) {
      id = lexer.next().value;
    } else if (idToken.type === TokenType.STRING) {
      id = lexer.next().value;
    } else {
      throw createParseError(
        ctx,
        `Expected entity ID, got ${idToken.type}`,
        idToken.position,
        idToken
      );
    }

    return { type: 'entity', namespace, id } as EntityLookup;
  }

  // Just an identifier - treat as entity without explicit ID
  throw createParseError(
    ctx,
    `Expected ':' after namespace '${namespace}'`,
    token.position,
    token
  );
}

/**
 * Parse property access: .friends
 */
function parsePropertyAccessWithContext(ctx: ParserContext, source: QueryNode): PropertyAccess {
  const { lexer } = ctx;
  const token = lexer.peek();

  if (token.type !== TokenType.IDENTIFIER) {
    throw createParseError(ctx, `Expected property name, got ${token.type}`, token.position, token);
  }

  const name = lexer.next().value;

  return { type: 'property', name, source };
}

/**
 * Parse reverse traversal: <- likes
 */
function parseReverseTraversalWithContext(ctx: ParserContext, source: QueryNode): ReverseTraversal {
  const { lexer } = ctx;
  const token = lexer.peek();

  if (token.type !== TokenType.IDENTIFIER) {
    throw createParseError(ctx, `Expected predicate name, got ${token.type}`, token.position, token);
  }

  const predicate = lexer.next().value;

  return { type: 'reverse', predicate, source };
}

/**
 * Parse filter or recursion bounds: [?age > 30] or [depth <= 3]
 */
function parseFilterOrRecursionBoundsWithContext(ctx: ParserContext, source: QueryNode): QueryNode {
  const { lexer } = ctx;
  lexer.expect(TokenType.LBRACKET);

  // Check if this is a filter [?...] or a recursion bound [depth...]
  const token = lexer.peek();

  if (token.type === TokenType.QUESTION) {
    lexer.next();
    const condition = parseFilterConditionWithContext(ctx);
    lexer.expect(TokenType.RBRACKET);
    return { type: 'filter', condition, source } as Filter;
  }

  // Check for depth constraint (used with recursion)
  if (token.type === TokenType.DEPTH) {
    // This is actually a recursion bound, but we need the * before it
    // So we'll parse it as part of recursion
    const condition = parseFilterConditionWithContext(ctx);
    lexer.expect(TokenType.RBRACKET);

    // Extract maxDepth from condition if it's a depth condition
    if (condition.type === 'depth') {
      return {
        type: 'recurse',
        maxDepth: condition.value,
        source,
      } as Recursion;
    }

    throw createParseError(
      ctx,
      'Expected depth constraint in recursion bounds',
      token.position,
      token
    );
  }

  // Otherwise parse as filter
  const condition = parseFilterConditionWithContext(ctx);
  lexer.expect(TokenType.RBRACKET);
  return { type: 'filter', condition, source } as Filter;
}

/**
 * Parse filter condition: age > 30, name = "Alice", etc.
 */
export function parseFilterCondition(lexer: Lexer): FilterCondition {
  return parseLogicalOr(lexer);
}

/**
 * Parse filter condition with context for depth tracking
 */
function parseFilterConditionWithContext(ctx: ParserContext): FilterCondition {
  return parseLogicalOrWithContext(ctx);
}

/**
 * Parse logical OR: ... or ...
 */
function parseLogicalOr(lexer: Lexer): FilterCondition {
  let left = parseLogicalAnd(lexer);

  while (lexer.match(TokenType.OR)) {
    const right = parseLogicalAnd(lexer);
    left = {
      type: 'logical',
      operator: 'or' as LogicalOperator,
      left,
      right,
    } as LogicalCondition;
  }

  return left;
}

/**
 * Parse logical OR with context: ... or ...
 */
function parseLogicalOrWithContext(ctx: ParserContext): FilterCondition {
  let left = parseLogicalAndWithContext(ctx);

  while (ctx.lexer.match(TokenType.OR)) {
    const right = parseLogicalAndWithContext(ctx);
    left = {
      type: 'logical',
      operator: 'or' as LogicalOperator,
      left,
      right,
    } as LogicalCondition;
  }

  return left;
}

/**
 * Parse logical AND: ... and ...
 */
function parseLogicalAnd(lexer: Lexer): FilterCondition {
  let left = parseComparison(lexer);

  while (lexer.match(TokenType.AND)) {
    const right = parseComparison(lexer);
    left = {
      type: 'logical',
      operator: 'and' as LogicalOperator,
      left,
      right,
    } as LogicalCondition;
  }

  return left;
}

/**
 * Parse logical AND with context: ... and ...
 */
function parseLogicalAndWithContext(ctx: ParserContext): FilterCondition {
  let left = parseComparisonWithContext(ctx);

  while (ctx.lexer.match(TokenType.AND)) {
    const right = parseComparisonWithContext(ctx);
    left = {
      type: 'logical',
      operator: 'and' as LogicalOperator,
      left,
      right,
    } as LogicalCondition;
  }

  return left;
}

/**
 * Parse comparison: age > 30, name = "Alice", depth <= 3
 */
function parseComparison(lexer: Lexer): FilterCondition {
  // Handle parenthesized expressions
  if (lexer.match(TokenType.LPAREN)) {
    const condition = parseFilterCondition(lexer);
    lexer.expect(TokenType.RPAREN);
    return condition;
  }

  const token = lexer.peek();

  // Check for depth keyword
  if (token.type === TokenType.DEPTH) {
    lexer.next();
    const operator = parseComparisonOperator(lexer);
    const value = parseNumber(lexer);
    return { type: 'depth', operator, value } as DepthCondition;
  }

  // Regular field comparison
  if (token.type !== TokenType.IDENTIFIER) {
    throw new ParseError(`Expected field name, got ${token.type}`, token.position, token);
  }

  const field = lexer.next().value;
  const operator = parseComparisonOperator(lexer);
  const value = parseFilterValue(lexer);

  return { type: 'comparison', field, operator, value } as ComparisonCondition;
}

/**
 * Parse comparison with context: age > 30, name = "Alice", depth <= 3
 */
function parseComparisonWithContext(ctx: ParserContext): FilterCondition {
  const { lexer } = ctx;

  // Handle parenthesized expressions - this is where recursion happens
  if (lexer.match(TokenType.LPAREN)) {
    checkDepth(ctx); // Track depth for nested parenthesized conditions
    const condition = parseFilterConditionWithContext(ctx);
    lexer.expect(TokenType.RPAREN);
    decrementDepth(ctx);
    return condition;
  }

  const token = lexer.peek();

  // Check for depth keyword
  if (token.type === TokenType.DEPTH) {
    lexer.next();
    const operator = parseComparisonOperator(lexer);
    const value = parseNumber(lexer);
    return { type: 'depth', operator, value } as DepthCondition;
  }

  // Regular field comparison
  if (token.type !== TokenType.IDENTIFIER) {
    throw createParseError(ctx, `Expected field name, got ${token.type}`, token.position, token);
  }

  const field = lexer.next().value;
  const operator = parseComparisonOperator(lexer);
  const value = parseFilterValue(lexer);

  return { type: 'comparison', field, operator, value } as ComparisonCondition;
}

/**
 * Parse comparison operator: =, !=, >, <, >=, <=
 */
function parseComparisonOperator(lexer: Lexer): ComparisonOperator {
  const token = lexer.peek();

  switch (token.type) {
    case TokenType.EQ:
      lexer.next();
      return '=';
    case TokenType.NEQ:
      lexer.next();
      return '!=';
    case TokenType.GT:
      lexer.next();
      return '>';
    case TokenType.LT:
      lexer.next();
      return '<';
    case TokenType.GTE:
      lexer.next();
      return '>=';
    case TokenType.LTE:
      lexer.next();
      return '<=';
    default:
      throw new ParseError(
        `Expected comparison operator, got ${token.type}`,
        token.position,
        token
      );
  }
}

/**
 * Parse filter value: string, number, or boolean
 */
function parseFilterValue(lexer: Lexer): FilterValue {
  const token = lexer.peek();

  switch (token.type) {
    case TokenType.STRING:
      return lexer.next().value;
    case TokenType.NUMBER:
      return parseFloat(lexer.next().value);
    case TokenType.TRUE:
      lexer.next();
      return true;
    case TokenType.FALSE:
      lexer.next();
      return false;
    case TokenType.IDENTIFIER:
      // Allow unquoted strings for simple values
      return lexer.next().value;
    default:
      throw new ParseError(`Expected value, got ${token.type}`, token.position, token);
  }
}

/**
 * Parse a number
 */
function parseNumber(lexer: Lexer): number {
  const token = lexer.peek();
  if (token.type !== TokenType.NUMBER) {
    throw new ParseError(`Expected number, got ${token.type}`, token.position, token);
  }
  return parseFloat(lexer.next().value);
}

/**
 * Parse JSON-style expansion: { friends { name, posts } }
 */
export function parseExpansion(lexer: Lexer, source: QueryNode): Expansion {
  lexer.expect(TokenType.LBRACE);
  const fields = parseExpansionFields(lexer);
  lexer.expect(TokenType.RBRACE);

  return { type: 'expand', fields, source };
}

/**
 * Parse JSON-style expansion with context: { friends { name, posts } }
 */
function parseExpansionWithContext(ctx: ParserContext, source: QueryNode): Expansion {
  const { lexer } = ctx;
  const startToken = lexer.peek();
  lexer.expect(TokenType.LBRACE);

  const fields = parseExpansionFieldsWithContext(ctx);

  // Validate that expansion has at least one field
  if (fields.length === 0) {
    throw createParseError(ctx, 'Expansion must have at least one field', startToken.position, startToken);
  }

  lexer.expect(TokenType.RBRACE);

  return { type: 'expand', fields, source };
}

/**
 * Parse expansion fields: name, posts { title, content }
 */
export function parseExpansionFields(lexer: Lexer): ExpansionField[] {
  const fields: ExpansionField[] = [];

  while (!lexer.isAtEnd() && lexer.peek().type !== TokenType.RBRACE) {
    const field = parseExpansionField(lexer);
    fields.push(field);

    // Optional comma between fields
    lexer.match(TokenType.COMMA);
  }

  return fields;
}

/**
 * Parse expansion fields with context: name, posts { title, content }
 */
function parseExpansionFieldsWithContext(ctx: ParserContext): ExpansionField[] {
  const { lexer } = ctx;
  const fields: ExpansionField[] = [];

  while (!lexer.isAtEnd() && lexer.peek().type !== TokenType.RBRACE) {
    const field = parseExpansionFieldWithContext(ctx);
    fields.push(field);

    // Optional comma between fields
    lexer.match(TokenType.COMMA);
  }

  return fields;
}

/**
 * Parse a single expansion field
 */
function parseExpansionField(lexer: Lexer): ExpansionField {
  const token = lexer.peek();

  if (token.type !== TokenType.IDENTIFIER) {
    throw new ParseError(`Expected field name, got ${token.type}`, token.position, token);
  }

  const name = lexer.next().value;
  const field: ExpansionField = { name };

  // Check for nested expansion
  if (lexer.peek().type === TokenType.LBRACE) {
    lexer.next();
    field.nested = parseExpansionFields(lexer);
    lexer.expect(TokenType.RBRACE);
  }

  return field;
}

/**
 * Parse a single expansion field with context
 */
function parseExpansionFieldWithContext(ctx: ParserContext): ExpansionField {
  const { lexer } = ctx;
  const token = lexer.peek();

  if (token.type !== TokenType.IDENTIFIER) {
    throw createParseError(ctx, `Expected field name, got ${token.type}`, token.position, token);
  }

  const name = lexer.next().value;
  const field: ExpansionField = { name };

  // Check for nested expansion
  if (lexer.peek().type === TokenType.LBRACE) {
    checkDepth(ctx); // Track recursion depth for nested expansion
    lexer.next();
    field.nested = parseExpansionFieldsWithContext(ctx);
    lexer.expect(TokenType.RBRACE);
    decrementDepth(ctx);
  }

  return field;
}

/**
 * Parse recursion: *[depth <= 3] or just *
 */
function parseRecursionWithContext(ctx: ParserContext, source: QueryNode): Recursion {
  const { lexer } = ctx;
  let maxDepth: number | undefined;

  // Check for depth constraint
  if (lexer.peek().type === TokenType.LBRACKET) {
    lexer.next();
    const condition = parseFilterConditionWithContext(ctx);
    lexer.expect(TokenType.RBRACKET);

    if (condition.type === 'depth') {
      maxDepth = condition.value;
    } else {
      throw createParseError(
        ctx,
        'Expected depth constraint in recursion',
        lexer.position(),
        lexer.peek()
      );
    }
  }

  const result: Recursion = { type: 'recurse', source };
  if (maxDepth !== undefined) {
    result.maxDepth = maxDepth;
  }
  return result;
}

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * Stringify an AST node back to query syntax (for debugging).
 *
 * Converts a parsed AST back into a query string representation.
 * Useful for debugging and logging parsed queries.
 *
 * @param node - The AST node to stringify
 * @returns The query string representation
 * @example
 * ```typescript
 * const ast = parse("user:123.friends[?age > 30]");
 * console.log(stringify(ast)); // "user:123.friends[?age > 30]"
 * ```
 */
export function stringify(node: QueryNode): string {
  switch (node.type) {
    case 'entity':
      return `${node.namespace}:${node.id}`;

    case 'property':
      return `${stringify(node.source)}.${node.name}`;

    case 'reverse':
      return `${stringify(node.source)} <- ${node.predicate}`;

    case 'filter':
      return `${stringify(node.source)}[?${stringifyCondition(node.condition)}]`;

    case 'expand':
      return `${stringify(node.source)} { ${stringifyFields(node.fields)} }`;

    case 'recurse':
      if (node.maxDepth !== undefined) {
        return `${stringify(node.source)}*[depth <= ${node.maxDepth}]`;
      }
      return `${stringify(node.source)}*`;

    default:
      return '<unknown>';
  }
}

function stringifyCondition(condition: FilterCondition): string {
  switch (condition.type) {
    case 'comparison':
      const value =
        typeof condition.value === 'string'
          ? `"${condition.value}"`
          : condition.value;
      return `${condition.field} ${condition.operator} ${value}`;

    case 'logical':
      return `(${stringifyCondition(condition.left)} ${condition.operator} ${stringifyCondition(condition.right)})`;

    case 'depth':
      return `depth ${condition.operator} ${condition.value}`;

    default:
      return '<unknown>';
  }
}

function stringifyFields(fields: ExpansionField[]): string {
  return fields
    .map((f) => {
      if (f.nested) {
        return `${f.name} { ${stringifyFields(f.nested)} }`;
      }
      return f.name;
    })
    .join(', ');
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Create an entity lookup node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param namespace - The entity namespace (e.g., "user", "post")
 * @param id - The entity ID
 * @returns An EntityLookup node
 * @example
 * ```typescript
 * const node = entity("user", "123");
 * // Equivalent to parsing "user:123"
 * ```
 */
export function entity(namespace: string, id: string): EntityLookup {
  return { type: 'entity', namespace, id };
}

/**
 * Create a property access node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param name - The property/edge name to traverse
 * @param source - The source node to traverse from
 * @returns A PropertyAccess node
 * @example
 * ```typescript
 * const node = property("friends", entity("user", "123"));
 * // Equivalent to parsing "user:123.friends"
 * ```
 */
export function property(name: string, source: QueryNode): PropertyAccess {
  return { type: 'property', name, source };
}

/**
 * Create a reverse traversal node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param predicate - The predicate to traverse in reverse
 * @param source - The source node to find incoming edges for
 * @returns A ReverseTraversal node
 * @example
 * ```typescript
 * const node = reverse("likes", entity("post", "456"));
 * // Equivalent to parsing "post:456 <- likes"
 * ```
 */
export function reverse(predicate: string, source: QueryNode): ReverseTraversal {
  return { type: 'reverse', predicate, source };
}

/**
 * Create a filter node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param condition - The filter condition to apply
 * @param source - The source node to filter
 * @returns A Filter node
 * @example
 * ```typescript
 * const cond = comparison("age", ">", 30);
 * const node = filter(cond, property("friends", entity("user", "123")));
 * // Equivalent to parsing "user:123.friends[?age > 30]"
 * ```
 */
export function filter(condition: FilterCondition, source: QueryNode): Filter {
  return { type: 'filter', condition, source };
}

/**
 * Create an expansion node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param fields - The fields to expand
 * @param source - The source node to expand
 * @returns An Expansion node
 * @example
 * ```typescript
 * const node = expand([{ name: "name" }, { name: "email" }], entity("user", "123"));
 * // Equivalent to parsing "user:123 { name, email }"
 * ```
 */
export function expand(fields: ExpansionField[], source: QueryNode): Expansion {
  return { type: 'expand', fields, source };
}

/**
 * Create a recursion node.
 *
 * Factory function for programmatically building AST nodes.
 *
 * @param source - The source node to recurse from
 * @param maxDepth - Optional maximum recursion depth
 * @returns A Recursion node
 * @example
 * ```typescript
 * const node = recurse(property("friends", entity("user", "123")), 3);
 * // Equivalent to parsing "user:123.friends*[depth <= 3]"
 * ```
 */
export function recurse(source: QueryNode, maxDepth?: number): Recursion {
  const result: Recursion = { type: 'recurse', source };
  if (maxDepth !== undefined) {
    result.maxDepth = maxDepth;
  }
  return result;
}

/**
 * Create a comparison condition.
 *
 * Factory function for building filter conditions.
 *
 * @param field - The field name to compare
 * @param operator - The comparison operator (=, !=, >, <, >=, <=)
 * @param value - The value to compare against
 * @returns A ComparisonCondition
 * @example
 * ```typescript
 * const cond = comparison("age", ">", 30);
 * // Represents: age > 30
 * ```
 */
export function comparison(
  field: string,
  operator: ComparisonOperator,
  value: FilterValue
): ComparisonCondition {
  return { type: 'comparison', field, operator, value };
}

/**
 * Create a logical condition.
 *
 * Factory function for combining filter conditions.
 *
 * @param operator - The logical operator ('and' or 'or')
 * @param left - The left operand condition
 * @param right - The right operand condition
 * @returns A LogicalCondition
 * @example
 * ```typescript
 * const cond = logical("and",
 *   comparison("age", ">", 18),
 *   comparison("status", "=", "active")
 * );
 * // Represents: age > 18 and status = "active"
 * ```
 */
export function logical(
  operator: LogicalOperator,
  left: FilterCondition,
  right: FilterCondition
): LogicalCondition {
  return { type: 'logical', operator, left, right };
}

/**
 * Create a depth condition.
 *
 * Factory function for recursion depth constraints.
 *
 * @param operator - The comparison operator (usually <= or <)
 * @param value - The maximum depth value
 * @returns A DepthCondition
 * @example
 * ```typescript
 * const cond = depth("<=", 3);
 * // Represents: depth <= 3
 * ```
 */
export function depth(operator: ComparisonOperator, value: number): DepthCondition {
  return { type: 'depth', operator, value };
}

/**
 * Count the number of hops in a query.
 *
 * Useful for estimating query complexity and cost.
 * Returns Infinity for unbounded recursion.
 *
 * @param node - The query AST node to analyze
 * @returns The number of traversal hops
 * @example
 * ```typescript
 * countHops(parse("user:123"));                    // 0
 * countHops(parse("user:123.friends"));            // 1
 * countHops(parse("user:123.friends.posts"));      // 2
 * countHops(parse("user:123.friends*"));           // Infinity
 * countHops(parse("user:123.friends*[depth <= 3]")); // 3
 * ```
 */
export function countHops(node: QueryNode): number {
  switch (node.type) {
    case 'entity':
      return 0;
    case 'property':
    case 'reverse':
      return 1 + countHops(node.source);
    case 'filter':
    case 'expand':
      return countHops(node.source);
    case 'recurse':
      return node.maxDepth ?? Infinity;
    default:
      return 0;
  }
}
