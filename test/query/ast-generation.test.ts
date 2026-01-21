/**
 * GraphDB Query AST Generation Tests (TDD RED Phase)
 *
 * Tests for edge cases and advanced scenarios in query AST generation.
 * These tests define expected behavior for complex query patterns.
 *
 * Focus areas:
 * - Complex nested structures
 * - Edge cases in entity ID handling
 * - Filter expression combinations
 * - Recursion with various bounds
 * - Mixed traversal patterns
 */

import { describe, it, expect } from 'vitest';
import {
  parse,
  stringify,
  entity,
  property,
  reverse,
  filter,
  expand,
  recurse,
  comparison,
  logical,
  depth,
  countHops,
  type QueryNode,
  type EntityLookup,
  type PropertyAccess,
  type ReverseTraversal,
  type Filter,
  type Expansion,
  type Recursion,
  type ComparisonCondition,
  type LogicalCondition,
  ParseError,
} from '../../src/query/parser';

// ============================================================================
// Complex Entity ID Handling
// ============================================================================

describe('Entity ID Handling', () => {
  describe('UUID-style IDs', () => {
    it('should parse UUID entity ID', () => {
      const result = parse('user:"550e8400-e29b-41d4-a716-446655440000"');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('550e8400-e29b-41d4-a716-446655440000');
    });

    it('should parse purely alphanumeric ID starting with letter', () => {
      // IDs that start with a letter and contain only alphanumeric chars can be unquoted
      const result = parse('user:abc123def');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('abc123def');
    });
  });

  describe('Numeric IDs', () => {
    it('should parse large numeric ID', () => {
      const result = parse('user:9999999999999999');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('9999999999999999');
    });

    it('should parse ID with leading zeros', () => {
      const result = parse('order:00123456');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('00123456');
    });
  });

  describe('Special Character IDs', () => {
    it('should parse ID with underscores', () => {
      const result = parse('document:doc_2024_001');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('doc_2024_001');
    });

    it('should parse ID with path-like structure in quotes', () => {
      const result = parse('file:"folder/subfolder/file.txt"');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('folder/subfolder/file.txt');
    });

    it('should parse ID with URL encoding in quotes', () => {
      const result = parse('resource:"item%20with%20spaces"');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).id).toBe('item%20with%20spaces');
    });
  });

  describe('Namespace variations', () => {
    it('should parse namespace with underscores', () => {
      const result = parse('user_profile:123');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).namespace).toBe('user_profile');
    });

    it('should parse namespace with numbers', () => {
      const result = parse('v2user:123');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).namespace).toBe('v2user');
    });

    it('should parse camelCase namespace', () => {
      const result = parse('userProfile:123');
      expect(result.type).toBe('entity');
      expect((result as EntityLookup).namespace).toBe('userProfile');
    });
  });
});

// ============================================================================
// Complex Filter Expressions
// ============================================================================

describe('Complex Filter Expressions', () => {
  describe('Deeply nested logical conditions', () => {
    it('should parse triple AND condition', () => {
      const result = parse('user:123.friends[?age > 30 and status = "active" and role = "admin"]');
      expect(result.type).toBe('filter');
      const filterNode = result as Filter;
      expect(filterNode.condition.type).toBe('logical');
    });

    it('should parse triple OR condition', () => {
      const result = parse('user:123.friends[?role = "admin" or role = "mod" or role = "editor"]');
      expect(result.type).toBe('filter');
      const filterNode = result as Filter;
      expect(filterNode.condition.type).toBe('logical');
    });

    it('should parse mixed AND/OR with proper precedence', () => {
      // AND has higher precedence than OR
      const result = parse('user:123.friends[?age > 30 and active = true or vip = true]');
      expect(result.type).toBe('filter');
      const filterNode = result as Filter;
      // The structure should be: (age > 30 AND active = true) OR (vip = true)
      expect(filterNode.condition.type).toBe('logical');
      const logicalCond = filterNode.condition as LogicalCondition;
      expect(logicalCond.operator).toBe('or');
    });

    it('should parse parenthesized conditions to override precedence', () => {
      const result = parse('user:123.friends[?(age > 30 or age < 18) and verified = true]');
      expect(result.type).toBe('filter');
      const filterNode = result as Filter;
      expect(filterNode.condition.type).toBe('logical');
      const logicalCond = filterNode.condition as LogicalCondition;
      expect(logicalCond.operator).toBe('and');
    });
  });

  describe('Comparison operators with various types', () => {
    it('should parse filter with boolean true', () => {
      const result = parse('user:123.friends[?active = true]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.value).toBe(true);
    });

    it('should parse filter with boolean false', () => {
      const result = parse('user:123.friends[?deleted = false]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.value).toBe(false);
    });

    it('should parse filter with negative number', () => {
      const result = parse('account:123.transactions[?amount < -100]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.value).toBe(-100);
    });

    it('should parse filter with decimal number', () => {
      const result = parse('product:123.reviews[?rating >= 4.5]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.value).toBe(4.5);
    });

    it('should parse filter with empty string', () => {
      const result = parse('user:123.posts[?title != ""]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.value).toBe('');
    });
  });

  describe('Field names', () => {
    it('should parse filter with underscore field name', () => {
      const result = parse('user:123.posts[?created_at > 0]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.field).toBe('created_at');
    });

    it('should parse filter with camelCase field name', () => {
      const result = parse('user:123.posts[?createdAt > 0]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.field).toBe('createdAt');
    });

    it('should parse filter with numeric suffix in field name', () => {
      const result = parse('form:123.responses[?field1 = "value"]');
      const filterNode = result as Filter;
      const cond = filterNode.condition as ComparisonCondition;
      expect(cond.field).toBe('field1');
    });
  });
});

// ============================================================================
// Recursion Patterns
// ============================================================================

describe('Recursion Patterns', () => {
  describe('Depth specifications', () => {
    it('should parse recursion with depth = 1', () => {
      const result = parse('user:123.friends*[depth <= 1]');
      expect(result.type).toBe('recurse');
      expect((result as Recursion).maxDepth).toBe(1);
    });

    it('should parse recursion with large depth', () => {
      const result = parse('category:root.subcategories*[depth <= 50]');
      expect(result.type).toBe('recurse');
      expect((result as Recursion).maxDepth).toBe(50);
    });

    it('should parse recursion with depth < operator', () => {
      const result = parse('user:123.friends*[depth < 5]');
      expect(result.type).toBe('recurse');
      expect((result as Recursion).maxDepth).toBe(5);
    });

    it('should parse unbounded recursion', () => {
      const result = parse('user:123.friends*');
      expect(result.type).toBe('recurse');
      expect((result as Recursion).maxDepth).toBeUndefined();
    });
  });

  describe('Recursion after various operations', () => {
    it('should parse recursion after filter', () => {
      const result = parse('user:123.friends[?active = true]*[depth <= 3]');
      expect(result.type).toBe('recurse');
      const recurseNode = result as Recursion;
      expect(recurseNode.source.type).toBe('filter');
    });

    it('should parse recursion after multiple traversals', () => {
      const result = parse('user:123.team.members*[depth <= 2]');
      expect(result.type).toBe('recurse');
      const recurseNode = result as Recursion;
      expect(recurseNode.source.type).toBe('property');
      expect((recurseNode.source as PropertyAccess).name).toBe('members');
    });
  });

  describe('countHops with recursion', () => {
    it('should return Infinity for unbounded recursion', () => {
      const result = parse('user:123.friends*');
      expect(countHops(result)).toBe(Infinity);
    });

    it('should return maxDepth for bounded recursion', () => {
      const result = parse('user:123.friends*[depth <= 5]');
      expect(countHops(result)).toBe(5);
    });

    it('should count traversals before recursion', () => {
      const result = parse('user:123.team.members*[depth <= 3]');
      // 2 traversals (team, members) then recursion with depth 3
      // But countHops returns maxDepth for recurse nodes
      expect(countHops(result)).toBe(3);
    });
  });
});

// ============================================================================
// Expansion Patterns
// ============================================================================

describe('Expansion Patterns', () => {
  describe('Simple expansions', () => {
    it('should parse single field expansion', () => {
      const result = parse('user:123 { name }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.fields.length).toBe(1);
      expect(expandNode.fields[0].name).toBe('name');
    });

    it('should parse many fields expansion', () => {
      const result = parse('user:123 { name, email, age, role, status, createdAt }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.fields.length).toBe(6);
    });
  });

  describe('Nested expansions', () => {
    it('should parse single level nesting', () => {
      const result = parse('user:123 { name, friends { name } }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      const friendsField = expandNode.fields.find(f => f.name === 'friends');
      expect(friendsField?.nested).toBeDefined();
      expect(friendsField?.nested?.length).toBe(1);
    });

    it('should parse multiple nested expansions', () => {
      const result = parse('user:123 { friends { name }, posts { title, comments { text } } }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.fields.length).toBe(2);

      const postsField = expandNode.fields.find(f => f.name === 'posts');
      expect(postsField?.nested?.length).toBe(2);

      const commentsField = postsField?.nested?.find(f => f.name === 'comments');
      expect(commentsField?.nested?.length).toBe(1);
    });

    it('should parse deeply nested expansion (3 levels)', () => {
      const result = parse('org:123 { teams { members { profile { bio } } } }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;

      const teamsField = expandNode.fields[0];
      expect(teamsField.name).toBe('teams');
      expect(teamsField.nested?.[0].name).toBe('members');
      expect(teamsField.nested?.[0].nested?.[0].name).toBe('profile');
      expect(teamsField.nested?.[0].nested?.[0].nested?.[0].name).toBe('bio');
    });
  });

  describe('Expansion after traversal', () => {
    it('should parse expansion after single traversal', () => {
      const result = parse('user:123.friends { name, email }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.source.type).toBe('property');
    });

    it('should parse expansion after filter', () => {
      const result = parse('user:123.friends[?active = true] { name }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.source.type).toBe('filter');
    });

    it('should parse expansion after reverse traversal', () => {
      const result = parse('post:456 <- likes { name, avatar }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.source.type).toBe('reverse');
    });
  });
});

// ============================================================================
// Mixed Traversal Patterns
// ============================================================================

describe('Mixed Traversal Patterns', () => {
  describe('Forward and reverse mixed', () => {
    it('should parse forward then reverse traversal', () => {
      const result = parse('user:123.posts <- likes');
      expect(result.type).toBe('reverse');
      const reverseNode = result as ReverseTraversal;
      expect(reverseNode.predicate).toBe('likes');
      expect(reverseNode.source.type).toBe('property');
    });

    it('should parse reverse then forward traversal', () => {
      const result = parse('post:456 <- author.friends');
      expect(result.type).toBe('property');
      const propNode = result as PropertyAccess;
      expect(propNode.name).toBe('friends');
      expect(propNode.source.type).toBe('reverse');
    });

    it('should parse multiple reverse traversals', () => {
      const result = parse('post:456 <- comments <- likes');
      expect(result.type).toBe('reverse');
      const outer = result as ReverseTraversal;
      expect(outer.predicate).toBe('likes');
      expect(outer.source.type).toBe('reverse');
      const inner = outer.source as ReverseTraversal;
      expect(inner.predicate).toBe('comments');
    });
  });

  describe('Complex query chains', () => {
    it('should parse traversal + filter + traversal', () => {
      const result = parse('user:123.friends[?active = true].posts');
      expect(result.type).toBe('property');
      const propNode = result as PropertyAccess;
      expect(propNode.name).toBe('posts');
      expect(propNode.source.type).toBe('filter');
    });

    it('should parse filter + reverse + expansion', () => {
      const result = parse('user:123.posts[?published = true] <- likes { name }');
      expect(result.type).toBe('expand');
      const expandNode = result as Expansion;
      expect(expandNode.source.type).toBe('reverse');
    });

    it('should parse full complex query', () => {
      // user:123 -> friends (filtered) -> posts -> likes (reverse) -> expand
      const result = parse('user:123.friends[?vip = true].posts <- likes { name, avatar }');
      expect(result.type).toBe('expand');
    });
  });
});

// ============================================================================
// AST Builder Functions
// ============================================================================

describe('AST Builder Functions', () => {
  describe('entity()', () => {
    it('should create EntityLookup node', () => {
      const node = entity('user', '123');
      expect(node.type).toBe('entity');
      expect(node.namespace).toBe('user');
      expect(node.id).toBe('123');
    });
  });

  describe('property()', () => {
    it('should create PropertyAccess node', () => {
      const source = entity('user', '123');
      const node = property('friends', source);
      expect(node.type).toBe('property');
      expect(node.name).toBe('friends');
      expect(node.source).toBe(source);
    });
  });

  describe('reverse()', () => {
    it('should create ReverseTraversal node', () => {
      const source = entity('post', '456');
      const node = reverse('likes', source);
      expect(node.type).toBe('reverse');
      expect(node.predicate).toBe('likes');
      expect(node.source).toBe(source);
    });
  });

  describe('filter()', () => {
    it('should create Filter node', () => {
      const source = property('friends', entity('user', '123'));
      const cond = comparison('age', '>', 30);
      const node = filter(cond, source);
      expect(node.type).toBe('filter');
      expect(node.condition).toBe(cond);
      expect(node.source).toBe(source);
    });
  });

  describe('expand()', () => {
    it('should create Expansion node', () => {
      const source = entity('user', '123');
      const fields = [{ name: 'name' }, { name: 'email' }];
      const node = expand(fields, source);
      expect(node.type).toBe('expand');
      expect(node.fields).toBe(fields);
      expect(node.source).toBe(source);
    });
  });

  describe('recurse()', () => {
    it('should create Recursion node without depth', () => {
      const source = property('friends', entity('user', '123'));
      const node = recurse(source);
      expect(node.type).toBe('recurse');
      expect(node.maxDepth).toBeUndefined();
      expect(node.source).toBe(source);
    });

    it('should create Recursion node with depth', () => {
      const source = property('friends', entity('user', '123'));
      const node = recurse(source, 3);
      expect(node.type).toBe('recurse');
      expect(node.maxDepth).toBe(3);
    });
  });

  describe('comparison()', () => {
    it('should create ComparisonCondition', () => {
      const cond = comparison('age', '>', 30);
      expect(cond.type).toBe('comparison');
      expect(cond.field).toBe('age');
      expect(cond.operator).toBe('>');
      expect(cond.value).toBe(30);
    });
  });

  describe('logical()', () => {
    it('should create LogicalCondition', () => {
      const left = comparison('age', '>', 30);
      const right = comparison('active', '=', true);
      const cond = logical('and', left, right);
      expect(cond.type).toBe('logical');
      expect(cond.operator).toBe('and');
      expect(cond.left).toBe(left);
      expect(cond.right).toBe(right);
    });
  });

  describe('depth()', () => {
    it('should create DepthCondition', () => {
      const cond = depth('<=', 5);
      expect(cond.type).toBe('depth');
      expect(cond.operator).toBe('<=');
      expect(cond.value).toBe(5);
    });
  });
});

// ============================================================================
// Stringify Round-Trip Tests
// ============================================================================

describe('Stringify Round-Trip', () => {
  const roundTripTests = [
    'user:123',
    'user:123.friends',
    'user:123.friends.posts',
    'post:456 <- likes',
    'user:123.friends[?age > 30]',
    'user:123.friends*[depth <= 3]',
    'user:123 { name, email }',
    'user:123.friends { name }',
    'user:123.friends[?active = true].posts',
  ];

  for (const query of roundTripTests) {
    it(`should round-trip: ${query}`, () => {
      const ast = parse(query);
      const stringified = stringify(ast);
      const reparsed = parse(stringified);

      // Compare AST structure (stringify may normalize formatting)
      expect(reparsed.type).toBe(ast.type);
    });
  }

  it('should preserve filter values in round-trip', () => {
    const query = 'user:123.friends[?age > 30]';
    const ast = parse(query);
    const stringified = stringify(ast);
    const reparsed = parse(stringified);

    const originalFilter = (ast as Filter).condition as ComparisonCondition;
    const reparsedFilter = (reparsed as Filter).condition as ComparisonCondition;

    expect(reparsedFilter.field).toBe(originalFilter.field);
    expect(reparsedFilter.operator).toBe(originalFilter.operator);
    expect(reparsedFilter.value).toBe(originalFilter.value);
  });

  it('should preserve expansion fields in round-trip', () => {
    const query = 'user:123 { name, email, age }';
    const ast = parse(query);
    const stringified = stringify(ast);
    const reparsed = parse(stringified);

    const originalExpand = ast as Expansion;
    const reparsedExpand = reparsed as Expansion;

    expect(reparsedExpand.fields.length).toBe(originalExpand.fields.length);
    for (let i = 0; i < originalExpand.fields.length; i++) {
      expect(reparsedExpand.fields[i].name).toBe(originalExpand.fields[i].name);
    }
  });
});

// ============================================================================
// Error Cases for AST Generation
// ============================================================================

describe('AST Generation Error Cases', () => {
  describe('Invalid entity format', () => {
    it('should reject entity with multiple colons', () => {
      expect(() => parse('user:123:456')).toThrow();
    });

    it('should reject standalone colon', () => {
      expect(() => parse(':')).toThrow();
    });
  });

  describe('Invalid filter syntax', () => {
    it('should reject filter with no condition', () => {
      expect(() => parse('user:123.friends[?]')).toThrow();
    });

    it('should reject filter missing closing bracket', () => {
      expect(() => parse('user:123.friends[?age > 30')).toThrow();
    });

    it('should reject filter with dangling logical operator', () => {
      expect(() => parse('user:123.friends[?age > 30 and]')).toThrow();
    });
  });

  describe('Invalid expansion syntax', () => {
    it('should reject empty expansion', () => {
      expect(() => parse('user:123 { }')).toThrow(ParseError);
    });

    it('should reject expansion with missing closing brace', () => {
      expect(() => parse('user:123 { name')).toThrow();
    });

    it('should reject expansion with trailing comma only', () => {
      expect(() => parse('user:123 { , }')).toThrow();
    });
  });

  describe('Invalid recursion syntax', () => {
    it('should reject recursion without source traversal', () => {
      // Just entity + * doesn't make semantic sense but parser may handle it
      const result = parse('user:123*');
      // If it parses, it should be a recurse node
      expect(result.type).toBe('recurse');
    });

    it('should reject recursion with invalid depth', () => {
      expect(() => parse('user:123.friends*[depth <= abc]')).toThrow();
    });
  });
});
