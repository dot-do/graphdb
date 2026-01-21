# GraphDB Query Language

GraphDB uses a path-based query language for graph traversals. This document provides comprehensive documentation for all query syntax features.

## Table of Contents

- [Entity Lookup](#entity-lookup)
- [Property Traversal](#property-traversal)
- [Reverse Traversal](#reverse-traversal)
- [Filtering](#filtering)
- [Expansion](#expansion)
- [Recursion](#recursion)
- [Combining Operations](#combining-operations)
- [AST Helpers](#ast-helpers)
- [Error Handling](#error-handling)

## Entity Lookup

Entity lookup is the starting point for all queries. Entities are identified by a `namespace:id` pattern.

### Syntax

```
namespace:id
```

### Examples

```typescript
import { parse } from '@dotdo/graphdb/query';

// Simple entity lookup
const ast = parse('user:123');
// Result: { type: 'entity', namespace: 'user', id: '123' }

// String IDs
const ast2 = parse('user:alice');
// Result: { type: 'entity', namespace: 'user', id: 'alice' }

// Quoted IDs (for special characters)
const ast3 = parse('user:"john-doe-123"');
// Result: { type: 'entity', namespace: 'user', id: 'john-doe-123' }
```

### Entity ID Format

| Format | Example | Description |
|--------|---------|-------------|
| Numeric | `user:123` | Simple numeric ID |
| Alphanumeric | `user:alice` | Alphanumeric ID |
| Quoted | `user:"abc-123"` | IDs with special characters |

## Property Traversal

Property traversal follows edges from one entity to another using dot notation.

### Syntax

```
entity.property
entity.property1.property2
```

### Single-Hop Traversal

```typescript
// Get all friends of user:123
const ast = parse('user:123.friends');
// Result: {
//   type: 'property',
//   name: 'friends',
//   source: { type: 'entity', namespace: 'user', id: '123' }
// }
```

### Multi-Hop Traversal

```typescript
// Get posts from friends of user:123
const ast = parse('user:123.friends.posts');

// Three-hop traversal
const ast2 = parse('user:123.friends.posts.comments');

// Get all posts from follows of follows
const ast3 = parse('user:alice.follows.follows.posts');
```

### Usage with Client SDK

```typescript
import { createGraphClient } from '@dotdo/graphdb/protocol';

const client = createGraphClient('wss://api.example.com/connect');

// Single-hop: get friends
const friends = await client.traverse('user:123', 'friends');

// Multi-hop: get posts from friends
const posts = await client.pathTraverse('user:123', ['friends', 'posts']);

// Using query string
const result = await client.query('user:123.friends.posts');
```

## Reverse Traversal

Reverse traversal finds entities that have edges pointing TO the current entity. Use the `<-` operator.

### Syntax

```
entity <- predicate
```

### Examples

```typescript
// Find all users who liked post:456
const ast = parse('post:456 <- likes');
// Result: {
//   type: 'reverse',
//   predicate: 'likes',
//   source: { type: 'entity', namespace: 'post', id: '456' }
// }

// Find all users who follow user:alice
const ast2 = parse('user:alice <- follows');

// Find comments on a post, then find authors
const ast3 = parse('post:456 <- comments');
```

### Use Cases

| Pattern | Description |
|---------|-------------|
| `post:123 <- likes` | Who liked this post? |
| `user:alice <- follows` | Who follows Alice? |
| `product:456 <- purchases` | Who purchased this product? |
| `article:789 <- comments` | What comments are on this article? |

## Filtering

Filters narrow down traversal results based on conditions. Filters are enclosed in `[?...]`.

### Syntax

```
entity.property[?condition]
```

### Comparison Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `[?status = "active"]` |
| `!=` | Not equal | `[?status != "deleted"]` |
| `>` | Greater than | `[?age > 21]` |
| `<` | Less than | `[?price < 100]` |
| `>=` | Greater than or equal | `[?score >= 80]` |
| `<=` | Less than or equal | `[?rating <= 5]` |

### Value Types

```typescript
// String comparison
parse('user:123.friends[?status = "active"]');

// Numeric comparison
parse('user:123.friends[?age > 30]');

// Boolean comparison
parse('user:123.posts[?published = true]');

// Unquoted string (simple values)
parse('user:123.friends[?role = admin]');
```

### Logical Operators

Combine conditions with `and` / `or`:

```typescript
// AND condition: age > 30 AND status = "active"
parse('user:123.friends[?age > 30 and status = "active"]');

// OR condition: role = "admin" OR role = "moderator"
parse('user:123.friends[?role = "admin" or role = "moderator"]');

// Parentheses for precedence
parse('user:123.friends[?(age > 25 and status = "active") or role = "admin"]');
```

### Filter Examples

```typescript
// Get active friends over 30
const ast = parse('user:123.friends[?age > 30 and status = "active"]');

// Get published posts from 2024
const ast2 = parse('user:alice.posts[?published = true and year >= 2024]');

// Get high-rated products under $50
const ast3 = parse('store:main.products[?rating > 4 and price < 50]');
```

## Expansion

Expansion specifies which fields to include in the response, similar to GraphQL field selection.

### Syntax

```
entity { field1, field2 }
entity { field1 { nested1, nested2 } }
```

### Basic Expansion

```typescript
// Get name and email for user:123
const ast = parse('user:123 { name, email }');
// Result: {
//   type: 'expand',
//   fields: [{ name: 'name' }, { name: 'email' }],
//   source: { type: 'entity', namespace: 'user', id: '123' }
// }
```

### Nested Expansion

```typescript
// Get user with friends' names
const ast = parse('user:123 { name, friends { name, email } }');

// Deep nesting
const ast2 = parse('user:123 { name, friends { name, posts { title, content } } }');
```

### Combining with Traversal

```typescript
// Traverse then expand
const ast = parse('user:123.friends { name, email }');

// Multi-hop with expansion
const ast2 = parse('user:123.friends.posts { title, published, author { name } }');
```

### Expansion Examples

```typescript
// Get user profile with selected fields
const result = await client.query('user:alice { name, email, avatar }');

// Get friends with their posts
const result2 = await client.query('user:alice.friends { name, posts { title } }');

// Social graph expansion
const result3 = await client.query(`
  user:alice {
    name,
    email,
    friends {
      name,
      posts {
        title,
        likes
      }
    }
  }
`);
```

## Recursion

Recursion repeats a traversal pattern to explore the graph to a specified depth.

### Syntax

```
entity.property*                    # Unbounded recursion (use with caution)
entity.property*[depth <= N]        # Bounded recursion to depth N
```

### Bounded Recursion

```typescript
// Find all friends up to 3 hops away
const ast = parse('user:123.friends*[depth <= 3]');
// Result: {
//   type: 'recurse',
//   maxDepth: 3,
//   source: {
//     type: 'property',
//     name: 'friends',
//     source: { type: 'entity', namespace: 'user', id: '123' }
//   }
// }
```

### Unbounded Recursion

```typescript
// WARNING: Use with caution - may traverse entire graph
const ast = parse('user:123.friends*');
// Result: { type: 'recurse', source: ... }
// maxDepth is undefined (no limit)
```

### Depth Constraints

| Pattern | Description |
|---------|-------------|
| `*[depth <= 1]` | Direct connections only |
| `*[depth <= 3]` | Up to 3 hops |
| `*[depth < 5]` | Less than 5 hops |
| `*` | Unlimited (dangerous) |

### Recursion Examples

```typescript
// Find extended social network (3 degrees of separation)
const network = await client.query('user:alice.friends*[depth <= 3]');

// Find all descendants in an org hierarchy
const descendants = await client.query('org:root.children*[depth <= 10]');

// Find connected components (bounded)
const connected = await client.query('node:start.connections*[depth <= 5]');
```

### Depth Limits

The executor enforces depth limits to prevent runaway queries:

- Default maximum depth: `100`
- Default traversal timeout: `30 seconds`
- Configurable via `ExecutionContext`

```typescript
// Executor will cap depth at MAX_PATH_DEPTH (100)
// even if specified as higher
const plan = planQuery(parse('user:123.friends*[depth <= 500]'));
// Will be capped to 100
```

## Combining Operations

Operations can be combined to build complex queries.

### Traversal with Filter

```typescript
// Get friends over 30 and their posts
parse('user:123.friends[?age > 30].posts');

// Filter at multiple levels
parse('user:123.friends[?active = true].posts[?published = true]');
```

### Filter with Expansion

```typescript
// Get active friends with selected fields
parse('user:123.friends[?status = "active"] { name, email }');
```

### Recursion with Expansion

```typescript
// Get social network with names
parse('user:123.friends*[depth <= 2] { name }');
```

### Complex Queries

```typescript
// Full example: Get active friends within 2 hops with their recent posts
const query = `
  user:alice
    .friends*[depth <= 2]
    [?status = "active"]
    {
      name,
      posts[?year = 2024] {
        title,
        likes
      }
    }
`;
```

## AST Helpers

The parser exports helper functions for programmatically building AST nodes.

### Factory Functions

```typescript
import {
  entity,
  property,
  reverse,
  filter,
  expand,
  recurse,
  comparison,
  logical,
  depth,
} from '@dotdo/graphdb/query';

// Build: user:123.friends[?age > 30]
const ast = filter(
  comparison('age', '>', 30),
  property('friends', entity('user', '123'))
);

// Build: user:123 { name, email }
const ast2 = expand(
  [{ name: 'name' }, { name: 'email' }],
  entity('user', '123')
);

// Build: user:123.friends*[depth <= 3]
const ast3 = recurse(
  property('friends', entity('user', '123')),
  3
);

// Build: post:456 <- likes
const ast4 = reverse('likes', entity('post', '456'));
```

### Utility Functions

```typescript
import { stringify, countHops } from '@dotdo/graphdb/query';

// Stringify AST back to query
const ast = parse('user:123.friends[?age > 30]');
console.log(stringify(ast)); // "user:123.friends[?age > 30]"

// Count traversal hops
countHops(parse('user:123'));                      // 0
countHops(parse('user:123.friends'));              // 1
countHops(parse('user:123.friends.posts'));        // 2
countHops(parse('user:123.friends*'));             // Infinity
countHops(parse('user:123.friends*[depth <= 3]')); // 3
```

## Error Handling

The parser provides detailed error messages with line and column information.

### Parse Errors

```typescript
import { parse, ParseError } from '@dotdo/graphdb/query';

try {
  parse('user');  // Missing :id
} catch (error) {
  if (error instanceof ParseError) {
    console.log(error.message);   // "Parse error at position 4..."
    console.log(error.position);  // 4
    console.log(error.line);      // 1
    console.log(error.column);    // 5
  }
}
```

### Lexer Errors

```typescript
import { LexerError, LexerErrorCode } from '@dotdo/graphdb/snippet';

try {
  parse('user:123[?"unterminated');
} catch (error) {
  if (error instanceof LexerError) {
    console.log(error.code);      // 'UNTERMINATED_STRING'
    console.log(error.position);  // { offset: 9, line: 1, column: 10 }
    console.log(error.source);    // Source snippet for context
  }
}
```

### Common Error Cases

| Query | Error |
|-------|-------|
| `user` | Missing `:id` after namespace |
| `user:123[?age > 30` | Unclosed bracket |
| `user:123 { name` | Unclosed brace |
| `post:456 <-` | Missing predicate after `<-` |
| `user:123 { }` | Empty expansion |
| `""` | Empty query |

### Depth Limit Protection

The parser has a maximum nesting depth (default: 50) to prevent stack overflow:

```typescript
// This will throw if nesting exceeds MAX_PARSER_DEPTH
try {
  parse(deeplyNestedQuery);
} catch (error) {
  if (error instanceof ParseError) {
    // "Maximum nesting depth (50) exceeded..."
  }
}
```

## Quick Reference

### Syntax Summary

```
# Entity lookup
namespace:id

# Forward traversal
entity.property
entity.prop1.prop2.prop3

# Reverse traversal
entity <- predicate

# Filtering
entity.property[?field op value]
entity.property[?cond1 and cond2]
entity.property[?cond1 or cond2]

# Expansion
entity { field1, field2 }
entity { field1 { nested } }

# Recursion
entity.property*[depth <= N]
entity.property*
```

### Operators

| Category | Operators |
|----------|-----------|
| Comparison | `=`, `!=`, `>`, `<`, `>=`, `<=` |
| Logical | `and`, `or` |
| Traversal | `.` (forward), `<-` (reverse) |
| Recursion | `*` |
| Depth | `depth <= N`, `depth < N` |

### Value Types

| Type | Example |
|------|---------|
| String (quoted) | `"hello world"` |
| String (unquoted) | `active` |
| Number | `42`, `3.14` |
| Boolean | `true`, `false` |

## See Also

- [Getting Started](./GETTING_STARTED.md) - Installation and basic usage
- [README](../README.md) - Architecture overview and API reference
