import { describe, it, expect } from 'vitest';
import {
  type Entity,
  urlToStoragePath,
  storagePathToUrl,
  resolveNamespace,
  parseEntityId,
  createEntity,
  validateEntity,
  isValidFieldName,
} from '../../src/core/entity';
import { createEntityId, type EntityId } from '../../src/core/types';

describe('urlToStoragePath', () => {
  it('should convert URL to reverse-domain storage path', () => {
    expect(urlToStoragePath('https://example.com/crm/acme/customer/123')).toBe(
      '.com/.example/crm/acme/customer/123'
    );
  });

  it('should handle subdomains correctly', () => {
    expect(urlToStoragePath('https://api.example.com/users/456')).toBe(
      '.com/.example/.api/users/456'
    );
  });

  it('should handle http protocol', () => {
    expect(urlToStoragePath('http://example.com/path')).toBe(
      '.com/.example/path'
    );
  });

  it('should handle root path', () => {
    // Note: URL parser normalizes https://example.com to https://example.com/
    expect(urlToStoragePath('https://example.com/')).toBe('.com/.example/');
    expect(urlToStoragePath('https://example.com')).toBe('.com/.example/');
  });

  it('should handle deep paths', () => {
    expect(urlToStoragePath('https://example.com/a/b/c/d/e')).toBe(
      '.com/.example/a/b/c/d/e'
    );
  });

  it('should handle different TLDs', () => {
    expect(urlToStoragePath('https://example.org/path')).toBe(
      '.org/.example/path'
    );
    expect(urlToStoragePath('https://example.co.uk/path')).toBe(
      '.uk/.co/.example/path'
    );
  });

  it('should throw for invalid URLs', () => {
    expect(() => urlToStoragePath('not-a-url')).toThrow();
    expect(() => urlToStoragePath('ftp://example.com/path')).toThrow();
  });
});

describe('storagePathToUrl', () => {
  it('should convert storage path back to URL', () => {
    expect(storagePathToUrl('.com/.example/crm/acme/customer/123')).toBe(
      'https://example.com/crm/acme/customer/123'
    );
  });

  it('should handle subdomains correctly', () => {
    expect(storagePathToUrl('.com/.example/.api/users/456')).toBe(
      'https://api.example.com/users/456'
    );
  });

  it('should handle root path', () => {
    expect(storagePathToUrl('.com/.example/')).toBe('https://example.com/');
    // Without trailing slash in storage path, we still return without trailing slash
    expect(storagePathToUrl('.com/.example')).toBe('https://example.com');
  });

  it('should handle different TLDs', () => {
    expect(storagePathToUrl('.org/.example/path')).toBe(
      'https://example.org/path'
    );
    expect(storagePathToUrl('.uk/.co/.example/path')).toBe(
      'https://example.co.uk/path'
    );
  });

  it('should round-trip correctly with urlToStoragePath', () => {
    const testUrls = [
      'https://example.com/crm/acme/customer/123',
      'https://api.example.com/users/456',
      'https://example.org/path/to/resource',
      'https://example.com/',
      // Note: https://example.com (no trailing slash) normalizes to https://example.com/
      'https://sub.domain.example.com/deep/path',
    ];

    for (const url of testUrls) {
      const storagePath = urlToStoragePath(url);
      const roundTripped = storagePathToUrl(storagePath);
      expect(roundTripped).toBe(url);
    }
  });
});

describe('resolveNamespace', () => {
  it('should extract namespace, context, and localId from URL', () => {
    const result = resolveNamespace('https://example.com/crm/acme/customer/123');
    expect(result.namespace).toBe('https://example.com');
    expect(result.context).toBe('https://example.com/crm/acme/customer');
    expect(result.localId).toBe('123');
  });

  it('should handle URLs with minimal path', () => {
    const result = resolveNamespace('https://example.com/users/1');
    expect(result.namespace).toBe('https://example.com');
    expect(result.context).toBe('https://example.com/users');
    expect(result.localId).toBe('1');
  });

  it('should handle URLs with trailing slash', () => {
    const result = resolveNamespace('https://example.com/users/1/');
    expect(result.namespace).toBe('https://example.com');
    expect(result.context).toBe('https://example.com/users/1');
    expect(result.localId).toBe('');
  });

  it('should handle subdomains', () => {
    const result = resolveNamespace('https://api.example.com/v1/resource/abc');
    expect(result.namespace).toBe('https://api.example.com');
    expect(result.context).toBe('https://api.example.com/v1/resource');
    expect(result.localId).toBe('abc');
  });

  it('should throw for invalid URLs', () => {
    expect(() => resolveNamespace('not-a-url')).toThrow();
    expect(() => resolveNamespace('ftp://example.com/path')).toThrow();
  });
});

describe('parseEntityId', () => {
  it('should parse protocol, hostname, path segments, and localId', () => {
    const result = parseEntityId(
      'https://example.com/crm/acme/customer/123' as EntityId
    );
    expect(result.protocol).toBe('https:');
    expect(result.hostname).toBe('example.com');
    expect(result.path).toEqual(['crm', 'acme', 'customer', '123']);
    expect(result.localId).toBe('123');
  });

  it('should handle http protocol', () => {
    const result = parseEntityId('http://example.com/path/id' as EntityId);
    expect(result.protocol).toBe('http:');
    expect(result.hostname).toBe('example.com');
    expect(result.path).toEqual(['path', 'id']);
    expect(result.localId).toBe('id');
  });

  it('should handle subdomains', () => {
    const result = parseEntityId(
      'https://api.example.com/v1/users/uuid-123' as EntityId
    );
    expect(result.protocol).toBe('https:');
    expect(result.hostname).toBe('api.example.com');
    expect(result.path).toEqual(['v1', 'users', 'uuid-123']);
    expect(result.localId).toBe('uuid-123');
  });

  it('should handle root path', () => {
    const result = parseEntityId('https://example.com/' as EntityId);
    expect(result.protocol).toBe('https:');
    expect(result.hostname).toBe('example.com');
    expect(result.path).toEqual([]);
    expect(result.localId).toBe('');
  });

  it('should handle minimal path', () => {
    const result = parseEntityId('https://example.com/id' as EntityId);
    expect(result.protocol).toBe('https:');
    expect(result.hostname).toBe('example.com');
    expect(result.path).toEqual(['id']);
    expect(result.localId).toBe('id');
  });
});

describe('createEntity', () => {
  it('should create an entity with $id, $type, and $context', () => {
    const entity = createEntity(
      'https://example.com/users/123' as EntityId,
      'Person',
      { name: 'John', age: 30 }
    );

    expect(entity.$id).toBe('https://example.com/users/123');
    expect(entity.$type).toBe('Person');
    expect(entity.$context).toBe('https://example.com/users');
  });

  it('should set _namespace and _localId correctly', () => {
    const entity = createEntity(
      'https://example.com/crm/customer/456' as EntityId,
      'Customer',
      {}
    );

    expect(entity._namespace).toBe('https://example.com');
    expect(entity._localId).toBe('456');
  });

  it('should include all provided properties', () => {
    const entity = createEntity(
      'https://example.com/items/1' as EntityId,
      'Item',
      { name: 'Widget', price: 9.99, inStock: true }
    );

    expect(entity.name).toBe('Widget');
    expect(entity.price).toBe(9.99);
    expect(entity.inStock).toBe(true);
  });

  it('should support multiple types as array', () => {
    const entity = createEntity(
      'https://example.com/entities/1' as EntityId,
      ['Person', 'Employee'],
      { name: 'Jane' }
    );

    expect(entity.$type).toEqual(['Person', 'Employee']);
  });

  it('should reject properties with colons in field names', () => {
    expect(() =>
      createEntity('https://example.com/items/1' as EntityId, 'Item', {
        'schema:name': 'Widget',
      })
    ).toThrow();
  });

  it('should reject reserved field names in properties', () => {
    expect(() =>
      createEntity('https://example.com/items/1' as EntityId, 'Item', {
        $id: 'override',
      })
    ).toThrow();

    expect(() =>
      createEntity('https://example.com/items/1' as EntityId, 'Item', {
        _namespace: 'override',
      })
    ).toThrow();
  });
});

describe('validateEntity', () => {
  it('should return valid:true for valid entity', () => {
    const entity = createEntity(
      'https://example.com/users/1' as EntityId,
      'User',
      { name: 'Test' }
    );

    const result = validateEntity(entity);
    expect(result.valid).toBe(true);
    expect(result.errors).toEqual([]);
  });

  it('should reject entity with colons in field names', () => {
    const entity = {
      $id: 'https://example.com/users/1' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
      _namespace: 'https://example.com',
      _localId: '1',
      'schema:name': 'Test', // Invalid - contains colon
    } as unknown as Entity;

    const result = validateEntity(entity);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
    expect(result.errors[0]).toContain('colon');
  });

  it('should reject entity with missing $id', () => {
    const entity = {
      $type: 'User',
      $context: 'https://example.com/users',
      _namespace: 'https://example.com',
      _localId: '1',
    } as unknown as Entity;

    const result = validateEntity(entity);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('$id is required');
  });

  it('should reject entity with missing $type', () => {
    const entity = {
      $id: 'https://example.com/users/1' as EntityId,
      $context: 'https://example.com/users',
      _namespace: 'https://example.com',
      _localId: '1',
    } as unknown as Entity;

    const result = validateEntity(entity);
    expect(result.valid).toBe(false);
    expect(result.errors).toContain('$type is required');
  });

  it('should reject entity with invalid $id URL', () => {
    const entity = {
      $id: 'not-a-url' as EntityId,
      $type: 'User',
      $context: 'https://example.com/users',
      _namespace: 'https://example.com',
      _localId: '1',
    } as unknown as Entity;

    const result = validateEntity(entity);
    expect(result.valid).toBe(false);
    expect(result.errors.some((e) => e.includes('$id'))).toBe(true);
  });

  it('should collect multiple validation errors', () => {
    const entity = {
      $type: 'User',
      'schema:name': 'Test', // Invalid field name
      'rdf:type': 'Thing', // Another invalid field name
    } as unknown as Entity;

    const result = validateEntity(entity);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThanOrEqual(2);
  });
});

describe('isValidFieldName', () => {
  it('should return true for valid field names', () => {
    expect(isValidFieldName('name')).toBe(true);
    expect(isValidFieldName('firstName')).toBe(true);
    expect(isValidFieldName('first_name')).toBe(true);
    expect(isValidFieldName('age')).toBe(true);
    expect(isValidFieldName('_private')).toBe(true);
    expect(isValidFieldName('CONSTANT')).toBe(true);
    expect(isValidFieldName('value123')).toBe(true);
  });

  it('should return false for field names with colons', () => {
    expect(isValidFieldName('schema:name')).toBe(false);
    expect(isValidFieldName('rdf:type')).toBe(false);
    expect(isValidFieldName('foaf:knows')).toBe(false);
    expect(isValidFieldName('prefix:suffix:extra')).toBe(false);
    expect(isValidFieldName(':startWithColon')).toBe(false);
    expect(isValidFieldName('endWithColon:')).toBe(false);
  });

  it('should return false for empty string', () => {
    expect(isValidFieldName('')).toBe(false);
  });

  it('should return false for names with spaces', () => {
    expect(isValidFieldName('field name')).toBe(false);
    expect(isValidFieldName('field\tname')).toBe(false);
    expect(isValidFieldName('field\nname')).toBe(false);
  });

  it('should return false for names starting with numbers', () => {
    expect(isValidFieldName('123field')).toBe(false);
    expect(isValidFieldName('1name')).toBe(false);
  });

  it('should allow $ prefix for reserved fields', () => {
    expect(isValidFieldName('$id')).toBe(true);
    expect(isValidFieldName('$type')).toBe(true);
    expect(isValidFieldName('$context')).toBe(true);
  });

  it('should return false for names with special characters', () => {
    expect(isValidFieldName('field@name')).toBe(false);
    expect(isValidFieldName('field#name')).toBe(false);
    expect(isValidFieldName('field.name')).toBe(false);
    expect(isValidFieldName('field-name')).toBe(false);
  });
});
