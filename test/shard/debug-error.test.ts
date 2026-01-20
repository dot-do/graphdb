import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { ShardDO } from '../../src/shard/shard-do.js';
import { ObjectType, createEntityId, createPredicate, createTransactionId } from '../../src/core/types.js';
import { typedObjectToJson } from '../../src/core/type-converters.js';
import type { Triple, TypedObject, StringTypedObject } from '../../src/core/triple.js';

const VALID_TX_ID = '01ARZ3NDEKTSV4RRFFQ69G5FAV';

function createTestTriple(
  subjectSuffix: string,
  predicate: string,
  objectType: ObjectType,
  value: unknown,
  txIdSuffix = VALID_TX_ID
): Triple {
  const object = { type: objectType, value } as StringTypedObject;

  return {
    subject: createEntityId(`https://example.com/entity/${subjectSuffix}`),
    predicate: createPredicate(predicate),
    object,
    timestamp: BigInt(Date.now()),
    txId: createTransactionId(txIdSuffix),
  };
}

function tripleToHttpBody(triple: Triple): Record<string, unknown> {
  return {
    subject: triple.subject,
    predicate: triple.predicate,
    object: typedObjectToJson(triple.object),
    timestamp: Number(triple.timestamp),
    txId: triple.txId,
  };
}

describe('Debug Error Test', () => {
  it('should show the actual error from /triples POST', async () => {
    const id = env.SHARD.idFromName('debug-error-test-' + Date.now());
    const stub = env.SHARD.get(id);

    await runInDurableObject(stub, async (instance: ShardDO, state: DurableObjectState) => {
      const triple = createTestTriple('person1', 'name', ObjectType.STRING, 'John Doe');
      const body = tripleToHttpBody(triple);
      console.log('Request body:', JSON.stringify(body, null, 2));

      const response = await instance.fetch(
        new Request('https://shard-do/triples', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body),
        })
      );

      const text = await response.text();
      console.log('Response status:', response.status);
      console.log('Response body:', text);

      // If 400, show the error
      if (response.status === 400) {
        console.error('ERROR DETAILS:', text);
      }

      expect(response.status).toBe(201);
    });
  });
});
