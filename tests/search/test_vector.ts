import { describe, it, expect, beforeEach } from 'vitest';
import { VectorIndexEngine } from '../../src/search/vector_index';

describe('VectorIndexEngine Module', () => {
  let index: VectorIndexEngine;

  beforeEach(() => {
    index = new VectorIndexEngine(3, 'COSINE');
  });

  it('should upsert and retrieve nearest neighbor vector search results', () => {
    index.upsert({ id: 'doc_1', vector: [1.0, 0.0, 0.0], metadata: { category: 'tech' } });
    index.upsert({ id: 'doc_2', vector: [0.9, 0.1, 0.0], metadata: { category: 'tech' } });
    index.upsert({ id: 'doc_3', vector: [0.0, 1.0, 1.0], metadata: { category: 'finance' } });

    const results = index.search({ vector: [1.0, 0.0, 0.0], topK: 2 });

    expect(results).toHaveLength(2);
    expect(results[0]?.id).toBe('doc_1');
    expect(results[1]?.id).toBe('doc_2');
  });

  it('should filter search results by metadata attributes', () => {
    index.upsert({ id: 'doc_a', vector: [1.0, 1.0, 1.0], metadata: { env: 'prod' } });
    index.upsert({ id: 'doc_b', vector: [1.0, 1.0, 0.9], metadata: { env: 'dev' } });

    const results = index.search({
      vector: [1.0, 1.0, 1.0],
      topK: 5,
      filter: { env: 'prod' },
    });

    expect(results).toHaveLength(1);
    expect(results[0]?.id).toBe('doc_a');
  });

  it('should compute euclidean distance correctly', () => {
    index.upsert({ id: 'origin', vector: [0, 0, 0], metadata: {} });
    index.upsert({ id: 'point', vector: [3, 4, 0], metadata: {} });

    const results = index.search({ vector: [0, 0, 0], topK: 2, metric: 'EUCLIDEAN' });
    // The origin is at distance 0 from itself, so it ranks first; the point is 5 away (3-4-5 triangle).
    expect(results[0]?.id).toBe('origin');
    expect(results[0]?.score).toBe(0);
    expect(results[1]?.id).toBe('point');
    expect(results[1]?.score).toBe(5);
  });
});
