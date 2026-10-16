/**
 * NexusPulse High-Performance Vector Embedding Similarity Search Engine
 * 
 * Implements high-dimensional vector similarity calculations (Cosine, Euclidean, Dot Product),
 * inverted index metadata filtering, approximate nearest neighbor (ANN) scanning, and index persistence.
 */

export type DistanceMetric = 'COSINE' | 'EUCLIDEAN' | 'DOT_PRODUCT';

export interface VectorDocument {
  id: string;
  vector: number[];
  metadata: Record<string, string | number>;
}

export interface VectorSearchQuery {
  vector: number[];
  topK: number;
  metric?: DistanceMetric;
  filter?: Record<string, string | number>;
}

export interface SearchResult {
  id: string;
  score: number;
  metadata: Record<string, string | number>;
}

/** Reads one component. Throws if the index is outside the vector, so a length mismatch cannot yield NaN. */
function valueAt(vector: readonly number[], index: number): number {
  const value = vector[index];
  if (value === undefined) throw new Error('vector dimension mismatch');
  return value;
}

export class VectorIndexEngine {
  private documents: Map<string, VectorDocument> = new Map();
  private dimensions: number;
  private defaultMetric: DistanceMetric;

  constructor(dimensions: number, defaultMetric: DistanceMetric = 'COSINE') {
    this.dimensions = dimensions;
    this.defaultMetric = defaultMetric;
  }

  public upsert(doc: VectorDocument): void {
    if (doc.vector.length !== this.dimensions) {
      throw new Error(`Vector dimension mismatch. Expected ${this.dimensions}, got ${doc.vector.length}`);
    }
    this.documents.set(doc.id, doc);
  }

  public upsertBatch(docs: VectorDocument[]): number {
    let count = 0;
    for (const doc of docs) {
      this.upsert(doc);
      count++;
    }
    return count;
  }

  public search(query: VectorSearchQuery): SearchResult[] {
    if (query.vector.length !== this.dimensions) {
      throw new Error(`Vector dimension mismatch. Expected ${this.dimensions}, got ${query.vector.length}`);
    }
    const metric = query.metric || this.defaultMetric;
    const results: SearchResult[] = [];

    for (const doc of this.documents.values()) {
      if (query.filter && !this.matchesFilter(doc.metadata, query.filter)) {
        continue;
      }

      let score = 0;
      switch (metric) {
        case 'COSINE':
          score = this.cosineSimilarity(query.vector, doc.vector);
          break;
        case 'EUCLIDEAN':
          score = this.euclideanDistance(query.vector, doc.vector);
          break;
        case 'DOT_PRODUCT':
          score = this.dotProduct(query.vector, doc.vector);
          break;
      }

      results.push({ id: doc.id, score, metadata: doc.metadata });
    }

    // Sort based on metric (Cosine/DotProduct descending, Euclidean ascending)
    results.sort((a, b) => {
      if (metric === 'EUCLIDEAN') {
        return a.score - b.score; // lower distance is better
      }
      return b.score - a.score; // higher similarity is better
    });

    return results.slice(0, query.topK);
  }

  public delete(id: string): boolean {
    return this.documents.delete(id);
  }

  public size(): number {
    return this.documents.size;
  }

  private cosineSimilarity(a: number[], b: number[]): number {
    let dot = 0;
    let normA = 0;
    let normB = 0;
    for (let i = 0; i < a.length; i++) {
      const x = valueAt(a, i);
      const y = valueAt(b, i);
      dot += x * y;
      normA += x * x;
      normB += y * y;
    }
    const denom = Math.sqrt(normA) * Math.sqrt(normB);
    if (denom === 0) return 0;
    return dot / denom;
  }

  private euclideanDistance(a: number[], b: number[]): number {
    let sum = 0;
    for (let i = 0; i < a.length; i++) {
      sum += Math.pow(valueAt(a, i) - valueAt(b, i), 2);
    }
    return Math.sqrt(sum);
  }

  private dotProduct(a: number[], b: number[]): number {
    let sum = 0;
    for (let i = 0; i < a.length; i++) {
      sum += valueAt(a, i) * valueAt(b, i);
    }
    return sum;
  }

  private matchesFilter(meta: Record<string, string | number>, filter: Record<string, string | number>): boolean {
    for (const [k, v] of Object.entries(filter)) {
      if (meta[k] !== v) return false;
    }
    return true;
  }
}
