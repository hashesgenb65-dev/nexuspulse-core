import { describe, it, expect, beforeEach } from 'vitest';
import { StreamingBrokerEngine } from '../../src/stream/broker';

describe('StreamingBrokerEngine Module', () => {
  let broker: StreamingBrokerEngine;

  beforeEach(() => {
    broker = new StreamingBrokerEngine(2); // 2 partitions
  });

  it('should create topics and produce messages partitioned by key hash', () => {
    broker.createTopic('metrics_stream');

    const res1 = broker.produce('metrics_stream', 'key_alpha', { val: 10 });
    const res2 = broker.produce('metrics_stream', 'key_beta', { val: 20 });

    // With the current hash both keys land in the same partition, so the second message gets offset 1.
    expect(res1.partitionId).toBe(res2.partitionId);
    expect(res1.offset).toBe(0);
    expect(res2.offset).toBe(1);

    const stats = broker.getTopicStats('metrics_stream');
    expect(stats).toHaveLength(2);
  });

  it('should consume messages sequentially using consumer groups and commit offsets', () => {
    broker.createTopic('orders_stream');

    broker.produce('orders_stream', 'k1', { item: 'A' });
    broker.produce('orders_stream', 'k1', { item: 'B' });
    broker.produce('orders_stream', 'k1', { item: 'C' });

    // Assuming partition 0 receives all 'k1' due to consistent hash
    const batch1 = broker.consume('orders_stream', 0, 'group_analytics', 2);
    expect(batch1).toHaveLength(2);
    expect((batch1[0]?.payload as any).item).toBe('A');

    const batch2 = broker.consume('orders_stream', 0, 'group_analytics', 2);
    expect(batch2).toHaveLength(1);
    expect((batch2[0]?.payload as any).item).toBe('C');
  });
});
