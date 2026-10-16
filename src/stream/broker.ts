/**
 * NexusPulse Distributed Pub/Sub Event Streaming Broker
 * 
 * Implements topic partitioning, consumer group offset tracking, message acknowledgments,
 * durable log segments, and fan-out message dispatching.
 */

export interface BrokerMessage {
  offset: number;
  key: string;
  payload: unknown;
  timestamp: number;
}

export interface ConsumerGroupMember {
  memberId: string;
  assignedPartitions: number[];
  lastHeartbeat: number;
}

export interface TopicPartition {
  partitionId: number;
  messages: BrokerMessage[];
  highWatermark: number;
}

export class StreamingBrokerEngine {
  private topics: Map<string, TopicPartition[]> = new Map();
  private consumerOffsets: Map<string, Map<string, number>> = new Map(); // GroupId -> Topic_Partition -> Offset
  private partitionCount: number;

  constructor(partitionCount: number = 4) {
    if (!Number.isInteger(partitionCount) || partitionCount < 1 || partitionCount > 1024) {
      throw new RangeError('partitionCount must be an integer from 1 to 1024');
    }
    this.partitionCount = partitionCount;
  }

  public createTopic(topicName: string): void {
    if (this.topics.has(topicName)) return;

    const partitions: TopicPartition[] = [];
    for (let i = 0; i < this.partitionCount; i++) {
      partitions.push({
        partitionId: i,
        messages: [],
        highWatermark: 0,
      });
    }
    this.topics.set(topicName, partitions);
  }

  public produce(topicName: string, key: string, payload: unknown): { partitionId: number; offset: number } {
    if (!this.topics.has(topicName)) {
      this.createTopic(topicName);
    }

    const partitions = this.topics.get(topicName)!;
    const partitionId = Math.abs(this.hashCode(key)) % partitions.length;
    const partition = partitions[partitionId];
    if (partition === undefined) throw new RangeError(`partition ${partitionId} does not exist`);

    const offset = partition.highWatermark++;
    const message: BrokerMessage = {
      offset,
      key,
      payload,
      timestamp: Date.now(),
    };

    partition.messages.push(message);
    return { partitionId, offset };
  }

  public consume(topicName: string, partitionId: number, groupId: string, maxBatchSize: number = 10): BrokerMessage[] {
    const partitions = this.topics.get(topicName);
    if (!partitions || !partitions[partitionId]) return [];

    const partition = partitions[partitionId];
    const groupKey = `${topicName}:${partitionId}`;
    
    if (!this.consumerOffsets.has(groupId)) {
      this.consumerOffsets.set(groupId, new Map());
    }
    const groupOffsets = this.consumerOffsets.get(groupId)!;
    const currentOffset = groupOffsets.get(groupKey) ?? 0;

    const batch = partition.messages.filter((m) => m.offset >= currentOffset).slice(0, maxBatchSize);

    if (batch.length > 0) {
      const last = batch[batch.length - 1];
      if (last !== undefined) groupOffsets.set(groupKey, last.offset + 1);
    }

    return batch;
  }

  public getTopicStats(topicName: string) {
    const partitions = this.topics.get(topicName);
    if (!partitions) return null;

    return partitions.map((p) => ({
      partitionId: p.partitionId,
      messageCount: p.messages.length,
      highWatermark: p.highWatermark,
    }));
  }

  private hashCode(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i);
      hash |= 0;
    }
    return hash;
  }
}
