import type { Topic } from 'generated-sources';
import {
  buildProjectedTopic,
  generateBalancedAssignments,
  getBrokerAssignmentCounts,
} from 'components/Topics/Topic/Reassignment/assignmentGenerator';

const topic: Topic = {
  name: 'orders',
  partitionCount: 4,
  replicationFactor: 2,
  partitions: [0, 1, 2, 3].map((partition) => ({
    partition,
    leader: 1,
    offsetMin: 0,
    offsetMax: 10,
    replicas: [
      { broker: 1, leader: true, inSync: true },
      { broker: 2, leader: false, inSync: true },
    ],
  })),
};

describe('assignment planner', () => {
  it('generates deterministic balanced assignments across selected brokers', () => {
    expect(generateBalancedAssignments(topic, [3, 1, 2], 2)).toEqual([
      { topic: 'orders', partition: 0, replicas: [1, 2] },
      { topic: 'orders', partition: 1, replicas: [2, 3] },
      { topic: 'orders', partition: 2, replicas: [3, 1] },
      { topic: 'orders', partition: 3, replicas: [1, 2] },
    ]);
  });

  it('generates assignments only for the selected partitions', () => {
    expect(generateBalancedAssignments(topic, [3, 1, 2], 2, [0, 3])).toEqual([
      { topic: 'orders', partition: 0, replicas: [1, 2] },
      { topic: 'orders', partition: 3, replicas: [2, 3] },
    ]);
  });

  it('requires at least one selected partition', () => {
    expect(() => generateBalancedAssignments(topic, [1, 2], 2, [])).toThrow(
      'Select at least one partition'
    );
  });

  it.each([
    {
      brokers: [],
      replicationFactor: 1,
      message: 'Select at least one broker',
    },
    {
      brokers: [1, 2],
      replicationFactor: 3,
      message: 'Replication factor cannot exceed selected brokers',
    },
    {
      brokers: [1, 2],
      replicationFactor: 0,
      message: 'Replication factor must be at least 1',
    },
  ])(
    'rejects invalid planner input: $message',
    ({ brokers, replicationFactor, message }) => {
      expect(() =>
        generateBalancedAssignments(topic, brokers, replicationFactor)
      ).toThrow(message);
    }
  );

  it('builds a projected topic with the first target replica as preferred leader', () => {
    const projected = buildProjectedTopic(topic, [
      { topic: 'orders', partition: 0, replicas: [2, 3] },
      { topic: 'orders', partition: 1, replicas: [3, 1] },
      { topic: 'orders', partition: 2, replicas: [1, 2] },
      { topic: 'orders', partition: 3, replicas: [2, 3] },
    ]);

    expect(projected.replicationFactor).toBe(2);
    expect(projected.underReplicatedPartitions).toBe(0);
    expect(projected.partitions?.[0]).toMatchObject({
      partition: 0,
      leader: 2,
      offsetMax: 10,
      replicas: [
        { broker: 2, leader: true, inSync: true },
        { broker: 3, leader: false, inSync: true },
      ],
    });
  });

  it('counts assigned replicas even when a replica is out of sync', () => {
    const topicWithOutOfSyncReplica: Topic = {
      ...topic,
      partitions: topic.partitions?.map((partition, index) => ({
        ...partition,
        replicas: partition.replicas?.map((replica) => ({
          ...replica,
          inSync: index === 0 && replica.broker === 2 ? false : replica.inSync,
        })),
      })),
    };

    expect(getBrokerAssignmentCounts(topicWithOutOfSyncReplica)).toEqual([
      { brokerId: 1, replicas: 4, leaders: 4 },
      { brokerId: 2, replicas: 4, leaders: 0 },
    ]);
  });
});
