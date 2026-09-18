import type { Topic } from 'generated-sources';
import {
  getBrokerTopicDistribution,
  getTopicDistributionMetrics,
} from 'components/Topics/lib/topicDistributionMetrics';

const balancedTopic: Topic = {
  name: 'balanced-topic',
  partitionCount: 3,
  replicationFactor: 3,
  underReplicatedPartitions: 0,
  bytesInPerSec: 1024,
  bytesOutPerSec: 2048,
  partitions: [
    {
      partition: 0,
      leader: 1,
      offsetMin: 0,
      offsetMax: 100,
      replicas: [
        { broker: 1, leader: true, inSync: true },
        { broker: 2, leader: false, inSync: true },
        { broker: 3, leader: false, inSync: true },
      ],
    },
    {
      partition: 1,
      leader: 2,
      offsetMin: 0,
      offsetMax: 200,
      replicas: [
        { broker: 2, leader: true, inSync: true },
        { broker: 3, leader: false, inSync: true },
        { broker: 1, leader: false, inSync: true },
      ],
    },
    {
      partition: 2,
      leader: 3,
      offsetMin: 0,
      offsetMax: 300,
      replicas: [
        { broker: 3, leader: true, inSync: true },
        { broker: 1, leader: false, inSync: true },
        { broker: 2, leader: false, inSync: true },
      ],
    },
  ],
};

describe('topic distribution metrics', () => {
  it('matches CMAK percentages for a balanced topic', () => {
    expect(getTopicDistributionMetrics(balancedTopic, 4)).toEqual({
      topicBrokers: 3,
      brokerSpreadPercentage: 75,
      brokerSkewPercentage: 0,
      brokerLeaderSkewPercentage: 0,
      preferredReplicasPercentage: 100,
      underReplicatedPercentage: 0,
      summedTopicOffsets: 600,
    });
  });

  it('reports replica and leader skew using brokers above the CMAK average', () => {
    const skewedTopic: Topic = {
      name: 'skewed-topic',
      partitionCount: 4,
      replicationFactor: 1,
      underReplicatedPartitions: 0,
      partitions: [0, 1, 2, 3].map((partition) => ({
        partition,
        leader: partition === 3 ? 2 : 1,
        offsetMin: 0,
        offsetMax: 1,
        replicas: [
          {
            broker: partition === 3 ? 2 : 1,
            leader: true,
            inSync: true,
          },
        ],
      })),
    };

    expect(getTopicDistributionMetrics(skewedTopic, 2)).toMatchObject({
      brokerSkewPercentage: 50,
      brokerLeaderSkewPercentage: 50,
    });
  });

  it('uses actual replica counts when partitions have mixed replication factors', () => {
    const mixedReplicationTopic: Topic = {
      name: 'mixed-replication-topic',
      partitionCount: 4,
      replicationFactor: 2,
      underReplicatedPartitions: 0,
      partitions: [
        { partition: 0, replicas: [1, 2, 3] },
        { partition: 1, replicas: [2, 3, 1] },
        { partition: 2, replicas: [1, 2] },
        { partition: 3, replicas: [2, 1] },
      ].map(({ partition, replicas }) => ({
        partition,
        leader: replicas[0],
        offsetMin: 0,
        offsetMax: 1,
        replicas: replicas.map((broker, index) => ({
          broker,
          leader: index === 0,
          inSync: true,
        })),
      })),
    };

    expect(getTopicDistributionMetrics(mixedReplicationTopic, 3)).toMatchObject(
      {
        brokerSkewPercentage: 0,
      }
    );
  });

  it('uses in-sync replicas for broker distribution and placement health', () => {
    const underReplicatedTopic: Topic = {
      name: 'under-replicated-topic',
      partitionCount: 2,
      replicationFactor: 2,
      underReplicatedPartitions: 1,
      partitions: [
        {
          partition: 0,
          leader: 2,
          offsetMin: 0,
          offsetMax: 10,
          replicas: [
            { broker: 1, leader: false, inSync: false },
            { broker: 2, leader: true, inSync: true },
          ],
        },
        {
          partition: 1,
          leader: 2,
          offsetMin: 0,
          offsetMax: 20,
          replicas: [
            { broker: 2, leader: true, inSync: true },
            { broker: 1, leader: false, inSync: true },
          ],
        },
      ],
    };

    expect(getTopicDistributionMetrics(underReplicatedTopic, 3)).toMatchObject({
      topicBrokers: 2,
      preferredReplicasPercentage: 50,
      underReplicatedPercentage: 50,
    });
    expect(getBrokerTopicDistribution(underReplicatedTopic)).toEqual([
      {
        brokerId: 1,
        partitions: [1],
        leaders: [],
        skewed: false,
        leaderSkewed: false,
      },
      {
        brokerId: 2,
        partitions: [0, 1],
        leaders: [0, 1],
        skewed: false,
        leaderSkewed: true,
      },
    ]);
  });

  it('returns stable defaults when a topic has no partitions', () => {
    expect(
      getTopicDistributionMetrics(
        { name: 'empty', partitionCount: 0, partitions: [] },
        0
      )
    ).toEqual({
      topicBrokers: 0,
      brokerSpreadPercentage: 100,
      brokerSkewPercentage: 0,
      brokerLeaderSkewPercentage: 0,
      preferredReplicasPercentage: 100,
      underReplicatedPercentage: 0,
      summedTopicOffsets: 0,
    });
  });
});
