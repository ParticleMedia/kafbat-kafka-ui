import type { Topic } from 'generated-sources';

export interface BrokerTopicDistribution {
  brokerId: number;
  partitions: number[];
  leaders: number[];
  skewed: boolean;
  leaderSkewed: boolean;
}

export interface TopicDistributionMetrics {
  topicBrokers: number;
  brokerSpreadPercentage: number;
  brokerSkewPercentage: number;
  brokerLeaderSkewPercentage: number;
  preferredReplicasPercentage: number;
  underReplicatedPercentage: number;
  summedTopicOffsets: number;
}

const percentage = (count: number, total: number, emptyValue = 0) =>
  total > 0 ? Math.floor((100 * count) / total) : emptyValue;

export const getBrokerTopicDistribution = (
  topic: Topic
): BrokerTopicDistribution[] => {
  const partitions = topic.partitions ?? [];
  const partitionCount = topic.partitionCount ?? partitions.length;
  const brokerPartitions = new Map<
    number,
    { partitions: number[]; leaders: number[] }
  >();

  partitions.forEach((partition) => {
    partition.replicas
      ?.filter((replica) => replica.inSync && replica.broker !== undefined)
      .forEach((replica) => {
        const brokerId = replica.broker as number;
        const placement = brokerPartitions.get(brokerId) ?? {
          partitions: [],
          leaders: [],
        };
        placement.partitions.push(partition.partition);
        if (partition.leader === brokerId || replica.leader) {
          placement.leaders.push(partition.partition);
        }
        brokerPartitions.set(brokerId, placement);
      });
  });

  const topicBrokers = brokerPartitions.size;
  const replicaCount = [...brokerPartitions.values()].reduce(
    (total, placement) => total + placement.partitions.length,
    0
  );
  const averageReplicasPerBroker = topicBrokers
    ? Math.ceil(replicaCount / topicBrokers)
    : 0;
  const averageLeadersPerBroker = topicBrokers
    ? Math.ceil(partitionCount / topicBrokers)
    : 0;

  return [...brokerPartitions.entries()]
    .map(([brokerId, placement]) => ({
      brokerId,
      partitions: placement.partitions.sort((a, b) => a - b),
      leaders: placement.leaders.sort((a, b) => a - b),
      skewed: placement.partitions.length > averageReplicasPerBroker,
      leaderSkewed: placement.leaders.length > averageLeadersPerBroker,
    }))
    .sort((a, b) => a.brokerId - b.brokerId);
};

export const getTopicDistributionMetrics = (
  topic: Topic,
  clusterBrokerCount: number
): TopicDistributionMetrics => {
  const partitions = topic.partitions ?? [];
  const partitionCount = topic.partitionCount ?? partitions.length;
  const distribution = getBrokerTopicDistribution(topic);
  const topicBrokers = distribution.length;
  const preferredLeaders = partitions.filter(
    (partition) => partition.replicas?.[0]?.broker === partition.leader
  ).length;
  const underReplicatedPartitions =
    topic.underReplicatedPartitions ??
    partitions.filter(
      (partition) =>
        partition.replicas?.filter((replica) => replica.inSync).length !==
        partition.replicas?.length
    ).length;

  return {
    topicBrokers,
    brokerSpreadPercentage: percentage(topicBrokers, clusterBrokerCount, 100),
    brokerSkewPercentage: percentage(
      distribution.filter((broker) => broker.skewed).length,
      topicBrokers
    ),
    brokerLeaderSkewPercentage: percentage(
      distribution.filter((broker) => broker.leaderSkewed).length,
      topicBrokers
    ),
    preferredReplicasPercentage: percentage(
      preferredLeaders,
      partitionCount,
      100
    ),
    underReplicatedPercentage: percentage(
      underReplicatedPartitions,
      partitionCount
    ),
    summedTopicOffsets: partitions.reduce(
      (sum, partition) => sum + partition.offsetMax,
      0
    ),
  };
};
