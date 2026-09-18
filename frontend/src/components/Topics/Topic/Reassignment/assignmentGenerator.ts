import type { PartitionReassignmentTarget, Topic } from 'generated-sources';

const normalizeBrokerIds = (brokerIds: number[]) =>
  [...new Set(brokerIds)].sort((left, right) => left - right);

const sameReplicas = (left: number[], right: number[]) =>
  left.length === right.length &&
  left.every((brokerId, index) => brokerId === right[index]);

export interface BrokerAssignmentCount {
  brokerId: number;
  replicas: number;
  leaders: number;
}

export const getBrokerAssignmentCounts = (
  topic: Topic
): BrokerAssignmentCount[] => {
  const counts = new Map<number, Omit<BrokerAssignmentCount, 'brokerId'>>();

  topic.partitions?.forEach((partition) => {
    partition.replicas?.forEach((replica) => {
      if (replica.broker === undefined) return;
      const current = counts.get(replica.broker) ?? { replicas: 0, leaders: 0 };
      current.replicas += 1;
      if (partition.leader === replica.broker || replica.leader) {
        current.leaders += 1;
      }
      counts.set(replica.broker, current);
    });
  });

  return [...counts.entries()]
    .map(([brokerId, count]) => ({ brokerId, ...count }))
    .sort((left, right) => left.brokerId - right.brokerId);
};

export const hasAssignmentChanges = (
  topic: Topic,
  assignments: PartitionReassignmentTarget[]
) => {
  const currentByPartition = new Map(
    (topic.partitions ?? []).map((partition) => [
      partition.partition,
      (partition.replicas ?? []).flatMap((replica) =>
        replica.broker === undefined ? [] : [replica.broker]
      ),
    ])
  );
  return assignments.some(
    (assignment) =>
      !sameReplicas(
        currentByPartition.get(assignment.partition) ?? [],
        assignment.replicas
      )
  );
};

export const generateBalancedAssignments = (
  topic: Topic,
  brokerIds: number[],
  replicationFactor: number,
  partitionIds?: number[]
): PartitionReassignmentTarget[] => {
  const brokers = normalizeBrokerIds(brokerIds);
  const partitions = [...(topic.partitions ?? [])].sort(
    (left, right) => left.partition - right.partition
  );
  const selectedPartitions = new Set(
    partitionIds ?? partitions.map((partition) => partition.partition)
  );

  if (brokers.length === 0) {
    throw new Error('Select at least one broker');
  }
  if (!Number.isInteger(replicationFactor) || replicationFactor < 1) {
    throw new Error('Replication factor must be at least 1');
  }
  if (replicationFactor > brokers.length) {
    throw new Error('Replication factor cannot exceed selected brokers');
  }
  if (partitions.length === 0) {
    throw new Error('Topic has no partitions to assign');
  }
  if (selectedPartitions.size === 0) {
    throw new Error('Select at least one partition');
  }

  const knownPartitions = new Set(
    partitions.map((partition) => partition.partition)
  );
  if (
    [...selectedPartitions].some((partition) => !knownPartitions.has(partition))
  ) {
    throw new Error('Selected partition is not available');
  }

  return partitions
    .filter((partition) => selectedPartitions.has(partition.partition))
    .map((partition, selectedIndex) => ({
      topic: topic.name,
      partition: partition.partition,
      replicas: Array.from(
        { length: replicationFactor },
        (_, replicaIndex) =>
          brokers[(selectedIndex + replicaIndex) % brokers.length]
      ),
    }));
};

export const buildProjectedTopic = (
  topic: Topic,
  assignments: PartitionReassignmentTarget[]
): Topic => {
  const assignmentsByPartition = new Map(
    assignments.map((assignment) => [assignment.partition, assignment])
  );

  const partitions = topic.partitions?.map((partition) => {
    const assignment = assignmentsByPartition.get(partition.partition);
    if (!assignment) return partition;

    return {
      ...partition,
      leader: assignment.replicas[0],
      replicas: assignment.replicas.map((broker, replicaIndex) => ({
        broker,
        leader: replicaIndex === 0,
        inSync: true,
      })),
    };
  });
  const replicaCounts = new Set(
    partitions?.map((partition) => partition.replicas?.length ?? 0)
  );

  return {
    ...topic,
    replicationFactor:
      replicaCounts.size === 1
        ? [...replicaCounts][0]
        : topic.replicationFactor,
    underReplicatedPartitions:
      partitions?.filter(
        (partition) =>
          partition.replicas?.some((replica) => !replica.inSync) ?? false
      ).length ?? 0,
    partitions,
  };
};
