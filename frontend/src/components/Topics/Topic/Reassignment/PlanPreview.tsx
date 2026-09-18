import React from 'react';
import type {
  PartitionReassignmentPlan,
  PartitionReassignmentTarget,
  Topic,
} from 'generated-sources';
import { Button } from 'components/common/Button/Button';
import Heading from 'components/common/heading/Heading.styled';
import { getTopicDistributionMetrics } from 'components/Topics/lib/topicDistributionMetrics';

import {
  buildProjectedTopic,
  getBrokerAssignmentCounts,
} from './assignmentGenerator';
import * as S from './AssignmentPlanner.styled';

interface Props {
  assignments: PartitionReassignmentTarget[];
  clusterBrokerCount: number;
  isValid: boolean;
  isValidating: boolean;
  isExecutionEnabled: boolean;
  executionDisabledReason?: string;
  onExecute(): void;
  onValidate(): void;
  plan: PartitionReassignmentPlan;
  selectedBrokerCount: number;
  topic: Topic;
}

const getMetricRows = (
  topic: Topic,
  projectedTopic: Topic,
  clusterBrokerCount: number
) => {
  const current = getTopicDistributionMetrics(topic, clusterBrokerCount);
  const projected = getTopicDistributionMetrics(
    projectedTopic,
    clusterBrokerCount
  );
  return [
    {
      label: 'Broker Spread',
      current: current.brokerSpreadPercentage,
      projected: projected.brokerSpreadPercentage,
    },
    {
      label: 'Replica Skew',
      current: current.brokerSkewPercentage,
      projected: projected.brokerSkewPercentage,
    },
    {
      label: 'Leader Skew',
      current: current.brokerLeaderSkewPercentage,
      projected: projected.brokerLeaderSkewPercentage,
    },
    {
      label: 'Preferred Leaders',
      current: current.preferredReplicasPercentage,
      projected: projected.preferredReplicasPercentage,
    },
  ];
};

const PlanPreview: React.FC<Props> = ({
  assignments,
  clusterBrokerCount,
  isValid,
  isValidating,
  isExecutionEnabled,
  executionDisabledReason,
  onExecute,
  onValidate,
  plan,
  selectedBrokerCount,
  topic,
}) => {
  const projectedTopic = React.useMemo(
    () => buildProjectedTopic(topic, assignments),
    [assignments, topic]
  );
  const comparison = getMetricRows(topic, projectedTopic, clusterBrokerCount);
  const currentDistribution = new Map(
    getBrokerAssignmentCounts(topic).map((broker) => [broker.brokerId, broker])
  );
  const projectedDistribution = new Map(
    getBrokerAssignmentCounts(projectedTopic).map((broker) => [
      broker.brokerId,
      broker,
    ])
  );
  const brokerIds = [
    ...new Set([
      ...currentDistribution.keys(),
      ...projectedDistribution.keys(),
    ]),
  ].sort((left, right) => left - right);
  const replicaMoves = plan.changes.reduce(
    (total, change) =>
      total +
      change.targetReplicas.filter(
        (brokerId) => !change.currentReplicas.includes(brokerId)
      ).length,
    0
  );

  return (
    <S.Preview>
      <Heading level={4}>Plan preview</Heading>
      <S.Summary aria-label="Plan summary">
        <S.SummaryItem>
          <span>Changed partitions</span>
          <strong>{plan.changes.length}</strong>
        </S.SummaryItem>
        <S.SummaryItem>
          <span>Replica moves</span>
          <strong>{replicaMoves}</strong>
        </S.SummaryItem>
        <S.SummaryItem>
          <span>Selected brokers</span>
          <strong>{selectedBrokerCount}</strong>
        </S.SummaryItem>
      </S.Summary>

      <S.Hint>
        Projection assumes target replicas are in sync and the first replica
        becomes the preferred leader. Actual leaders can differ until an
        election occurs.
      </S.Hint>

      <S.Table aria-label="Assignment health comparison">
        <thead>
          <tr>
            <th>Metric</th>
            <th>Current</th>
            <th>Projected</th>
          </tr>
        </thead>
        <tbody>
          {comparison.map((metric) => (
            <tr key={metric.label}>
              <td>{metric.label}</td>
              <td>{metric.current}%</td>
              <td>{metric.projected}%</td>
            </tr>
          ))}
        </tbody>
      </S.Table>

      <S.Table aria-label="Broker distribution changes">
        <thead>
          <tr>
            <th>Broker</th>
            <th>Current replicas</th>
            <th>Target replicas</th>
            <th>Replica delta</th>
            <th>Current leaders</th>
            <th>Target preferred leaders</th>
          </tr>
        </thead>
        <tbody>
          {brokerIds.map((brokerId) => {
            const currentReplicas =
              currentDistribution.get(brokerId)?.replicas ?? 0;
            const targetReplicas =
              projectedDistribution.get(brokerId)?.replicas ?? 0;
            const delta = targetReplicas - currentReplicas;
            return (
              <tr key={brokerId}>
                <td>Broker {brokerId}</td>
                <td>{currentReplicas}</td>
                <td>{targetReplicas}</td>
                <td>{delta > 0 ? `+${delta}` : delta}</td>
                <td>{currentDistribution.get(brokerId)?.leaders ?? 0}</td>
                <td>{projectedDistribution.get(brokerId)?.leaders ?? 0}</td>
              </tr>
            );
          })}
        </tbody>
      </S.Table>

      <S.Table aria-label="Partition assignment changes">
        <thead>
          <tr>
            <th>Topic</th>
            <th>Partition</th>
            <th>Current replicas</th>
            <th>Target replicas</th>
            <th>Current leader</th>
            <th>Target preferred leader</th>
          </tr>
        </thead>
        <tbody>
          {plan.changes.map((change) => (
            <tr key={`${change.topic}-${change.partition}`}>
              <td>{change.topic}</td>
              <td>{change.partition}</td>
              <td>{change.currentReplicas.join(', ')}</td>
              <td>{change.targetReplicas.join(', ')}</td>
              <td>
                {topic.partitions?.find(
                  (partition) => partition.partition === change.partition
                )?.leader ?? '—'}
              </td>
              <td>{change.targetReplicas[0] ?? '—'}</td>
            </tr>
          ))}
        </tbody>
      </S.Table>

      <S.AdvancedPreview>
        <summary>Advanced JSON preview</summary>
        <pre>{JSON.stringify({ assignments, plan }, null, 2)}</pre>
      </S.AdvancedPreview>
      <S.Actions>
        <Button
          buttonType="secondary"
          buttonSize="M"
          inProgress={isValidating}
          onClick={onValidate}
        >
          Revalidate plan
        </Button>
        <Button
          buttonType="danger"
          buttonSize="M"
          disabled={!isValid || !isExecutionEnabled}
          onClick={onExecute}
        >
          Execute reassignment
        </Button>
      </S.Actions>
      {!isExecutionEnabled && executionDisabledReason && (
        <S.Hint role="status">{executionDisabledReason}</S.Hint>
      )}
      {isValid && <S.SuccessMessage>Plan is valid</S.SuccessMessage>}
    </S.Preview>
  );
};

export default PlanPreview;
