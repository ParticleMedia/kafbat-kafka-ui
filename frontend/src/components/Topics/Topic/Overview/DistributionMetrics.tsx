import React from 'react';
import type { Topic } from 'generated-sources';
import BytesFormatted from 'components/common/BytesFormatted/BytesFormatted';
import * as Metrics from 'components/common/Metrics';
import { getTopicDistributionMetrics } from 'components/Topics/lib/topicDistributionMetrics';

interface Props {
  topic: Topic;
  clusterBrokerCount: number;
}

const badWhenHigh = (value: number) => {
  if (value === 0) return 'success';
  return value <= 33 ? 'warning' : 'error';
};

const goodWhenHigh = (value: number) => {
  if (value > 75) return 'success';
  return value > 50 ? 'warning' : 'error';
};

const DistributionMetrics: React.FC<Props> = ({
  topic,
  clusterBrokerCount,
}) => {
  const distribution = getTopicDistributionMetrics(topic, clusterBrokerCount);

  return (
    <Metrics.Section title="Distribution health">
      <Metrics.Indicator label="Topic Brokers">
        {distribution.topicBrokers}
        <Metrics.LightText> of {clusterBrokerCount}</Metrics.LightText>
      </Metrics.Indicator>
      <Metrics.Indicator
        label="Broker Spread"
        title="Percentage of cluster brokers hosting in-sync replicas for this topic"
        isAlert
        alertType={goodWhenHigh(distribution.brokerSpreadPercentage)}
      >
        {distribution.brokerSpreadPercentage}%
      </Metrics.Indicator>
      <Metrics.Indicator
        label="Replica Skew"
        title="Percentage of topic brokers hosting more replicas than the average"
        isAlert
        alertType={badWhenHigh(distribution.brokerSkewPercentage)}
      >
        {distribution.brokerSkewPercentage}%
      </Metrics.Indicator>
      <Metrics.Indicator
        label="Leader Skew"
        title="Percentage of topic brokers hosting more leaders than the average"
        isAlert
        alertType={badWhenHigh(distribution.brokerLeaderSkewPercentage)}
      >
        {distribution.brokerLeaderSkewPercentage}%
      </Metrics.Indicator>
      <Metrics.Indicator
        label="Preferred Leaders"
        title="Percentage of partitions whose leader is the first replica"
        isAlert
        alertType={goodWhenHigh(distribution.preferredReplicasPercentage)}
      >
        {distribution.preferredReplicasPercentage}%
      </Metrics.Indicator>
      <Metrics.Indicator
        label="Under Replicated"
        title="Percentage of partitions with missing in-sync replicas"
        isAlert
        alertType={badWhenHigh(distribution.underReplicatedPercentage)}
      >
        {distribution.underReplicatedPercentage}%
      </Metrics.Indicator>
      <Metrics.Indicator label="Recent Offsets">
        {distribution.summedTopicOffsets.toLocaleString()}
      </Metrics.Indicator>
      <Metrics.Indicator label="Bytes In / sec">
        {topic.bytesInPerSec === undefined ? (
          'N/A'
        ) : (
          <BytesFormatted value={topic.bytesInPerSec} precision={2} />
        )}
      </Metrics.Indicator>
      <Metrics.Indicator label="Bytes Out / sec">
        {topic.bytesOutPerSec === undefined ? (
          'N/A'
        ) : (
          <BytesFormatted value={topic.bytesOutPerSec} precision={2} />
        )}
      </Metrics.Indicator>
    </Metrics.Section>
  );
};

export default DistributionMetrics;
