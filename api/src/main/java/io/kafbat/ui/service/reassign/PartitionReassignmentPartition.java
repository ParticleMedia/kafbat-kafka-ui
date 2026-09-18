package io.kafbat.ui.service.reassign;

import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

public record PartitionReassignmentPartition(String topic, int partition) {

  public PartitionReassignmentPartition {
    Objects.requireNonNull(topic, "topic");
  }

  TopicPartition topicPartition() {
    return new TopicPartition(topic, partition);
  }
}
