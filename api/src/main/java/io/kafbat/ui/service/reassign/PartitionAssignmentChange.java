package io.kafbat.ui.service.reassign;

import java.util.List;
import java.util.Objects;
import org.apache.kafka.common.TopicPartition;

public record PartitionAssignmentChange(
    String topic,
    int partition,
    List<Integer> currentReplicas,
    List<Integer> targetReplicas) {

  public PartitionAssignmentChange {
    Objects.requireNonNull(topic, "topic");
    currentReplicas = List.copyOf(currentReplicas);
    targetReplicas = List.copyOf(targetReplicas);
  }

  TopicPartition topicPartition() {
    return new TopicPartition(topic, partition);
  }
}
