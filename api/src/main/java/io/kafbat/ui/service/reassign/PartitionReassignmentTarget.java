package io.kafbat.ui.service.reassign;

import java.util.List;
import java.util.Objects;

public record PartitionReassignmentTarget(
    String topic,
    int partition,
    List<Integer> replicas) {

  public PartitionReassignmentTarget {
    Objects.requireNonNull(topic, "topic");
    replicas = List.copyOf(replicas);
  }
}
