package io.kafbat.ui.service.reassign;

import java.util.List;
import java.util.Objects;

public record PartitionReassignmentPlan(
    String clusterName,
    List<PartitionAssignmentChange> changes,
    String planHash) {

  public PartitionReassignmentPlan {
    Objects.requireNonNull(clusterName, "clusterName");
    changes = List.copyOf(changes);
    Objects.requireNonNull(planHash, "planHash");
  }
}
