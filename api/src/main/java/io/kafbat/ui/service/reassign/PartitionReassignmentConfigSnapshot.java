package io.kafbat.ui.service.reassign;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

public record PartitionReassignmentConfigSnapshot(
    String resourceType,
    String resourceName,
    String configName,
    @Nullable String previousValue,
    String appliedValue,
    PartitionReassignmentConfigState state) {

  public PartitionReassignmentConfigSnapshot {
    Objects.requireNonNull(resourceType, "resourceType");
    Objects.requireNonNull(resourceName, "resourceName");
    Objects.requireNonNull(configName, "configName");
    Objects.requireNonNull(appliedValue, "appliedValue");
    Objects.requireNonNull(state, "state");
  }

  public PartitionReassignmentConfigSnapshot withState(
      PartitionReassignmentConfigState newState) {
    return new PartitionReassignmentConfigSnapshot(
        resourceType,
        resourceName,
        configName,
        previousValue,
        appliedValue,
        newState);
  }
}
