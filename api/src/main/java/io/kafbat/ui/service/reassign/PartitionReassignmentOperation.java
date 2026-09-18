package io.kafbat.ui.service.reassign;

import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

public record PartitionReassignmentOperation(
    String operationId,
    String clusterName,
    PartitionReassignmentOperationKind kind,
    String requestFingerprint,
    List<PartitionAssignmentChange> changes,
    List<PartitionReassignmentPartition> partitions,
    @Nullable Long throttleBytesPerSecond,
    PartitionReassignmentOperationStatus status,
    List<PartitionReassignmentConfigSnapshot> configSnapshots,
    int acceptedPartitions,
    int cancelledPartitions,
    int skippedPartitions,
    List<String> cleanupConflicts) {

  public PartitionReassignmentOperation {
    Objects.requireNonNull(operationId, "operationId");
    Objects.requireNonNull(clusterName, "clusterName");
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(requestFingerprint, "requestFingerprint");
    changes = List.copyOf(changes);
    partitions = List.copyOf(partitions);
    Objects.requireNonNull(status, "status");
    configSnapshots = List.copyOf(configSnapshots);
    cleanupConflicts = List.copyOf(cleanupConflicts);
  }

  public PartitionReassignmentOperation withStatus(
      PartitionReassignmentOperationStatus newStatus) {
    return copy(newStatus, configSnapshots, acceptedPartitions,
        cancelledPartitions, skippedPartitions, cleanupConflicts);
  }

  public PartitionReassignmentOperation withConfigSnapshots(
      List<PartitionReassignmentConfigSnapshot> snapshots) {
    return copy(status, snapshots, acceptedPartitions,
        cancelledPartitions, skippedPartitions, cleanupConflicts);
  }

  public PartitionReassignmentOperation withExecutionResult(
      PartitionReassignmentOperationStatus newStatus,
      int accepted) {
    return copy(newStatus, configSnapshots, accepted,
        cancelledPartitions, skippedPartitions, cleanupConflicts);
  }

  public PartitionReassignmentOperation withCancellationResult(
      int cancelled,
      int skipped) {
    return copy(status, configSnapshots, acceptedPartitions,
        cancelled, skipped, cleanupConflicts);
  }

  public PartitionReassignmentOperation withCleanupResult(
      PartitionReassignmentOperationStatus newStatus,
      List<PartitionReassignmentConfigSnapshot> snapshots,
      List<String> conflicts) {
    return copy(newStatus, snapshots, acceptedPartitions,
        cancelledPartitions, skippedPartitions, conflicts);
  }

  private PartitionReassignmentOperation copy(
      PartitionReassignmentOperationStatus newStatus,
      List<PartitionReassignmentConfigSnapshot> snapshots,
      int accepted,
      int cancelled,
      int skipped,
      List<String> conflicts) {
    return new PartitionReassignmentOperation(
        operationId,
        clusterName,
        kind,
        requestFingerprint,
        changes,
        partitions,
        throttleBytesPerSecond,
        newStatus,
        snapshots,
        accepted,
        cancelled,
        skipped,
        conflicts);
  }
}
