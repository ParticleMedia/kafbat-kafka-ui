package io.kafbat.ui.service.reassign;

public record PartitionReassignmentExecution(
    String operationId,
    int acceptedPartitions,
    PartitionReassignmentOperationStatus status) {
}
