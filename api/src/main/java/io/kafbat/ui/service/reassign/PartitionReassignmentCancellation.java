package io.kafbat.ui.service.reassign;

public record PartitionReassignmentCancellation(
    String operationId,
    int cancelledPartitions,
    int skippedPartitions) {
}
