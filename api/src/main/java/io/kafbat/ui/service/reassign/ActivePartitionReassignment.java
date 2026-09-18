package io.kafbat.ui.service.reassign;

import java.util.List;

public record ActivePartitionReassignment(
    String topic,
    int partition,
    List<Integer> currentReplicas,
    List<Integer> targetReplicas,
    List<Integer> addingReplicas,
    List<Integer> removingReplicas,
    int progressPercent) {
}
