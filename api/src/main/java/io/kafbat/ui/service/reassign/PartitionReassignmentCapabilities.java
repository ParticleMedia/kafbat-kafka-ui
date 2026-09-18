package io.kafbat.ui.service.reassign;

import org.jspecify.annotations.Nullable;

public record PartitionReassignmentCapabilities(
    boolean executionEnabled,
    @Nullable String reason) {
}
