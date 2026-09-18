package io.kafbat.ui.service.reassign;

public enum PartitionReassignmentOperationStatus {
  PREPARED(false),
  THROTTLE_APPLIED(false),
  REASSIGNMENT_SUBMITTED(false),
  CLEANUP_PENDING(false),
  COMPLETED(true),
  CANCELLED(true),
  FAILED(true),
  CLEANUP_CONFLICT(true);

  private final boolean terminal;

  PartitionReassignmentOperationStatus(boolean terminal) {
    this.terminal = terminal;
  }

  public boolean isTerminal() {
    return terminal;
  }
}
