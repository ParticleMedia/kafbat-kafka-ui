package io.kafbat.ui.exception;

public class PartitionReassignmentExecutionDisabledException extends CustomBaseException {

  public PartitionReassignmentExecutionDisabledException() {
    super("Partition reassignment execution is disabled");
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.PARTITION_REASSIGNMENT_EXECUTION_DISABLED;
  }
}
