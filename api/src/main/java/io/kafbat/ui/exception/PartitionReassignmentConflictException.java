package io.kafbat.ui.exception;

public class PartitionReassignmentConflictException extends CustomBaseException {

  public PartitionReassignmentConflictException(String message) {
    super(message);
  }

  @Override
  public ErrorCode getErrorCode() {
    return ErrorCode.PARTITION_REASSIGNMENT_CONFLICT;
  }
}
