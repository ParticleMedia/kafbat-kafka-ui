import "@typespec/openapi";
import "./responses.tsp";

namespace Api;

using TypeSpec.Http;
using OpenAPI;

@route("/api/clusters/{clusterName}/partition-reassignments")
@tag("PartitionReassignments")
interface PartitionReassignmentsApi {
  @get
  @route("/capabilities")
  @operationId("getPartitionReassignmentCapabilities")
  @summary("Get effective partition reassignment execution capabilities")
  getPartitionReassignmentCapabilities(
    @path clusterName: string,
  ): PartitionReassignmentCapabilities;

  @get
  @operationId("listActivePartitionReassignments")
  @summary("List active Kafka partition reassignments")
  listActivePartitionReassignments(
    @path clusterName: string,
  ): PartitionReassignmentStatus;

  @post
  @route("/plan")
  @operationId("createPartitionReassignmentPlan")
  @summary("Preview a partition reassignment plan without changing Kafka")
  createPartitionReassignmentPlan(
    @path clusterName: string,
    @body request: PartitionReassignmentPlanRequest,
  ): PartitionReassignmentPlan | ApiBadRequestResponse;

  @post
  @route("/validate")
  @operationId("validatePartitionReassignmentPlan")
  @summary("Revalidate a partition reassignment plan against the current Kafka state")
  validatePartitionReassignmentPlan(
    @path clusterName: string,
    @body request: PartitionReassignmentValidationRequest,
  ): PartitionReassignmentValidationResult | ApiBadRequestResponse | ApiConflictResponse;

  @post
  @route("/execute")
  @operationId("executePartitionReassignmentPlan")
  @summary("Validate and submit a partition reassignment plan to Kafka")
  executePartitionReassignmentPlan(
    @path clusterName: string,
    @body request: PartitionReassignmentExecutionRequest,
  ): PartitionReassignmentExecutionResult | ApiBadRequestResponse | ApiForbiddenResponse | ApiConflictResponse;

  @post
  @route("/cancel")
  @operationId("cancelPartitionReassignments")
  @summary("Cancel selected active partition reassignments")
  cancelPartitionReassignments(
    @path clusterName: string,
    @body request: PartitionReassignmentCancellationRequest,
  ): PartitionReassignmentCancellationResult | ApiBadRequestResponse | ApiForbiddenResponse | ApiConflictResponse;

}

model PartitionReassignmentCapabilities {
  executionEnabled: boolean;
  reason?: string;
}

model PartitionReassignmentStatus {
  reassignments: ActivePartitionReassignment[];
  operation?: PartitionReassignmentOperationStatus;
}

model ActivePartitionReassignment {
  topic: string;
  partition: int32;
  currentReplicas: int32[];
  targetReplicas: int32[];
  addingReplicas: int32[];
  removingReplicas: int32[];
  /** Percentage of target replicas that are currently in the ISR. */
  progressPercent: int32;
}

model PartitionReassignmentPlanRequest {
  assignments: PartitionReassignmentTarget[];
}

model PartitionReassignmentTarget {
  topic: string;
  partition: int32;
  replicas: int32[];
}

model PartitionReassignmentPlan {
  clusterName: string;
  changes: PartitionAssignmentChange[];
  planHash: string;
}

model PartitionAssignmentChange {
  topic: string;
  partition: int32;
  currentReplicas: int32[];
  targetReplicas: int32[];
}

model PartitionReassignmentValidationRequest {
  changes: PartitionAssignmentChange[];
  planHash: string;
}

model PartitionReassignmentValidationResult {
  valid: boolean;
}

model PartitionReassignmentExecutionRequest {
  changes: PartitionAssignmentChange[];
  planHash: string;
  operationId?: string;
  throttleBytesPerSecond?: int64;
}

model PartitionReassignmentExecutionResult {
  acceptedPartitions: int32;
  operationId: string;
  status: PartitionReassignmentOperationState;
}

model PartitionReassignmentCancellationRequest {
  operationId: string;
  partitions: PartitionReassignmentPartition[];
}

model PartitionReassignmentPartition {
  topic: string;
  partition: int32;
}

model PartitionReassignmentCancellationResult {
  operationId: string;
  cancelledPartitions: int32;
  skippedPartitions: int32;
}

model PartitionReassignmentOperationStatus {
  operationId: string;
  status: PartitionReassignmentOperationState;
  throttleBytesPerSecond?: int64;
  cleanupConflicts: string[];
}

enum PartitionReassignmentOperationState {
  PREPARED,
  THROTTLE_APPLIED,
  REASSIGNMENT_SUBMITTED,
  CLEANUP_PENDING,
  COMPLETED,
  CANCELLED,
  FAILED,
  CLEANUP_CONFLICT,
}
