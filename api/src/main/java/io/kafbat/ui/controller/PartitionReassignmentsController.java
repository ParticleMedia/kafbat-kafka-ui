package io.kafbat.ui.controller;

import io.kafbat.ui.api.PartitionReassignmentsApi;
import io.kafbat.ui.model.ActivePartitionReassignmentDTO;
import io.kafbat.ui.model.PartitionAssignmentChangeDTO;
import io.kafbat.ui.model.PartitionReassignmentCancellationRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentCancellationResultDTO;
import io.kafbat.ui.model.PartitionReassignmentCapabilitiesDTO;
import io.kafbat.ui.model.PartitionReassignmentExecutionRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentExecutionResultDTO;
import io.kafbat.ui.model.PartitionReassignmentOperationStateDTO;
import io.kafbat.ui.model.PartitionReassignmentOperationStatusDTO;
import io.kafbat.ui.model.PartitionReassignmentPlanDTO;
import io.kafbat.ui.model.PartitionReassignmentPlanRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentStatusDTO;
import io.kafbat.ui.model.PartitionReassignmentValidationRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentValidationResultDTO;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.model.rbac.permission.ClusterOperationAction;
import io.kafbat.ui.service.reassign.PartitionAssignmentChange;
import io.kafbat.ui.service.reassign.PartitionReassignmentOperation;
import io.kafbat.ui.service.reassign.PartitionReassignmentPartition;
import io.kafbat.ui.service.reassign.PartitionReassignmentPlan;
import io.kafbat.ui.service.reassign.PartitionReassignmentService;
import io.kafbat.ui.service.reassign.PartitionReassignmentTarget;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
public class PartitionReassignmentsController extends AbstractController
    implements PartitionReassignmentsApi {

  private final PartitionReassignmentService service;

  @Override
  public Mono<ResponseEntity<PartitionReassignmentCapabilitiesDTO>>
      getPartitionReassignmentCapabilities(String clusterName, ServerWebExchange exchange) {
    var context = viewContext(clusterName, "getPartitionReassignmentCapabilities");
    return validateAccess(context)
        .then(Mono.fromSupplier(() -> service.getCapabilities(getCluster(clusterName))))
        .map(capabilities -> ResponseEntity.ok(new PartitionReassignmentCapabilitiesDTO()
            .executionEnabled(capabilities.executionEnabled())
            .reason(capabilities.reason())))
        .doOnEach(signal -> audit(context, signal));
  }

  @Override
  public Mono<ResponseEntity<PartitionReassignmentStatusDTO>> listActivePartitionReassignments(
      String clusterName,
      ServerWebExchange exchange) {
    var context = viewContext(clusterName, "listActivePartitionReassignments");
    return validateAccess(context)
        .then(Mono.defer(() -> Mono.zip(
            service.listActiveReassignments(getCluster(clusterName)),
            service.getCurrentOperation(getCluster(clusterName)))))
        .map(result -> new PartitionReassignmentStatusDTO()
            .reassignments(result.getT1().stream()
                .map(reassignment -> new ActivePartitionReassignmentDTO()
                    .topic(reassignment.topic())
                    .partition(reassignment.partition())
                    .currentReplicas(reassignment.currentReplicas())
                    .targetReplicas(reassignment.targetReplicas())
                    .addingReplicas(reassignment.addingReplicas())
                    .removingReplicas(reassignment.removingReplicas())
                    .progressPercent(reassignment.progressPercent()))
                .toList())
            .operation(result.getT2()
                .map(PartitionReassignmentsController::toOperationStatus)
                .orElse(null)))
        .map(ResponseEntity::ok)
        .doOnEach(signal -> audit(context, signal));
  }

  @Override
  public Mono<ResponseEntity<PartitionReassignmentPlanDTO>> createPartitionReassignmentPlan(
      String clusterName,
      Mono<PartitionReassignmentPlanRequestDTO> request,
      ServerWebExchange exchange) {
    var context = viewContext(clusterName, "createPartitionReassignmentPlan");
    return validateAccess(context)
        .then(request)
        .flatMap(body -> service.createPlan(
            getCluster(clusterName),
            body.getAssignments().stream()
                .map(target -> new PartitionReassignmentTarget(
                    target.getTopic(), target.getPartition(), target.getReplicas()))
                .toList()))
        .map(PartitionReassignmentsController::toDto)
        .map(ResponseEntity::ok)
        .doOnEach(signal -> audit(context, signal));
  }

  @Override
  public Mono<ResponseEntity<PartitionReassignmentValidationResultDTO>>
      validatePartitionReassignmentPlan(
          String clusterName,
          Mono<PartitionReassignmentValidationRequestDTO> request,
          ServerWebExchange exchange) {
    var context = viewContext(clusterName, "validatePartitionReassignmentPlan");
    return validateAccess(context)
        .then(request)
        .flatMap(body -> service.validatePlan(
            getCluster(clusterName),
            new PartitionReassignmentPlan(
                clusterName,
                body.getChanges().stream()
                    .map(PartitionReassignmentsController::toDomain)
                    .toList(),
                body.getPlanHash())))
        .thenReturn(ResponseEntity.ok(new PartitionReassignmentValidationResultDTO().valid(true)))
        .doOnEach(signal -> audit(context, signal));
  }

  @Override
  public Mono<ResponseEntity<PartitionReassignmentExecutionResultDTO>>
      executePartitionReassignmentPlan(
          String clusterName,
          Mono<PartitionReassignmentExecutionRequestDTO> request,
          ServerWebExchange exchange) {
    var context = executeContext(clusterName, "executePartitionReassignmentPlan");
    return validateAccess(context)
        .then(request)
        .flatMap(body -> {
          var operationId = Optional.ofNullable(body.getOperationId())
              .filter(value -> !value.isBlank())
              .orElseGet(() -> UUID.randomUUID().toString());
          return service.executePlan(
              getCluster(clusterName),
              new PartitionReassignmentPlan(
                  clusterName,
                  body.getChanges().stream()
                      .map(PartitionReassignmentsController::toDomain)
                      .toList(),
                  body.getPlanHash()),
              operationId,
              body.getThrottleBytesPerSecond());
        })
        .map(execution -> new PartitionReassignmentExecutionResultDTO()
            .operationId(execution.operationId())
            .acceptedPartitions(execution.acceptedPartitions())
            .status(toOperationStateDto(execution.status())))
        .map(ResponseEntity::ok)
        .doOnEach(signal -> audit(context, signal));
  }

  @Override
  public Mono<ResponseEntity<PartitionReassignmentCancellationResultDTO>>
      cancelPartitionReassignments(
          String clusterName,
          Mono<PartitionReassignmentCancellationRequestDTO> request,
          ServerWebExchange exchange) {
    var context = executeContext(clusterName, "cancelPartitionReassignments");
    return validateAccess(context)
        .then(request)
        .flatMap(body -> service.cancelReassignments(
            getCluster(clusterName),
            body.getOperationId(),
            body.getPartitions().stream()
                .map(partition -> new PartitionReassignmentPartition(
                    partition.getTopic(), partition.getPartition()))
                .toList()))
        .map(result -> new PartitionReassignmentCancellationResultDTO()
            .operationId(result.operationId())
            .cancelledPartitions(result.cancelledPartitions())
            .skippedPartitions(result.skippedPartitions()))
        .map(ResponseEntity::ok)
        .doOnEach(signal -> audit(context, signal));
  }

  private static AccessContext viewContext(String clusterName, String operationName) {
    return AccessContext.builder()
        .cluster(clusterName)
        .clusterOperationActions(ClusterOperationAction.VIEW)
        .operationName(operationName)
        .operationParams(Map.of())
        .build();
  }

  private static AccessContext executeContext(String clusterName, String operationName) {
    return AccessContext.builder()
        .cluster(clusterName)
        .clusterOperationActions(ClusterOperationAction.REASSIGN_PARTITIONS)
        .operationName(operationName)
        .operationParams(Map.of())
        .build();
  }

  private static PartitionReassignmentPlanDTO toDto(PartitionReassignmentPlan plan) {
    return new PartitionReassignmentPlanDTO()
        .clusterName(plan.clusterName())
        .changes(plan.changes().stream()
            .map(change -> new PartitionAssignmentChangeDTO()
                .topic(change.topic())
                .partition(change.partition())
                .currentReplicas(change.currentReplicas())
                .targetReplicas(change.targetReplicas()))
            .toList())
        .planHash(plan.planHash());
  }

  private static PartitionAssignmentChange toDomain(PartitionAssignmentChangeDTO change) {
    return new PartitionAssignmentChange(
        change.getTopic(),
        change.getPartition(),
        change.getCurrentReplicas(),
        change.getTargetReplicas());
  }

  private static PartitionReassignmentOperationStatusDTO toOperationStatus(
      PartitionReassignmentOperation operation) {
    return new PartitionReassignmentOperationStatusDTO()
        .operationId(operation.operationId())
        .status(toOperationStateDto(operation.status()))
        .throttleBytesPerSecond(operation.throttleBytesPerSecond())
        .cleanupConflicts(operation.cleanupConflicts());
  }

  private static PartitionReassignmentOperationStateDTO toOperationStateDto(
      io.kafbat.ui.service.reassign.PartitionReassignmentOperationStatus status) {
    return PartitionReassignmentOperationStateDTO.valueOf(status.name());
  }
}
