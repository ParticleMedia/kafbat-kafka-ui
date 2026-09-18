package io.kafbat.ui.service.reassign;

import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.model.rbac.permission.ClusterOperationAction;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ClustersStorage;
import io.kafbat.ui.service.audit.AuditService;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@Slf4j
@RequiredArgsConstructor
public class PartitionReassignmentReconciler {

  private final PartitionReassignmentOperationJournal journal;
  private final PartitionReassignmentThrottleService throttleService;
  private final AdminClientService adminClientService;
  private final ClustersStorage clustersStorage;
  private final PartitionReassignmentService reassignmentService;
  private final PartitionReassignmentClusterCoordinator clusterCoordinator;
  private final AuditService auditService;
  private final AtomicBoolean running = new AtomicBoolean();

  public Mono<Void> reconcile() {
    return Mono.fromCallable(journal::listUnresolved)
        .subscribeOn(Schedulers.boundedElastic())
        .flatMapMany(Flux::fromIterable)
        .concatMap(operation -> reconcileOperation(operation)
            .onErrorResume(error -> {
              log.error(
                  "Unable to reconcile partition reassignment operation {} for cluster {}",
                  operation.operationId(),
                  operation.clusterName(),
                  error);
              return Mono.empty();
            }))
        .then();
  }

  @Scheduled(
      fixedDelayString = "${kafka.cluster-operations.reconciliation-interval-ms:5000}")
  void scheduledReconcile() {
    if (!running.compareAndSet(false, true)) {
      return;
    }
    reconcile()
        .doOnError(error -> log.error(
            "Unable to reconcile partition reassignment operations", error))
        .doFinally(ignored -> running.set(false))
        .subscribe();
  }

  private Mono<Void> reconcileOperation(PartitionReassignmentOperation operation) {
    var cluster = clustersStorage.getClusterByName(operation.clusterName());
    if (cluster.isEmpty()) {
      log.warn(
          "Cannot reconcile partition reassignment operation {}: cluster {} is unavailable",
          operation.operationId(),
          operation.clusterName());
      return Mono.empty();
    }
    return clusterCoordinator.withReconciliationLock(
        operation.clusterName(),
        findOperation(operation.clusterName(), operation.operationId())
            .filter(latest -> !latest.status().isTerminal())
            .flatMap(latest -> reconcileLatest(cluster.get(), latest)))
        .then();
  }

  private Mono<PartitionReassignmentOperation> reconcileLatest(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation) {
    if (operation.kind() == PartitionReassignmentOperationKind.CANCEL) {
      return reassignmentService.resumeCancellationOperation(cluster, operation);
    }
    if (operation.status() == PartitionReassignmentOperationStatus.PREPARED) {
      return throttleService.cleanup(
          cluster, operation, PartitionReassignmentOperationStatus.FAILED);
    }
    return reconcileSubmitted(cluster, operation);
  }

  private Mono<PartitionReassignmentOperation> reconcileSubmitted(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation) {
    var partitions = operation.changes().stream()
        .map(PartitionAssignmentChange::topicPartition)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    if (partitions.isEmpty()) {
      partitions = operation.partitions().stream()
          .map(PartitionReassignmentPartition::topicPartition)
          .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    }
    var requestedPartitions = partitions;
    return adminClientService.get(cluster)
        .flatMap(admin -> admin.listPartitionReassignments(requestedPartitions)
            .flatMap(active -> {
              if (!active.isEmpty()
                  && operation.status()
                      != PartitionReassignmentOperationStatus.THROTTLE_APPLIED) {
                return Mono.just(operation);
              }
              var topics = operation.changes().stream()
                  .map(PartitionAssignmentChange::topic)
                  .collect(java.util.stream.Collectors.toSet());
              return admin.describeTopics(topics)
                  .flatMap(descriptions -> classifyExecution(
                      cluster, operation, active.keySet(), descriptions));
            }));
  }

  private Mono<PartitionReassignmentOperation> classifyExecution(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation,
      Set<TopicPartition> activePartitions,
      Map<String, TopicDescription> descriptions) {
    if (activePartitions.isEmpty()) {
      if (targetAssignmentsInstalled(operation, descriptions)) {
        if (operation.acceptedPartitions() != operation.changes().size()) {
          return persist(operation.withExecutionResult(
                  PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
                  operation.changes().size()))
              .flatMap(accepted -> cleanup(
                  cluster, accepted, PartitionReassignmentOperationStatus.COMPLETED));
        }
        return cleanup(
            cluster, operation, PartitionReassignmentOperationStatus.COMPLETED);
      }
      return cleanup(cluster, operation, PartitionReassignmentOperationStatus.FAILED);
    }
    var allAccepted = operation.changes().stream().allMatch(change ->
        activePartitions.contains(change.topicPartition())
            || targetAssignmentInstalled(change, descriptions));
    if (!allAccepted) {
      return Mono.just(operation);
    }
    return persist(operation.withExecutionResult(
        PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
        operation.changes().size()));
  }

  private Mono<PartitionReassignmentOperation> cleanup(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation,
      PartitionReassignmentOperationStatus completedStatus) {
    return persist(operation.withStatus(
            PartitionReassignmentOperationStatus.CLEANUP_PENDING))
        .flatMap(pending -> throttleService.cleanup(cluster, pending, completedStatus))
        .doOnNext(result -> auditCleanup(result, null))
        .doOnError(error -> auditCleanup(operation, error));
  }

  private void auditCleanup(
      PartitionReassignmentOperation operation,
      Throwable error) {
    var context = AccessContext.builder()
        .cluster(operation.clusterName())
        .clusterOperationActions(ClusterOperationAction.REASSIGN_PARTITIONS)
        .operationName("reconcilePartitionReassignmentThrottle")
        .operationParams(Map.of(
            "operationId", operation.operationId(),
            "status", operation.status().name(),
            "cleanupConflicts", operation.cleanupConflicts()))
        .build();
    auditService.auditSystem(context, error);
  }

  private static boolean targetAssignmentsInstalled(
      PartitionReassignmentOperation operation,
      Map<String, TopicDescription> descriptions) {
    return operation.changes().stream()
        .allMatch(change -> targetAssignmentInstalled(change, descriptions));
  }

  private static boolean targetAssignmentInstalled(
      PartitionAssignmentChange change,
      Map<String, TopicDescription> descriptions) {
    var description = descriptions.get(change.topic());
    if (description == null) {
      return false;
    }
    return description.partitions().stream()
        .filter(partition -> partition.partition() == change.partition())
        .map(partition -> partition.replicas().stream()
            .map(node -> node.id())
            .toList())
        .anyMatch(change.targetReplicas()::equals);
  }

  private Mono<PartitionReassignmentOperation> findOperation(
      String clusterName,
      String operationId) {
    return Mono.fromCallable(() -> journal.find(clusterName, operationId))
        .subscribeOn(Schedulers.boundedElastic())
        .flatMap(Mono::justOrEmpty);
  }

  private Mono<PartitionReassignmentOperation> persist(
      PartitionReassignmentOperation operation) {
    return Mono.fromCallable(() -> journal.update(operation))
        .subscribeOn(Schedulers.boundedElastic());
  }
}
