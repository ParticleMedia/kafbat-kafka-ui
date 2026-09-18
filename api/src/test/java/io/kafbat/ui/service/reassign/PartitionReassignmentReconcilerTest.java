package io.kafbat.ui.service.reassign;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ClustersStorage;
import io.kafbat.ui.service.ReactiveAdminClient;
import io.kafbat.ui.service.audit.AuditService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.PartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class PartitionReassignmentReconcilerTest {

  private final KafkaCluster cluster = KafkaCluster.builder().name("dev").build();
  private final PartitionReassignmentOperationJournal journal =
      mock(PartitionReassignmentOperationJournal.class);
  private final PartitionReassignmentThrottleService throttleService =
      mock(PartitionReassignmentThrottleService.class);
  private final AdminClientService adminClientService = mock(AdminClientService.class);
  private final ReactiveAdminClient admin = mock(ReactiveAdminClient.class);
  private final ClustersStorage clustersStorage = mock(ClustersStorage.class);
  private final PartitionReassignmentService reassignmentService =
      mock(PartitionReassignmentService.class);
  private final AuditService auditService = mock(AuditService.class);
  private PartitionReassignmentReconciler reconciler;

  @BeforeEach
  void setUp() {
    when(clustersStorage.getClusterByName("dev")).thenReturn(Optional.of(cluster));
    when(adminClientService.get(cluster)).thenReturn(Mono.just(admin));
    reconciler = new PartitionReassignmentReconciler(
        journal,
        throttleService,
        adminClientService,
        clustersStorage,
        reassignmentService,
        new PartitionReassignmentClusterCoordinator(),
        auditService);
  }

  @Test
  void cleansUpACompletedOperationRecoveredFromTheJournal() {
    var operation = operation(PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED);
    var pending = operation.withStatus(PartitionReassignmentOperationStatus.CLEANUP_PENDING);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of()));
    when(admin.describeTopics(Set.of("orders")))
        .thenReturn(Mono.just(Map.of("orders", topicDescription(List.of(2, 3)))));
    when(throttleService.cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.COMPLETED))
        .thenReturn(Mono.just(pending.withStatus(
            PartitionReassignmentOperationStatus.COMPLETED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.COMPLETED);
  }

  @Test
  void leavesAnActiveRecoveredOperationThrottled() {
    var operation = operation(PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED);
    var topicPartition = new TopicPartition("orders", 0);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of(
        topicPartition,
        new PartitionReassignment(List.of(1, 2, 3), List.of(3), List.of(1)))));

    reconciler.reconcile().block();

    verify(throttleService, never()).cleanup(any(), any(), any());
  }

  @Test
  void doesNotPromoteAPartiallySubmittedBatch() {
    var operation = batchOperation(PartitionReassignmentOperationStatus.THROTTLE_APPLIED);
    var activePartition = new TopicPartition("orders", 0);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of(
        activePartition,
        new PartitionReassignment(List.of(1, 2, 3), List.of(3), List.of(1)))));
    when(admin.describeTopics(Set.of("orders")))
        .thenReturn(Mono.just(Map.of(
            "orders",
            topicDescription(Map.of(
                0, List.of(1, 2),
                1, List.of(1, 2))))));

    reconciler.reconcile().block();

    verify(journal, never()).update(any());
    verify(throttleService, never()).cleanup(any(), any(), any());
  }

  @Test
  void completesARecoveredThrottleWhenKafkaReachedEveryTargetAssignment() {
    var operation = operation(PartitionReassignmentOperationStatus.THROTTLE_APPLIED)
        .withExecutionResult(PartitionReassignmentOperationStatus.THROTTLE_APPLIED, 0);
    var submitted = operation.withExecutionResult(
        PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED, 1);
    var pending = submitted.withStatus(PartitionReassignmentOperationStatus.CLEANUP_PENDING);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of()));
    when(admin.describeTopics(Set.of("orders")))
        .thenReturn(Mono.just(Map.of("orders", topicDescription(List.of(2, 3)))));
    when(throttleService.cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.COMPLETED))
        .thenReturn(Mono.just(pending.withStatus(
            PartitionReassignmentOperationStatus.COMPLETED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.COMPLETED);
  }

  @Test
  void failsARecoveredThrottleWhenKafkaNeverReachedTheTargetAssignment() {
    var operation = operation(PartitionReassignmentOperationStatus.THROTTLE_APPLIED);
    var pending = operation.withStatus(PartitionReassignmentOperationStatus.CLEANUP_PENDING);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of()));
    when(admin.describeTopics(Set.of("orders")))
        .thenReturn(Mono.just(Map.of("orders", topicDescription(List.of(1, 2)))));
    when(throttleService.cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.FAILED))
        .thenReturn(Mono.just(pending.withStatus(
            PartitionReassignmentOperationStatus.FAILED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.FAILED);
  }

  @Test
  void failsASubmittedOperationWhenItsFinalAssignmentMissesTheTarget() {
    var operation = operation(PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED);
    var pending = operation.withStatus(PartitionReassignmentOperationStatus.CLEANUP_PENDING);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(admin.listPartitionReassignments(any())).thenReturn(Mono.just(Map.of()));
    when(admin.describeTopics(Set.of("orders")))
        .thenReturn(Mono.just(Map.of("orders", topicDescription(List.of(1, 2)))));
    when(throttleService.cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.FAILED))
        .thenReturn(Mono.just(pending.withStatus(
            PartitionReassignmentOperationStatus.FAILED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster,
        pending,
        PartitionReassignmentOperationStatus.FAILED);
  }

  @Test
  void rollsBackAPreparedOperationThatNeverReachedKafka() {
    var operation = operation(PartitionReassignmentOperationStatus.PREPARED);
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(operation));
    when(throttleService.cleanup(
        cluster,
        operation,
        PartitionReassignmentOperationStatus.FAILED))
        .thenReturn(Mono.just(operation.withStatus(
            PartitionReassignmentOperationStatus.FAILED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster,
        operation,
        PartitionReassignmentOperationStatus.FAILED);
    verify(admin, never()).listPartitionReassignments(any());
  }

  @Test
  void resumesAPreparedCancellationFromTheJournal() {
    var operation = cancellationOperation();
    when(journal.listUnresolved()).thenReturn(List.of(operation));
    when(journal.find("dev", "cancel-1")).thenReturn(Optional.of(operation));
    when(reassignmentService.resumeCancellationOperation(cluster, operation))
        .thenReturn(Mono.just(operation.withStatus(
            PartitionReassignmentOperationStatus.CANCELLED)));

    reconciler.reconcile().block();

    verify(reassignmentService).resumeCancellationOperation(cluster, operation);
    verify(throttleService, never()).cleanup(any(), any(), any());
  }

  @Test
  void continuesWithLaterOperationsAfterOneReconciliationFails() {
    var first = operation("operation-1", PartitionReassignmentOperationStatus.PREPARED);
    var second = operation("operation-2", PartitionReassignmentOperationStatus.PREPARED);
    when(journal.listUnresolved()).thenReturn(List.of(first, second));
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(first));
    when(journal.find("dev", "operation-2")).thenReturn(Optional.of(second));
    when(throttleService.cleanup(
        cluster, first, PartitionReassignmentOperationStatus.FAILED))
        .thenReturn(Mono.error(new IllegalStateException("first failed")));
    when(throttleService.cleanup(
        cluster, second, PartitionReassignmentOperationStatus.FAILED))
        .thenReturn(Mono.just(second.withStatus(
            PartitionReassignmentOperationStatus.FAILED)));

    reconciler.reconcile().block();

    verify(throttleService).cleanup(
        cluster, second, PartitionReassignmentOperationStatus.FAILED);
  }

  private static PartitionReassignmentOperation operation(
      PartitionReassignmentOperationStatus status) {
    return operation("operation-1", status);
  }

  private static PartitionReassignmentOperation operation(
      String operationId,
      PartitionReassignmentOperationStatus status) {
    return new PartitionReassignmentOperation(
        operationId,
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        "fingerprint",
        List.of(new PartitionAssignmentChange(
            "orders", 0, List.of(1, 2), List.of(2, 3))),
        List.of(),
        1_000_000L,
        status,
        List.of(new PartitionReassignmentConfigSnapshot(
            "BROKER",
            "1",
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            null,
            "1000000",
            PartitionReassignmentConfigState.APPLIED)),
        1,
        0,
        0,
        List.of());
  }

  private static PartitionReassignmentOperation cancellationOperation() {
    return new PartitionReassignmentOperation(
        "cancel-1",
        "dev",
        PartitionReassignmentOperationKind.CANCEL,
        "cancel:orders:0",
        List.of(),
        List.of(new PartitionReassignmentPartition("orders", 0)),
        null,
        PartitionReassignmentOperationStatus.PREPARED,
        List.of(),
        0,
        0,
        0,
        List.of());
  }

  private static PartitionReassignmentOperation batchOperation(
      PartitionReassignmentOperationStatus status) {
    return new PartitionReassignmentOperation(
        "operation-1",
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        "fingerprint",
        List.of(
            new PartitionAssignmentChange(
                "orders", 0, List.of(1, 2), List.of(2, 3)),
            new PartitionAssignmentChange(
                "orders", 1, List.of(1, 2), List.of(2, 3))),
        List.of(),
        1_000_000L,
        status,
        List.of(),
        0,
        0,
        0,
        List.of());
  }

  private static TopicDescription topicDescription(List<Integer> replicaIds) {
    return topicDescription(Map.of(0, replicaIds));
  }

  private static TopicDescription topicDescription(
      Map<Integer, List<Integer>> assignments) {
    return new TopicDescription(
        "orders",
        false,
        assignments.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> {
              var replicas = entry.getValue().stream()
                  .map(id -> new Node(id, "broker-" + id, 9092))
                  .toList();
              return new TopicPartitionInfo(
                  entry.getKey(), replicas.getFirst(), replicas, replicas);
            })
            .toList());
  }
}
