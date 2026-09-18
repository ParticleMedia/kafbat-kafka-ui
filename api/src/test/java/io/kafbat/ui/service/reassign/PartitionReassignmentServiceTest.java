package io.kafbat.ui.service.reassign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ReactiveAdminClient;
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

class PartitionReassignmentServiceTest {

  private final KafkaCluster cluster = KafkaCluster.builder().name("dev").build();
  private final AdminClientService adminClientService = mock(AdminClientService.class);
  private final ReactiveAdminClient adminClient = mock(ReactiveAdminClient.class);
  private final ClusterOperationsProperties properties = new ClusterOperationsProperties();
  private final PartitionReassignmentOperationJournal journal =
      mock(PartitionReassignmentOperationJournal.class);
  private final PartitionReassignmentThrottleService throttleService =
      mock(PartitionReassignmentThrottleService.class);
  private PartitionReassignmentService service;

  @BeforeEach
  void setUp() {
    when(adminClientService.get(cluster)).thenReturn(Mono.just(adminClient));
    when(journal.find(any(), any())).thenReturn(Optional.empty());
    when(journal.create(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    when(throttleService.apply(any(), any())).thenAnswer(invocation -> {
      PartitionReassignmentOperation operation = invocation.getArgument(1);
      return Mono.just(operation.withStatus(
          PartitionReassignmentOperationStatus.THROTTLE_APPLIED));
    });
    service = new PartitionReassignmentService(
        adminClientService,
        new PartitionReassignmentPlanService(properties),
        properties,
        journal,
        throttleService,
        new PartitionReassignmentClusterCoordinator());
  }

  @Test
  void createsPlanFromKafkaAssignmentsInsteadOfClientSuppliedCurrentState() {
    stubClusterState(List.of(1, 2, 3), "orders", List.of(
        partition(0, List.of(1, 2)),
        partition(1, List.of(2, 3))));

    var plan = service.createPlan(cluster, List.of(
        new PartitionReassignmentTarget("orders", 0, List.of(2, 3))))
        .block();

    assertThat(plan).isNotNull();
    assertThat(plan.clusterName()).isEqualTo("dev");
    assertThat(plan.changes()).containsExactly(
        new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3)));
  }

  @Test
  void rejectsDuplicatePartitionsBeforeReadingKafka() {
    var targets = List.of(
        new PartitionReassignmentTarget("orders", 0, List.of(1, 2)),
        new PartitionReassignmentTarget("orders", 0, List.of(2, 3)));

    assertThatThrownBy(() -> service.createPlan(cluster, targets).block())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Partition reassignment request contains duplicate partition orders-0");
  }

  @Test
  void rejectsEmptyAssignmentsBeforeReadingKafka() {
    assertThatThrownBy(() -> service.createPlan(cluster, List.of()).block())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Target assignments must not be empty");

    verifyNoInteractions(adminClient);
  }

  @Test
  void rejectsBlankTopicBeforeReadingKafka() {
    assertThatThrownBy(() -> service.createPlan(cluster, List.of(
        new PartitionReassignmentTarget("", 0, List.of(1)))).block())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Assignment topic name must not be blank");

    verifyNoInteractions(adminClient);
  }

  @Test
  void rejectsNegativePartitionBeforeReadingKafka() {
    assertThatThrownBy(() -> service.createPlan(cluster, List.of(
        new PartitionReassignmentTarget("orders", -1, List.of(1)))).block())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Assignment partition must not be negative");

    verifyNoInteractions(adminClient);
  }

  @Test
  void rejectsPartitionsThatDoNotExistInKafka() {
    stubClusterState(List.of(1, 2), "orders", List.of(partition(0, List.of(1, 2))));

    assertThatThrownBy(() -> service.createPlan(cluster, List.of(
        new PartitionReassignmentTarget("orders", 7, List.of(2, 1)))).block())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Current assignment not found for orders-7");
  }

  @Test
  void revalidatesPlanAgainstFreshKafkaAssignments() {
    stubClusterState(List.of(1, 2, 3), "orders", List.of(partition(0, List.of(1, 2))));
    var plan = new PartitionReassignmentPlanService(new ClusterOperationsProperties()).createPlan(
        "dev",
        Map.of(new TopicPartition("orders", 0), List.of(1, 2)),
        Map.of(new TopicPartition("orders", 0), List.of(2, 3)),
        Set.of(1, 2, 3));

    service.validatePlan(cluster, plan).block();
  }

  @Test
  void executesAValidatedPlanWithTheRequestedReplicaOrder() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var topicPartition = new TopicPartition("orders", 0);
    stubClusterState(List.of(1, 2, 3), "orders", List.of(partition(0, List.of(1, 2))));
    when(adminClient.listPartitionReassignments(Set.of(topicPartition)))
        .thenReturn(Mono.just(Map.of()));
    when(adminClient.alterPartitionReassignments(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Mono.empty());
    var plan = new PartitionReassignmentPlanService(properties).createPlan(
        "dev",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(3, 2)),
        Set.of(1, 2, 3));

    assertThat(service.executePlan(cluster, plan).block()).isEqualTo(1);

    verify(adminClient).alterPartitionReassignments(org.mockito.ArgumentMatchers.argThat(
        assignments -> assignments.size() == 1
            && assignments.containsKey(topicPartition)
            && assignments.get(topicPartition).isPresent()
            && assignments.get(topicPartition).orElseThrow().targetReplicas()
                .equals(List.of(3, 2))));
  }

  @Test
  void executesAThrottledPlanAsAPersistedOperation() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var topicPartition = new TopicPartition("orders", 0);
    stubClusterState(List.of(1, 2, 3), "orders", List.of(partition(0, List.of(1, 2))));
    when(adminClient.listPartitionReassignments(Set.of(topicPartition)))
        .thenReturn(Mono.just(Map.of()));
    when(adminClient.alterPartitionReassignments(any())).thenReturn(Mono.empty());
    var plan = new PartitionReassignmentPlanService(properties).createPlan(
        "dev",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(2, 3)),
        Set.of(1, 2, 3));

    var result = service.executePlan(
        cluster, plan, "operation-1", 1_000_000L).block();

    assertThat(result).isNotNull();
    assertThat(result.operationId()).isEqualTo("operation-1");
    assertThat(result.acceptedPartitions()).isEqualTo(1);
    assertThat(result.status())
        .isEqualTo(PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED);
    verify(throttleService).apply(any(), any());
    verify(journal).create(any());
  }

  @Test
  void replaysAPersistedExecutionWithoutSubmittingItAgain() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var plan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "hash");
    var existing = new PartitionReassignmentOperation(
        "operation-1",
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        PartitionReassignmentService.executionFingerprint(plan, 1_000_000L),
        plan.changes(),
        List.of(),
        1_000_000L,
        PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
        List.of(),
        1,
        0,
        0,
        List.of());
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(existing));

    var result = service.executePlan(
        cluster, plan, "operation-1", 1_000_000L).block();

    assertThat(result).isNotNull();
    assertThat(result.acceptedPartitions()).isEqualTo(1);
    verifyNoInteractions(adminClient);
  }

  @Test
  void rejectsAnExecutionReplayWhoseChangesDoNotMatchTheRecordedRequest() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var recordedPlan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "same-hash");
    var changedPlan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(3, 2))),
        "same-hash");
    var existing = new PartitionReassignmentOperation(
        "operation-1",
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        PartitionReassignmentService.executionFingerprint(recordedPlan, null),
        recordedPlan.changes(),
        List.of(),
        null,
        PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
        List.of(),
        1,
        0,
        0,
        List.of());
    when(journal.find("dev", "operation-1")).thenReturn(Optional.of(existing));

    assertThatThrownBy(() -> service.executePlan(
        cluster, changedPlan, "operation-1", null).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentConflictException.class);

    verifyNoInteractions(adminClient);
  }

  @Test
  void rejectsANewExecutionWhileAThrottledOperationNeedsCleanup() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var plan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "hash");
    var existing = new PartitionReassignmentOperation(
        "operation-1",
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        PartitionReassignmentService.executionFingerprint(plan, 1_000_000L),
        plan.changes(),
        List.of(),
        1_000_000L,
        PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
        List.of(),
        1,
        0,
        0,
        List.of());
    when(journal.list("dev")).thenReturn(List.of(existing));

    assertThatThrownBy(() -> service.executePlan(
        cluster, plan, "operation-2", null).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentConflictException.class)
        .hasMessageContaining("operation-1");

    verifyNoInteractions(adminClient);
  }

  @Test
  void cancelsOnlySelectedActivePartitionsAndSkipsFinishedOnes() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var active = new TopicPartition("orders", 0);
    var finished = new TopicPartition("orders", 1);
    when(adminClient.listPartitionReassignments(Set.of(active, finished)))
        .thenReturn(Mono.just(Map.of(
            active,
            new PartitionReassignment(List.of(1, 2, 3), List.of(3), List.of(1)))));
    when(adminClient.alterPartitionReassignments(any())).thenReturn(Mono.empty());

    var result = service.cancelReassignments(
        cluster,
        "cancel-1",
        List.of(
            new PartitionReassignmentPartition("orders", 0),
            new PartitionReassignmentPartition("orders", 1)))
        .block();

    assertThat(result).isNotNull();
    assertThat(result.cancelledPartitions()).isEqualTo(1);
    assertThat(result.skippedPartitions()).isEqualTo(1);
    verify(adminClient).alterPartitionReassignments(
        org.mockito.ArgumentMatchers.argThat(assignments ->
            assignments.size() == 1
                && assignments.containsKey(active)
                && assignments.get(active).equals(Optional.empty())));
  }

  @Test
  void resumesAPreparedCancellationInsteadOfReportingUnconfirmedCounts() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var partition = new PartitionReassignmentPartition("orders", 0);
    var existing = new PartitionReassignmentOperation(
        "cancel-1",
        "dev",
        PartitionReassignmentOperationKind.CANCEL,
        "cancel:orders:0",
        List.of(),
        List.of(partition),
        null,
        PartitionReassignmentOperationStatus.PREPARED,
        List.of(),
        0,
        1,
        0,
        List.of());
    when(journal.find("dev", "cancel-1")).thenReturn(Optional.of(existing));
    when(adminClient.listPartitionReassignments(Set.of(partition.topicPartition())))
        .thenReturn(Mono.just(Map.of()));

    var result = service.cancelReassignments(
        cluster, "cancel-1", List.of(partition)).block();

    assertThat(result).isNotNull();
    assertThat(result.cancelledPartitions()).isZero();
    assertThat(result.skippedPartitions()).isEqualTo(1);
    verify(journal).update(org.mockito.ArgumentMatchers.argThat(operation ->
        operation.status() == PartitionReassignmentOperationStatus.CANCELLED
            && operation.cancelledPartitions() == 0
            && operation.skippedPartitions() == 1));
  }

  @Test
  void rejectsExecutionWhenTheFeatureIsDisabled() {
    var plan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "hash");

    assertThatThrownBy(() -> service.executePlan(cluster, plan).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentExecutionDisabledException.class);

    verifyNoInteractions(adminClientService);
  }

  @Test
  void rejectsExecutionForAReadOnlyCluster() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var readOnlyCluster = KafkaCluster.builder().name("readonly").readOnly(true).build();
    var plan = new PartitionReassignmentPlan(
        "readonly",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "hash");

    assertThatThrownBy(() -> service.executePlan(readOnlyCluster, plan).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentExecutionDisabledException.class);

    verifyNoInteractions(adminClientService);
  }

  @Test
  void reportsWhyExecutionIsUnavailable() {
    assertThat(service.getCapabilities(cluster))
        .isEqualTo(new PartitionReassignmentCapabilities(
            false,
            "Partition reassignment execution is disabled by server configuration."));

    properties.setPartitionReassignmentExecutionEnabled(true);
    var readOnlyCluster = KafkaCluster.builder().name("readonly").readOnly(true).build();
    assertThat(service.getCapabilities(readOnlyCluster))
        .isEqualTo(new PartitionReassignmentCapabilities(
            false,
            "This cluster is configured as read-only."));
  }

  @Test
  void reportsWhenExecutionIsAvailable() {
    properties.setPartitionReassignmentExecutionEnabled(true);

    assertThat(service.getCapabilities(cluster))
        .isEqualTo(new PartitionReassignmentCapabilities(true, null));
  }

  @Test
  void listsActiveReassignmentsWithTargetIsrProgress() {
    var topicPartition = new TopicPartition("orders", 0);
    when(adminClient.listPartitionReassignments()).thenReturn(Mono.just(Map.of(
        topicPartition,
        new PartitionReassignment(List.of(1, 2, 3), List.of(3), List.of(1)))));
    when(adminClient.describeTopics(Set.of("orders"))).thenReturn(Mono.just(Map.of(
        "orders",
        new TopicDescription("orders", false, List.of(
            partition(0, List.of(1, 2, 3), List.of(1, 2)))))));

    assertThat(service.listActiveReassignments(cluster).block())
        .containsExactly(new ActivePartitionReassignment(
            "orders",
            0,
            List.of(1, 2),
            List.of(2, 3),
            List.of(3),
            List.of(1),
            50));
  }

  @Test
  void returnsAnEmptyActiveReassignmentListWithoutDescribingTopics() {
    when(adminClient.listPartitionReassignments()).thenReturn(Mono.just(Map.of()));

    assertThat(service.listActiveReassignments(cluster).block()).isEmpty();

    verify(adminClient, org.mockito.Mockito.never()).describeTopics(any());
  }

  @Test
  void rejectsExecutionWhenARequestedPartitionIsAlreadyBeingReassigned() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var topicPartition = new TopicPartition("orders", 0);
    stubClusterState(List.of(1, 2, 3), "orders", List.of(partition(0, List.of(1, 2))));
    when(adminClient.listPartitionReassignments(Set.of(topicPartition))).thenReturn(Mono.just(Map.of(
        topicPartition, new PartitionReassignment(List.of(1, 2), List.of(), List.of()))));
    var plan = new PartitionReassignmentPlanService(properties).createPlan(
        "dev",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(2, 3)),
        Set.of(1, 2, 3));

    assertThatThrownBy(() -> service.executePlan(cluster, plan).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentConflictException.class)
        .hasMessage("Partition reassignment is already in progress for orders-0");

    verify(adminClient, org.mockito.Mockito.never())
        .alterPartitionReassignments(org.mockito.ArgumentMatchers.any());
  }

  @Test
  void rejectsConcurrentExecutionRequestsForTheSameCluster() {
    properties.setPartitionReassignmentExecutionEnabled(true);
    var topicPartition = new TopicPartition("orders", 0);
    stubClusterState(List.of(1, 2, 3), "orders", List.of(partition(0, List.of(1, 2))));
    when(adminClient.listPartitionReassignments(Set.of(topicPartition)))
        .thenReturn(Mono.just(Map.of()));
    var submissions = new java.util.concurrent.atomic.AtomicInteger();
    when(adminClient.alterPartitionReassignments(org.mockito.ArgumentMatchers.any()))
        .thenAnswer(invocation -> submissions.getAndIncrement() == 0
            ? Mono.never()
            : Mono.empty());
    var plan = new PartitionReassignmentPlanService(properties).createPlan(
        "dev",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(2, 3)),
        Set.of(1, 2, 3));

    var firstExecution = service.executePlan(cluster, plan).subscribe();
    verify(adminClient, org.mockito.Mockito.timeout(1_000))
        .alterPartitionReassignments(org.mockito.ArgumentMatchers.any());

    assertThatThrownBy(() -> service.executePlan(cluster, plan).block())
        .isInstanceOf(io.kafbat.ui.exception.PartitionReassignmentConflictException.class)
        .hasMessage("Another partition reassignment request is being submitted for cluster dev");

    firstExecution.dispose();
  }

  private void stubClusterState(
      List<Integer> brokerIds,
      String topic,
      List<TopicPartitionInfo> partitions) {
    var nodes = brokerIds.stream().map(id -> new Node(id, "broker-" + id, 9092)).toList();
    when(adminClient.describeCluster()).thenReturn(Mono.just(
        new ReactiveAdminClient.ClusterDescription(nodes.getFirst(), "cluster-id", nodes, Set.of())));
    when(adminClient.describeTopics(Set.of(topic))).thenReturn(Mono.just(Map.of(
        topic, new TopicDescription(topic, false, partitions))));
  }

  private static TopicPartitionInfo partition(int id, List<Integer> replicas) {
    var nodes = replicas.stream().map(broker -> new Node(broker, "broker-" + broker, 9092)).toList();
    return new TopicPartitionInfo(id, nodes.getFirst(), nodes, nodes);
  }

  private static TopicPartitionInfo partition(
      int id,
      List<Integer> replicas,
      List<Integer> inSyncReplicas) {
    var nodes = replicas.stream().map(broker -> new Node(broker, "broker-" + broker, 9092)).toList();
    var inSyncNodes = inSyncReplicas.stream()
        .map(broker -> new Node(broker, "broker-" + broker, 9092))
        .toList();
    return new TopicPartitionInfo(id, nodes.getFirst(), nodes, inSyncNodes);
  }
}
