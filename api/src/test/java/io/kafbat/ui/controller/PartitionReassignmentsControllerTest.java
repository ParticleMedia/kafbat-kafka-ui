package io.kafbat.ui.controller;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.PartitionAssignmentChangeDTO;
import io.kafbat.ui.model.PartitionReassignmentCancellationRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentExecutionRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentPartitionDTO;
import io.kafbat.ui.model.PartitionReassignmentPlanRequestDTO;
import io.kafbat.ui.model.PartitionReassignmentTargetDTO;
import io.kafbat.ui.model.PartitionReassignmentValidationRequestDTO;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.model.rbac.Resource;
import io.kafbat.ui.model.rbac.permission.ClusterOperationAction;
import io.kafbat.ui.service.ClustersStorage;
import io.kafbat.ui.service.audit.AuditService;
import io.kafbat.ui.service.rbac.AccessControlService;
import io.kafbat.ui.service.reassign.ActivePartitionReassignment;
import io.kafbat.ui.service.reassign.PartitionAssignmentChange;
import io.kafbat.ui.service.reassign.PartitionReassignmentCancellation;
import io.kafbat.ui.service.reassign.PartitionReassignmentCapabilities;
import io.kafbat.ui.service.reassign.PartitionReassignmentExecution;
import io.kafbat.ui.service.reassign.PartitionReassignmentOperationStatus;
import io.kafbat.ui.service.reassign.PartitionReassignmentPlan;
import io.kafbat.ui.service.reassign.PartitionReassignmentService;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

class PartitionReassignmentsControllerTest {

  private final KafkaCluster cluster = KafkaCluster.builder().name("dev").build();
  private final PartitionReassignmentService service = mock(PartitionReassignmentService.class);
  private final AccessControlService accessControlService = mock(AccessControlService.class);
  private PartitionReassignmentsController controller;

  @BeforeEach
  void setUp() {
    var clustersStorage = mock(ClustersStorage.class);
    when(clustersStorage.getClusterByName("dev")).thenReturn(java.util.Optional.of(cluster));
    when(accessControlService.validateAccess(any())).thenReturn(Mono.empty());
    when(service.getCurrentOperation(cluster)).thenReturn(Mono.just(Optional.empty()));

    controller = new PartitionReassignmentsController(service);
    controller.setClustersStorage(clustersStorage);
    controller.setAccessControlService(accessControlService);
    controller.setAuditService(mock(AuditService.class));
  }

  @Test
  void createsReadOnlyPlanAndRequiresClusterOperationView() {
    var plan = new PartitionReassignmentPlan(
        "dev",
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 3))),
        "hash");
    when(service.createPlan(any(), any())).thenReturn(Mono.just(plan));
    var request = new PartitionReassignmentPlanRequestDTO().assignments(List.of(
        new PartitionReassignmentTargetDTO()
            .topic("orders")
            .partition(0)
            .replicas(List.of(2, 3))));

    var response = controller.createPartitionReassignmentPlan("dev", Mono.just(request), null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getPlanHash()).isEqualTo("hash");
    assertThat(response.getBody().getChanges()).singleElement().satisfies(change -> {
      assertThat(change.getCurrentReplicas()).containsExactly(1, 2);
      assertThat(change.getTargetReplicas()).containsExactly(2, 3);
    });
    assertViewPermissionWasChecked("createPartitionReassignmentPlan");
  }

  @Test
  void revalidatesPlanAndReturnsValidResult() {
    when(service.validatePlan(any(), any())).thenReturn(Mono.empty());
    var request = new PartitionReassignmentValidationRequestDTO()
        .planHash("hash")
        .changes(List.of(new PartitionAssignmentChangeDTO()
            .topic("orders")
            .partition(0)
            .currentReplicas(List.of(1, 2))
            .targetReplicas(List.of(2, 3))));

    var response = controller.validatePartitionReassignmentPlan(
        "dev", Mono.just(request), null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getValid()).isTrue();
    verify(service).validatePlan(any(), any());
    assertViewPermissionWasChecked("validatePartitionReassignmentPlan");
  }

  @Test
  void executesPlanAndRequiresPartitionReassignmentPermission() {
    when(service.executePlan(any(), any(), any(), any())).thenReturn(Mono.just(
        new PartitionReassignmentExecution(
            "operation-1",
            1,
            PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED)));
    var request = new PartitionReassignmentExecutionRequestDTO()
        .operationId("operation-1")
        .throttleBytesPerSecond(1_000_000L)
        .planHash("hash")
        .changes(List.of(new PartitionAssignmentChangeDTO()
            .topic("orders")
            .partition(0)
            .currentReplicas(List.of(1, 2))
            .targetReplicas(List.of(2, 3))));

    var response = controller.executePartitionReassignmentPlan(
        "dev", Mono.just(request), null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getAcceptedPartitions()).isEqualTo(1);
    assertThat(response.getBody().getOperationId()).isEqualTo("operation-1");
    assertThat(response.getBody().getStatus().getValue())
        .isEqualTo("REASSIGNMENT_SUBMITTED");
    verify(service).executePlan(any(), any(),
        org.mockito.ArgumentMatchers.eq("operation-1"),
        org.mockito.ArgumentMatchers.eq(1_000_000L));
    assertPermissionWasChecked(
        "executePartitionReassignmentPlan",
        ClusterOperationAction.REASSIGN_PARTITIONS);
  }

  @Test
  void cancelsSelectedPartitionsAndRequiresPartitionReassignmentPermission() {
    when(service.cancelReassignments(any(), any(), any())).thenReturn(Mono.just(
        new PartitionReassignmentCancellation("cancel-1", 1, 1)));
    var request = new PartitionReassignmentCancellationRequestDTO()
        .operationId("cancel-1")
        .partitions(List.of(
            new PartitionReassignmentPartitionDTO().topic("orders").partition(0),
            new PartitionReassignmentPartitionDTO().topic("orders").partition(1)));

    var response = controller.cancelPartitionReassignments(
        "dev", Mono.just(request), null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getCancelledPartitions()).isEqualTo(1);
    assertThat(response.getBody().getSkippedPartitions()).isEqualTo(1);
    assertPermissionWasChecked(
        "cancelPartitionReassignments",
        ClusterOperationAction.REASSIGN_PARTITIONS);
  }

  @Test
  void returnsExecutionCapabilitiesAndRequiresClusterOperationView() {
    when(service.getCapabilities(cluster)).thenReturn(
        new PartitionReassignmentCapabilities(false, "Execution is disabled."));

    var response = controller.getPartitionReassignmentCapabilities("dev", null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody()).isNotNull();
    assertThat(response.getBody().getExecutionEnabled()).isFalse();
    assertThat(response.getBody().getReason()).isEqualTo("Execution is disabled.");
    assertViewPermissionWasChecked("getPartitionReassignmentCapabilities");
  }

  @Test
  void returnsActiveReassignmentsAndRequiresClusterOperationView() {
    when(service.listActiveReassignments(cluster)).thenReturn(Mono.just(List.of(
        new ActivePartitionReassignment(
            "orders", 0, List.of(1, 2), List.of(2, 3), List.of(3), List.of(1), 50))));

    var response = controller.listActivePartitionReassignments("dev", null).block();

    assertThat(response).isNotNull();
    assertThat(response.getBody().getReassignments()).singleElement().satisfies(reassignment -> {
      assertThat(reassignment.getTopic()).isEqualTo("orders");
      assertThat(reassignment.getCurrentReplicas()).containsExactly(1, 2);
      assertThat(reassignment.getTargetReplicas()).containsExactly(2, 3);
      assertThat(reassignment.getProgressPercent()).isEqualTo(50);
    });
    assertViewPermissionWasChecked("listActivePartitionReassignments");
  }

  private void assertViewPermissionWasChecked(String operationName) {
    assertPermissionWasChecked(operationName, ClusterOperationAction.VIEW);
  }

  private void assertPermissionWasChecked(
      String operationName,
      ClusterOperationAction action) {
    var captor = ArgumentCaptor.forClass(AccessContext.class);
    verify(accessControlService).validateAccess(captor.capture());
    var context = captor.getValue();
    assertThat(context.cluster()).isEqualTo("dev");
    assertThat(context.operationName()).isEqualTo(operationName);
    assertThat(context.accessedResources()).singleElement().satisfies(access -> {
      assertThat(access.resourceType()).isEqualTo(Resource.CLUSTER_OPERATION);
      assertThat(access.requestedActions())
          .extracting(Object::toString)
          .containsExactly(action.toString());
    });
  }
}
