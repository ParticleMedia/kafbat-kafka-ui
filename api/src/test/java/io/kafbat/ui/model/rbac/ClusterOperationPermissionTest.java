package io.kafbat.ui.model.rbac;

import static org.assertj.core.api.Assertions.assertThat;

import io.kafbat.ui.model.rbac.permission.ClusterOperationAction;
import java.util.List;
import org.junit.jupiter.api.Test;

class ClusterOperationPermissionTest {

  @Test
  void alterActionsIncludeViewPermission() {
    assertThat(Resource.CLUSTER_OPERATION.parseActionsWithDependantsUnnest(
        List.of("reassign_partitions", "elect_leaders")))
        .containsExactly(
            ClusterOperationAction.REASSIGN_PARTITIONS,
            ClusterOperationAction.VIEW,
            ClusterOperationAction.ELECT_LEADERS);
  }

  @Test
  void accessContextBuilderAddsClusterOperationPermission() {
    var context = AccessContext.builder()
        .cluster("local")
        .clusterOperationActions(ClusterOperationAction.REASSIGN_PARTITIONS)
        .build();

    assertThat(context.accessedResources()).singleElement().satisfies(access -> {
      assertThat(access.resourceType()).isEqualTo(Resource.CLUSTER_OPERATION);
      assertThat(access.resourceId()).isNull();
      assertThat(access.requestedActions())
          .isEqualTo(List.of(ClusterOperationAction.REASSIGN_PARTITIONS));
    });
  }

  @Test
  void clusterOperationActionsDeclareWhetherTheyAlterState() {
    assertThat(ClusterOperationAction.VIEW.isAlter()).isFalse();
    assertThat(ClusterOperationAction.REASSIGN_PARTITIONS.isAlter()).isTrue();
    assertThat(ClusterOperationAction.ELECT_LEADERS.isAlter()).isTrue();
  }

  @Test
  void topicAndClusterConfigPermissionsDoNotGrantClusterOperationAccess() {
    var context = AccessContext.builder()
        .cluster("local")
        .clusterOperationActions(ClusterOperationAction.REASSIGN_PARTITIONS)
        .build();

    assertThat(context.isAccessible(List.of(
        permission(Resource.TOPIC, "EDIT"),
        permission(Resource.CLUSTERCONFIG, "EDIT"))))
        .isFalse();
    assertThat(context.isAccessible(List.of(
        permission(Resource.CLUSTER_OPERATION, "REASSIGN_PARTITIONS"))))
        .isTrue();
  }

  private static Permission permission(Resource resource, String action) {
    var permission = new Permission();
    permission.setResource(resource.name());
    permission.setActions(List.of(action));
    permission.validate();
    permission.transform();
    return permission;
  }
}
