package io.kafbat.ui.model.rbac.permission;

import java.util.Set;

public enum ClusterOperationAction implements PermissibleAction {

  VIEW,
  REASSIGN_PARTITIONS(VIEW),
  ELECT_LEADERS(VIEW);

  private static final Set<ClusterOperationAction> ALTER_ACTIONS = Set.of(
      REASSIGN_PARTITIONS,
      ELECT_LEADERS
  );

  private final ClusterOperationAction[] dependantActions;

  ClusterOperationAction(ClusterOperationAction... dependantActions) {
    this.dependantActions = dependantActions;
  }

  @Override
  public boolean isAlter() {
    return ALTER_ACTIONS.contains(this);
  }

  @Override
  public PermissibleAction[] dependantActions() {
    return dependantActions;
  }
}
