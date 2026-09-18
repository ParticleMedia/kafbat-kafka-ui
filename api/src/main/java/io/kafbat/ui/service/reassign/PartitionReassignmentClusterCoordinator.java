package io.kafbat.ui.service.reassign;

import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class PartitionReassignmentClusterCoordinator {

  private final Set<String> busyClusters = ConcurrentHashMap.newKeySet();

  public <T> Mono<T> withSubmissionLock(String clusterName, Mono<T> action) {
    return withLock(clusterName, action, true);
  }

  public <T> Mono<T> withReconciliationLock(String clusterName, Mono<T> action) {
    return withLock(clusterName, action, false);
  }

  private <T> Mono<T> withLock(
      String clusterName,
      Mono<T> action,
      boolean failWhenBusy) {
    return Mono.defer(() -> {
      if (!busyClusters.add(clusterName)) {
        if (failWhenBusy) {
          return Mono.error(new PartitionReassignmentConflictException(
              "Another partition reassignment request is being submitted for cluster "
                  + clusterName));
        }
        return Mono.empty();
      }
      return action
          .doOnSuccess(ignored -> busyClusters.remove(clusterName))
          .doOnError(ignored -> busyClusters.remove(clusterName))
          .doOnCancel(() -> busyClusters.remove(clusterName));
    });
  }
}
