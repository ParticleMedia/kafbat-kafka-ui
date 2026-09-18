package io.kafbat.ui.service.reassign;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class PartitionReassignmentClusterCoordinatorTest {

  @Test
  void skipsReconciliationWhileASubmissionOwnsTheClusterLock() {
    var coordinator = new PartitionReassignmentClusterCoordinator();
    var reconciled = new AtomicBoolean();
    var submission = coordinator.withSubmissionLock("dev", Mono.never()).subscribe();

    coordinator.withReconciliationLock(
        "dev",
        Mono.fromRunnable(() -> reconciled.set(true))).block();

    assertThat(reconciled).isFalse();
    submission.dispose();

    coordinator.withReconciliationLock(
        "dev",
        Mono.fromRunnable(() -> reconciled.set(true))).block();
    assertThat(reconciled).isTrue();
  }
}
