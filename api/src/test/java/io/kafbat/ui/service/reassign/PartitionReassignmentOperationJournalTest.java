package io.kafbat.ui.service.reassign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class PartitionReassignmentOperationJournalTest {

  @TempDir
  Path directory;

  @Test
  void persistsAndReloadsAnOperation() {
    try (var journal = journal(directory)) {
      var operation = operation("operation-1", "fingerprint-1");

      assertThat(journal.create(operation)).isEqualTo(operation);
      assertThat(journal.find("dev", "operation-1")).contains(operation);
    }

    try (var reopened = journal(directory)) {
      assertThat(reopened.find("dev", "operation-1"))
          .contains(operation("operation-1", "fingerprint-1"));
    }
  }

  @Test
  void replaysTheSameOperationButRejectsOperationIdReuse() {
    try (var journal = journal(directory)) {
      var operation = operation("operation-1", "fingerprint-1");

      assertThat(journal.create(operation)).isEqualTo(operation);
      assertThat(journal.create(operation)).isEqualTo(operation);

      assertThatThrownBy(() -> journal.create(operation("operation-1", "different")))
          .isInstanceOf(PartitionReassignmentConflictException.class)
          .hasMessageContaining("operation-1");
    }
  }

  @Test
  void returnsOnlyUnresolvedOperations() {
    try (var journal = journal(directory)) {
      var active = operation("active", "fingerprint-1");
      var completed = operation("completed", "fingerprint-2")
          .withStatus(PartitionReassignmentOperationStatus.COMPLETED);
      journal.create(active);
      journal.create(completed);

      assertThat(journal.listUnresolved())
          .extracting(PartitionReassignmentOperation::operationId)
          .containsExactly("active");
    }
  }

  @Test
  void clusterNamesCannotEscapeTheConfiguredDirectory() throws Exception {
    try (var journal = journal(directory)) {
      journal.create(operation("operation-1", "fingerprint-1", "../../outside"));
    }

    try (var files = Files.list(directory)) {
      assertThat(files)
          .allMatch(path -> path.getParent().equals(directory))
          .noneMatch(path -> path.getFileName().toString().contains("outside"));
    }
  }

  @Test
  void refusesASecondJournalOwner() {
    try (var first = journal(directory)) {
      first.create(operation("operation-1", "fingerprint-1"));

      assertThatThrownBy(() -> journal(directory).create(
          operation("operation-2", "fingerprint-2")))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("already in use");
    }
  }

  private static PartitionReassignmentOperationJournal journal(Path directory) {
    var properties = new ClusterOperationsProperties();
    properties.setOperationJournalPath(directory);
    return new PartitionReassignmentOperationJournal(
        new ObjectMapper().findAndRegisterModules(), properties);
  }

  private static PartitionReassignmentOperation operation(
      String operationId,
      String fingerprint) {
    return operation(operationId, fingerprint, "dev");
  }

  private static PartitionReassignmentOperation operation(
      String operationId,
      String fingerprint,
      String clusterName) {
    return new PartitionReassignmentOperation(
        operationId,
        clusterName,
        PartitionReassignmentOperationKind.EXECUTE,
        fingerprint,
        List.of(new PartitionAssignmentChange(
            "orders", 0, List.of(1, 2), List.of(2, 3))),
        List.of(),
        1_000_000L,
        PartitionReassignmentOperationStatus.PREPARED,
        List.of(),
        0,
        0,
        0,
        List.of());
  }
}
