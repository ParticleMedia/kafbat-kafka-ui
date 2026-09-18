package io.kafbat.ui.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Validation;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class ClusterOperationsPropertiesTest {

  @Test
  void providesConservativeDefaults() {
    var properties = new ClusterOperationsProperties();

    assertThat(properties.getMaxTopics()).isEqualTo(100);
    assertThat(properties.getMaxPartitions()).isEqualTo(10_000);
    assertThat(properties.getMaxReplicasPerPartition()).isEqualTo(20);
    assertThat(properties.getMaxRequestBytes()).isEqualTo(1_048_576);
    assertThat(properties.getExecutionTimeout()).isEqualTo(Duration.ofSeconds(30));
    assertThat(properties.getOperationJournalPath()).isEqualTo(Path.of(
        System.getProperty("java.io.tmpdir"),
        "kafbat-ui",
        "reassignment-operations"));
    assertThat(properties.isPartitionReassignmentExecutionEnabled()).isFalse();
  }

  @Test
  void rejectsNonPositiveLimits() {
    var properties = new ClusterOperationsProperties();
    properties.setMaxTopics(0);
    properties.setMaxPartitions(0);
    properties.setMaxReplicasPerPartition(0);
    properties.setMaxRequestBytes(0);
    properties.setExecutionTimeout(Duration.ZERO);

    try (var validatorFactory = Validation.buildDefaultValidatorFactory()) {
      assertThat(validatorFactory.getValidator().validate(properties))
          .extracting(violation -> violation.getPropertyPath().toString())
          .containsExactlyInAnyOrder(
              "maxTopics",
              "maxPartitions",
              "maxReplicasPerPartition",
              "maxRequestBytes",
              "executionTimeoutPositive");
    }
  }
}
