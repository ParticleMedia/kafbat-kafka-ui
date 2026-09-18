package io.kafbat.ui.config;

import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.nio.file.Path;
import java.time.Duration;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

@Configuration
@ConfigurationProperties("kafka.cluster-operations")
@Data
@Validated
public class ClusterOperationsProperties {

  private boolean partitionReassignmentExecutionEnabled = false;

  @Min(1)
  private int maxTopics = 100;

  @Min(1)
  private int maxPartitions = 10_000;

  @Min(1)
  private int maxReplicasPerPartition = 20;

  @Min(1)
  private long maxRequestBytes = 1_048_576;

  @NotNull
  private Duration executionTimeout = Duration.ofSeconds(30);

  @NotNull
  private Path operationJournalPath = Path.of(
      System.getProperty("java.io.tmpdir"),
      "kafbat-ui",
      "reassignment-operations");

  @AssertTrue(message = "executionTimeout must be positive")
  public boolean isExecutionTimeoutPositive() {
    return executionTimeout == null || (!executionTimeout.isZero() && !executionTimeout.isNegative());
  }
}
