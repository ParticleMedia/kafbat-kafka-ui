package io.kafbat.ui.service.reassign;

import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import io.kafbat.ui.exception.ValidationException;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PartitionReassignmentPlanService {

  private static final String HASH_FORMAT_VERSION = "partition-reassignment-plan:v1";
  private static final Comparator<PartitionAssignmentChange> CHANGE_ORDER = Comparator
      .comparing(PartitionAssignmentChange::topic)
      .thenComparingInt(PartitionAssignmentChange::partition);

  private final ClusterOperationsProperties properties;

  public PartitionReassignmentPlan createPlan(
      String clusterName,
      Map<TopicPartition, List<Integer>> currentAssignments,
      Map<TopicPartition, List<Integer>> targetAssignments,
      Set<Integer> availableBrokerIds) {
    validateClusterName(clusterName);
    requireValue(currentAssignments, "Current assignments are required");
    requireValue(targetAssignments, "Target assignments are required");
    requireValue(availableBrokerIds, "Available broker IDs are required");

    if (targetAssignments.isEmpty()) {
      throw new ValidationException("Target assignments must not be empty");
    }
    if (targetAssignments.size() > properties.getMaxPartitions()) {
      throw new ValidationException("Partition reassignment request exceeds the %d partition limit"
          .formatted(properties.getMaxPartitions()));
    }
    if (!currentAssignments.keySet().equals(targetAssignments.keySet())) {
      throw new ValidationException("Current and target assignments must contain the same partitions");
    }

    targetAssignments.keySet().forEach(PartitionReassignmentPlanService::validateTopicPartition);

    long topicCount = targetAssignments.keySet().stream()
        .map(TopicPartition::topic)
        .distinct()
        .count();
    if (topicCount > properties.getMaxTopics()) {
      throw new ValidationException("Partition reassignment request exceeds the %d topic limit"
          .formatted(properties.getMaxTopics()));
    }

    var changes = new ArrayList<PartitionAssignmentChange>();
    for (var topicPartition : targetAssignments.keySet()) {
      var currentReplicas = currentAssignments.get(topicPartition);
      var targetReplicas = targetAssignments.get(topicPartition);
      validateReplicaList(topicPartition, currentReplicas, "Current");
      validateReplicaList(topicPartition, targetReplicas, "Target");
      validateTargetBrokers(topicPartition, targetReplicas, availableBrokerIds);

      if (!currentReplicas.equals(targetReplicas)) {
        changes.add(new PartitionAssignmentChange(
            topicPartition.topic(), topicPartition.partition(), currentReplicas, targetReplicas));
      }
    }

    if (changes.isEmpty()) {
      throw new ValidationException("Partition reassignment plan contains no changes");
    }

    changes.sort(CHANGE_ORDER);
    var immutableChanges = List.copyOf(changes);
    return new PartitionReassignmentPlan(
        clusterName,
        immutableChanges,
        calculateHash(clusterName, immutableChanges));
  }

  public void validateForExecution(
      PartitionReassignmentPlan plan,
      Map<TopicPartition, List<Integer>> actualCurrentAssignments,
      Set<Integer> availableBrokerIds) {
    requireValue(plan, "Partition reassignment plan is required");
    requireValue(actualCurrentAssignments, "Current assignments are required");

    var expectedCurrentAssignments = new LinkedHashMap<TopicPartition, List<Integer>>();
    var targetAssignments = new LinkedHashMap<TopicPartition, List<Integer>>();
    for (var change : plan.changes()) {
      var topicPartition = change.topicPartition();
      if (expectedCurrentAssignments.put(topicPartition, change.currentReplicas()) != null) {
        throw new ValidationException("Partition reassignment plan contains duplicate partition "
            + topicPartition);
      }
      targetAssignments.put(topicPartition, change.targetReplicas());
    }

    var verifiedPlan = createPlan(
        plan.clusterName(),
        expectedCurrentAssignments,
        targetAssignments,
        availableBrokerIds);
    if (!hashesMatch(verifiedPlan.planHash(), plan.planHash())) {
      throw new ValidationException("Partition reassignment plan hash is invalid");
    }

    for (var entry : expectedCurrentAssignments.entrySet()) {
      if (!entry.getValue().equals(actualCurrentAssignments.get(entry.getKey()))) {
        throw new PartitionReassignmentConflictException(
            "Partition reassignment plan is stale for " + entry.getKey());
      }
    }
  }

  private void validateReplicaList(
      TopicPartition topicPartition,
      List<Integer> replicas,
      String assignmentType) {
    if (replicas == null || replicas.isEmpty()) {
      throw new ValidationException(assignmentType + " assignment for " + topicPartition
          + " must contain at least one broker");
    }
    if (replicas.size() > properties.getMaxReplicasPerPartition()) {
      throw new ValidationException(assignmentType + " assignment for " + topicPartition
          + " exceeds the %d replica limit".formatted(properties.getMaxReplicasPerPartition()));
    }
    if (replicas.stream().anyMatch(Objects::isNull)) {
      throw new ValidationException(assignmentType + " assignment for " + topicPartition
          + " contains a null broker ID");
    }
    if (replicas.stream().anyMatch(brokerId -> brokerId < 0)) {
      throw new ValidationException(assignmentType + " assignment for " + topicPartition
          + " contains a negative broker ID");
    }
    if (new HashSet<>(replicas).size() != replicas.size()) {
      throw new ValidationException(assignmentType + " assignment for " + topicPartition
          + " contains duplicate brokers");
    }
  }

  private void validateTargetBrokers(
      TopicPartition topicPartition,
      List<Integer> targetReplicas,
      Set<Integer> availableBrokerIds) {
    for (var brokerId : targetReplicas) {
      if (!availableBrokerIds.contains(brokerId)) {
        throw new ValidationException("Target assignment for " + topicPartition
            + " contains unavailable broker " + brokerId);
      }
    }
  }

  private static void validateClusterName(String clusterName) {
    if (clusterName == null || clusterName.isBlank()) {
      throw new ValidationException("Cluster name is required");
    }
  }

  private static void validateTopicPartition(TopicPartition topicPartition) {
    if (topicPartition == null) {
      throw new ValidationException("Assignments must not contain a null partition");
    }
    if (topicPartition.topic().isBlank()) {
      throw new ValidationException("Assignment topic name must not be blank");
    }
    if (topicPartition.partition() < 0) {
      throw new ValidationException("Assignment partition must not be negative");
    }
  }

  private static boolean hashesMatch(String expected, String actual) {
    if (actual == null) {
      return false;
    }
    return MessageDigest.isEqual(
        expected.getBytes(StandardCharsets.US_ASCII),
        actual.getBytes(StandardCharsets.US_ASCII));
  }

  private static String calculateHash(
      String clusterName,
      List<PartitionAssignmentChange> changes) {
    try {
      var canonicalBytes = new ByteArrayOutputStream();
      try (var output = new DataOutputStream(canonicalBytes)) {
        writeString(output, HASH_FORMAT_VERSION);
        writeString(output, clusterName);
        var sortedChanges = changes.stream().sorted(CHANGE_ORDER).toList();
        output.writeInt(sortedChanges.size());
        for (var change : sortedChanges) {
          writeString(output, change.topic());
          output.writeInt(change.partition());
          writeReplicas(output, change.currentReplicas());
          writeReplicas(output, change.targetReplicas());
        }
      }
      return HexFormat.of().formatHex(sha256().digest(canonicalBytes.toByteArray()));
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to encode partition reassignment plan", e);
    }
  }

  private static void writeString(DataOutputStream output, String value) throws IOException {
    var bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static void writeReplicas(DataOutputStream output, List<Integer> replicas) throws IOException {
    output.writeInt(replicas.size());
    for (var replica : replicas) {
      output.writeInt(replica);
    }
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is not available", e);
    }
  }

  private static <T> void requireValue(T value, String message) {
    if (value == null) {
      throw new ValidationException(message);
    }
  }
}
