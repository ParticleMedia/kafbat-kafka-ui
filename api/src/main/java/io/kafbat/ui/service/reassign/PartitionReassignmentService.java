package io.kafbat.ui.service.reassign;

import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import io.kafbat.ui.exception.PartitionReassignmentExecutionDisabledException;
import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class PartitionReassignmentService {

  private final AdminClientService adminClientService;
  private final PartitionReassignmentPlanService planService;
  private final ClusterOperationsProperties properties;
  private final PartitionReassignmentOperationJournal journal;
  private final PartitionReassignmentThrottleService throttleService;
  private final PartitionReassignmentClusterCoordinator clusterCoordinator;

  public Mono<PartitionReassignmentPlan> createPlan(
      KafkaCluster cluster,
      List<PartitionReassignmentTarget> targets) {
    var targetAssignments = toTargetAssignments(targets);
    return loadClusterState(cluster, targetAssignments.keySet())
        .map(state -> planService.createPlan(
            cluster.getName(),
            state.currentAssignments(),
            targetAssignments,
            state.availableBrokerIds()));
  }

  public Mono<Void> validatePlan(KafkaCluster cluster, PartitionReassignmentPlan plan) {
    if (!cluster.getName().equals(plan.clusterName())) {
      return Mono.error(new ValidationException(
          "Partition reassignment plan belongs to a different cluster"));
    }
    var partitions = plan.changes().stream()
        .map(PartitionAssignmentChange::topicPartition)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    return loadClusterState(cluster, partitions)
        .doOnNext(state -> planService.validateForExecution(
            plan, state.currentAssignments(), state.availableBrokerIds()))
        .then();
  }

  public PartitionReassignmentCapabilities getCapabilities(KafkaCluster cluster) {
    if (!properties.isPartitionReassignmentExecutionEnabled()) {
      return new PartitionReassignmentCapabilities(
          false,
          "Partition reassignment execution is disabled by server configuration.");
    }
    if (cluster.isReadOnly()) {
      return new PartitionReassignmentCapabilities(
          false,
          "This cluster is configured as read-only.");
    }
    return new PartitionReassignmentCapabilities(true, null);
  }

  public Mono<List<ActivePartitionReassignment>> listActiveReassignments(KafkaCluster cluster) {
    return adminClientService.get(cluster)
        .flatMap(admin -> admin.listPartitionReassignments()
            .flatMap(reassignments -> {
              if (reassignments.isEmpty()) {
                return Mono.just(List.of());
              }
              var topics = reassignments.keySet().stream()
                  .map(TopicPartition::topic)
                  .collect(java.util.stream.Collectors.toSet());
              return admin.describeTopics(topics)
                  .map(descriptions -> toActiveReassignments(reassignments, descriptions));
            }));
  }

  public Mono<Optional<PartitionReassignmentOperation>> getCurrentOperation(
      KafkaCluster cluster) {
    return Mono.fromCallable(() -> journal.list(cluster.getName()).stream()
            .filter(operation -> operation.kind() == PartitionReassignmentOperationKind.EXECUTE)
            .filter(operation -> !operation.status().isTerminal()
                || operation.status()
                    == PartitionReassignmentOperationStatus.CLEANUP_CONFLICT
                || operation.status() == PartitionReassignmentOperationStatus.FAILED)
            .reduce((first, second) -> second))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
  }

  public Mono<Integer> executePlan(KafkaCluster cluster, PartitionReassignmentPlan plan) {
    if (!getCapabilities(cluster).executionEnabled()) {
      return Mono.error(new PartitionReassignmentExecutionDisabledException());
    }
    var partitions = plan.changes().stream()
        .map(PartitionAssignmentChange::topicPartition)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    var reassignments = plan.changes().stream().collect(java.util.stream.Collectors.toMap(
        PartitionAssignmentChange::topicPartition,
        change -> java.util.Optional.of(
            new NewPartitionReassignment(change.targetReplicas())),
        (left, right) -> left,
        LinkedHashMap::new));

    return clusterCoordinator.withSubmissionLock(cluster.getName(),
        validatePlan(cluster, plan)
          .then(adminClientService.get(cluster))
          .flatMap(admin -> admin.listPartitionReassignments(partitions)
              .flatMap(active -> {
                var conflictingPartition = partitions.stream()
                    .filter(active::containsKey)
                    .findFirst();
                if (conflictingPartition.isPresent()) {
                  return Mono.error(new PartitionReassignmentConflictException(
                      "Partition reassignment is already in progress for "
                          + conflictingPartition.get()));
                }
                return admin.alterPartitionReassignments(reassignments)
                    .thenReturn(reassignments.size());
              }))
          .timeout(properties.getExecutionTimeout()));
  }

  public Mono<PartitionReassignmentExecution> executePlan(
      KafkaCluster cluster,
      PartitionReassignmentPlan plan,
      String operationId,
      Long throttleBytesPerSecond) {
    if (!getCapabilities(cluster).executionEnabled()) {
      return Mono.error(new PartitionReassignmentExecutionDisabledException());
    }
    validateOperationId(operationId);
    if (throttleBytesPerSecond != null && throttleBytesPerSecond <= 0) {
      return Mono.error(new ValidationException(
          "Reassignment throttle must be a positive number of bytes per second"));
    }
    var fingerprint = executionFingerprint(plan, throttleBytesPerSecond);
    return clusterCoordinator.withSubmissionLock(cluster.getName(), Mono.defer(() -> findOperation(
            cluster.getName(), operationId)
        .flatMap(existing -> replayExecution(existing, fingerprint))
        .switchIfEmpty(Mono.defer(() -> ensureNoOutstandingThrottle(cluster.getName())
            .then(submitOperation(
                cluster,
                plan,
                operationId,
                fingerprint,
                throttleBytesPerSecond))))));
  }

  public Mono<PartitionReassignmentCancellation> cancelReassignments(
      KafkaCluster cluster,
      String operationId,
      List<PartitionReassignmentPartition> requestedPartitions) {
    if (!getCapabilities(cluster).executionEnabled()) {
      return Mono.error(new PartitionReassignmentExecutionDisabledException());
    }
    validateOperationId(operationId);
    var partitions = validateCancellationPartitions(requestedPartitions);
    var fingerprint = cancellationFingerprint(partitions);
    return clusterCoordinator.withSubmissionLock(cluster.getName(), Mono.defer(() -> findOperation(
            cluster.getName(), operationId)
        .flatMap(existing -> replayCancellation(cluster, existing, fingerprint))
        .switchIfEmpty(Mono.defer(() -> submitCancellation(
            cluster, operationId, fingerprint, partitions)))));
  }

  private Mono<PartitionReassignmentExecution> submitOperation(
      KafkaCluster cluster,
      PartitionReassignmentPlan plan,
      String operationId,
      String fingerprint,
      Long throttleBytesPerSecond) {
    var partitions = plan.changes().stream()
        .map(PartitionAssignmentChange::topicPartition)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    var reassignments = plan.changes().stream().collect(java.util.stream.Collectors.toMap(
        PartitionAssignmentChange::topicPartition,
        change -> Optional.of(new NewPartitionReassignment(change.targetReplicas())),
        (left, right) -> left,
        LinkedHashMap::new));
    var operation = new PartitionReassignmentOperation(
        operationId,
        cluster.getName(),
        PartitionReassignmentOperationKind.EXECUTE,
        fingerprint,
        plan.changes(),
        List.of(),
        throttleBytesPerSecond,
        PartitionReassignmentOperationStatus.PREPARED,
        List.of(),
        0,
        0,
        0,
        List.of());

    return validatePlan(cluster, plan)
        .then(adminClientService.get(cluster))
        .flatMap(admin -> admin.listPartitionReassignments(partitions)
            .flatMap(active -> {
              var conflict = partitions.stream().filter(active::containsKey).findFirst();
              if (conflict.isPresent()) {
                return Mono.error(new PartitionReassignmentConflictException(
                    "Partition reassignment is already in progress for " + conflict.get()));
              }
              return persistNew(operation)
                  .flatMap(created -> throttleService.apply(cluster, created)
                      .onErrorResume(error -> rollbackFailedOperation(cluster, created, error)))
                  .flatMap(prepared -> prepared.status()
                      == PartitionReassignmentOperationStatus.THROTTLE_APPLIED
                          ? Mono.just(prepared)
                          : persist(prepared.withStatus(
                              PartitionReassignmentOperationStatus.THROTTLE_APPLIED)))
                  .flatMap(prepared -> admin.alterPartitionReassignments(reassignments)
                      .then(persist(prepared.withExecutionResult(
                          PartitionReassignmentOperationStatus.REASSIGNMENT_SUBMITTED,
                          reassignments.size()))));
            }))
        .map(PartitionReassignmentService::toExecution)
        .timeout(properties.getExecutionTimeout());
  }

  private Mono<PartitionReassignmentCancellation> submitCancellation(
      KafkaCluster cluster,
      String operationId,
      String fingerprint,
      List<PartitionReassignmentPartition> partitions) {
    var operation = new PartitionReassignmentOperation(
        operationId,
        cluster.getName(),
        PartitionReassignmentOperationKind.CANCEL,
        fingerprint,
        List.of(),
        partitions,
        null,
        PartitionReassignmentOperationStatus.PREPARED,
        List.of(),
        0,
        0,
        0,
        List.of());
    return persistNew(operation)
        .flatMap(created -> resumeCancellationOperation(cluster, created))
        .map(PartitionReassignmentService::toCancellation)
        .timeout(properties.getExecutionTimeout());
  }

  Mono<PartitionReassignmentOperation> resumeCancellationOperation(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation) {
    var topicPartitions = operation.partitions().stream()
        .map(PartitionReassignmentPartition::topicPartition)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    return adminClientService.get(cluster)
        .flatMap(admin -> admin.listPartitionReassignments(topicPartitions)
            .flatMap(active -> {
              var cancellations = new LinkedHashMap<TopicPartition,
                  Optional<NewPartitionReassignment>>();
              topicPartitions.stream()
                  .filter(active::containsKey)
                  .forEach(partition -> cancellations.put(partition, Optional.empty()));
              var completed = operation
                  .withCancellationResult(
                      cancellations.size(),
                      topicPartitions.size() - cancellations.size())
                  .withStatus(PartitionReassignmentOperationStatus.CANCELLED);
              return cancellations.isEmpty()
                  ? persist(completed)
                  : admin.alterPartitionReassignments(cancellations)
                      .then(persist(completed));
            }));
  }

  private Mono<PartitionReassignmentOperation> rollbackFailedOperation(
      KafkaCluster cluster,
      PartitionReassignmentOperation fallback,
      Throwable error) {
    return findOperation(cluster.getName(), fallback.operationId())
        .defaultIfEmpty(fallback)
        .flatMap(latest -> throttleService.cleanup(
            cluster, latest, PartitionReassignmentOperationStatus.FAILED))
        .onErrorResume(cleanupError -> Mono.empty())
        .then(Mono.error(error));
  }

  private Mono<PartitionReassignmentExecution> replayExecution(
      PartitionReassignmentOperation existing,
      String fingerprint) {
    assertReplayMatches(existing, PartitionReassignmentOperationKind.EXECUTE, fingerprint);
    return Mono.just(toExecution(existing));
  }

  private Mono<PartitionReassignmentCancellation> replayCancellation(
      KafkaCluster cluster,
      PartitionReassignmentOperation existing,
      String fingerprint) {
    assertReplayMatches(existing, PartitionReassignmentOperationKind.CANCEL, fingerprint);
    if (existing.status() == PartitionReassignmentOperationStatus.CANCELLED) {
      return Mono.just(toCancellation(existing));
    }
    if (existing.status() == PartitionReassignmentOperationStatus.PREPARED) {
      return resumeCancellationOperation(cluster, existing)
          .map(PartitionReassignmentService::toCancellation);
    }
    return Mono.error(new PartitionReassignmentConflictException(
        "Partition reassignment cancellation cannot be resumed from status "
            + existing.status()));
  }

  private static void assertReplayMatches(
      PartitionReassignmentOperation existing,
      PartitionReassignmentOperationKind kind,
      String fingerprint) {
    if (existing.kind() != kind || !existing.requestFingerprint().equals(fingerprint)) {
      throw new PartitionReassignmentConflictException(
          "Partition reassignment operation ID is already used: "
              + existing.operationId());
    }
  }

  private Mono<PartitionReassignmentOperation> findOperation(
      String clusterName,
      String operationId) {
    return Mono.fromCallable(() -> journal.find(clusterName, operationId))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
        .flatMap(Mono::justOrEmpty);
  }

  private Mono<Void> ensureNoOutstandingThrottle(String clusterName) {
    return Mono.fromCallable(() -> journal.list(clusterName).stream()
            .filter(operation -> operation.kind() == PartitionReassignmentOperationKind.EXECUTE)
            .filter(operation -> operation.throttleBytesPerSecond() != null)
            .filter(operation -> !operation.status().isTerminal()
                || operation.status()
                    == PartitionReassignmentOperationStatus.CLEANUP_CONFLICT)
            .findFirst())
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
        .flatMap(Mono::justOrEmpty)
        .flatMap(existing -> Mono.error(new PartitionReassignmentConflictException(
            "Throttled partition reassignment operation "
                + existing.operationId()
                + " must finish cleanup before another reassignment can be submitted")))
        .then();
  }

  private Mono<PartitionReassignmentOperation> persistNew(
      PartitionReassignmentOperation operation) {
    return Mono.fromCallable(() -> journal.create(operation))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
  }

  private Mono<PartitionReassignmentOperation> persist(
      PartitionReassignmentOperation operation) {
    return Mono.fromCallable(() -> journal.update(operation))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
  }

  private List<PartitionReassignmentPartition> validateCancellationPartitions(
      List<PartitionReassignmentPartition> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      throw new ValidationException("At least one partition must be selected for cancellation");
    }
    if (partitions.size() > properties.getMaxPartitions()) {
      throw new ValidationException("Partition cancellation request exceeds the %d partition limit"
          .formatted(properties.getMaxPartitions()));
    }
    var unique = new LinkedHashSet<PartitionReassignmentPartition>();
    for (var partition : partitions) {
      if (partition == null || partition.topic().isBlank() || partition.partition() < 0) {
        throw new ValidationException("Cancellation partitions must contain a topic and partition");
      }
      if (!unique.add(partition)) {
        throw new ValidationException("Partition cancellation request contains duplicate partition "
            + partition.topicPartition());
      }
    }
    return List.copyOf(unique);
  }

  private static void validateOperationId(String operationId) {
    if (operationId == null
        || !operationId.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")) {
      throw new ValidationException(
          "Operation ID must be 1-128 URL-safe characters");
    }
  }

  static String executionFingerprint(
      PartitionReassignmentPlan plan,
      Long throttleBytesPerSecond) {
    var changes = plan.changes().stream()
        .sorted(Comparator
            .comparing(PartitionAssignmentChange::topic)
            .thenComparingInt(PartitionAssignmentChange::partition))
        .map(change -> String.join(":",
            change.topic(),
            Integer.toString(change.partition()),
            change.currentReplicas().toString(),
            change.targetReplicas().toString()))
        .collect(java.util.stream.Collectors.joining("\n"));
    return "execute:" + sha256(String.join("\n",
        plan.clusterName(),
        plan.planHash(),
        Objects.toString(throttleBytesPerSecond, "none"),
        changes));
  }

  private static String sha256(String value) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
          .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is unavailable", exception);
    }
  }

  private static String cancellationFingerprint(
      List<PartitionReassignmentPartition> partitions) {
    return "cancel:" + partitions.stream()
        .sorted(java.util.Comparator
            .comparing(PartitionReassignmentPartition::topic)
            .thenComparingInt(PartitionReassignmentPartition::partition))
        .map(partition -> partition.topic() + ":" + partition.partition())
        .collect(java.util.stream.Collectors.joining(","));
  }

  private static PartitionReassignmentExecution toExecution(
      PartitionReassignmentOperation operation) {
    return new PartitionReassignmentExecution(
        operation.operationId(),
        operation.acceptedPartitions(),
        operation.status());
  }

  private static PartitionReassignmentCancellation toCancellation(
      PartitionReassignmentOperation operation) {
    return new PartitionReassignmentCancellation(
        operation.operationId(),
        operation.cancelledPartitions(),
        operation.skippedPartitions());
  }

  private List<ActivePartitionReassignment> toActiveReassignments(
      Map<TopicPartition, org.apache.kafka.clients.admin.PartitionReassignment> reassignments,
      Map<String, TopicDescription> descriptions) {
    return reassignments.entrySet().stream()
        .sorted(Map.Entry.comparingByKey(java.util.Comparator
            .comparing(TopicPartition::topic)
            .thenComparingInt(TopicPartition::partition)))
        .map(entry -> {
          var topicPartition = entry.getKey();
          var reassignment = entry.getValue();
          var partition = java.util.Optional.ofNullable(descriptions.get(topicPartition.topic()))
              .stream()
              .flatMap(description -> description.partitions().stream())
              .filter(candidate -> candidate.partition() == topicPartition.partition())
              .findFirst()
              .orElseThrow(() -> new ValidationException(
                  "Active assignment not found for " + topicPartition));
          var adding = Set.copyOf(reassignment.addingReplicas());
          var removing = Set.copyOf(reassignment.removingReplicas());
          var currentReplicas = reassignment.replicas().stream()
              .filter(brokerId -> !adding.contains(brokerId))
              .toList();
          var targetReplicas = reassignment.replicas().stream()
              .filter(brokerId -> !removing.contains(brokerId))
              .toList();
          var inSyncBrokerIds = partition.isr().stream()
              .map(Node::id)
              .collect(java.util.stream.Collectors.toSet());
          var inSyncTargets = targetReplicas.stream().filter(inSyncBrokerIds::contains).count();
          var progressPercent = targetReplicas.isEmpty()
              ? 0
              : (int) Math.round(inSyncTargets * 100.0 / targetReplicas.size());
          return new ActivePartitionReassignment(
              topicPartition.topic(),
              topicPartition.partition(),
              currentReplicas,
              targetReplicas,
              reassignment.addingReplicas(),
              reassignment.removingReplicas(),
              progressPercent);
        })
        .toList();
  }

  private Map<TopicPartition, List<Integer>> toTargetAssignments(
      List<PartitionReassignmentTarget> targets) {
    if (targets == null) {
      throw new ValidationException("Target assignments are required");
    }
    if (targets.isEmpty()) {
      throw new ValidationException("Target assignments must not be empty");
    }
    var assignments = new LinkedHashMap<TopicPartition, List<Integer>>();
    for (var target : targets) {
      if (target == null) {
        throw new ValidationException("Target assignments must not contain null entries");
      }
      if (target.topic() == null || target.topic().isBlank()) {
        throw new ValidationException("Assignment topic name must not be blank");
      }
      if (target.partition() < 0) {
        throw new ValidationException("Assignment partition must not be negative");
      }
      var topicPartition = new TopicPartition(target.topic(), target.partition());
      if (assignments.put(topicPartition, target.replicas()) != null) {
        throw new ValidationException(
            "Partition reassignment request contains duplicate partition " + topicPartition);
      }
    }
    return assignments;
  }

  private Mono<ClusterState> loadClusterState(
      KafkaCluster cluster,
      Collection<TopicPartition> requestedPartitions) {
    var topics = requestedPartitions.stream()
        .map(TopicPartition::topic)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    return adminClientService.get(cluster)
        .flatMap(admin -> Mono.zip(
            admin.describeTopics(topics),
            admin.describeCluster()))
        .map(tuple -> new ClusterState(
            currentAssignments(requestedPartitions, tuple.getT1()),
            tuple.getT2().getNodes().stream().map(Node::id).collect(java.util.stream.Collectors.toSet())));
  }

  private Map<TopicPartition, List<Integer>> currentAssignments(
      Collection<TopicPartition> requestedPartitions,
      Map<String, TopicDescription> topicDescriptions) {
    var assignments = new LinkedHashMap<TopicPartition, List<Integer>>();
    for (var topicPartition : requestedPartitions) {
      var replicas = java.util.Optional.ofNullable(topicDescriptions.get(topicPartition.topic()))
          .stream()
          .flatMap(description -> description.partitions().stream())
          .filter(partition -> partition.partition() == topicPartition.partition())
          .findFirst()
          .map(partition -> partition.replicas().stream().map(Node::id).toList())
          .orElseThrow(() -> new ValidationException(
              "Current assignment not found for " + topicPartition));
      assignments.put(topicPartition, replicas);
    }
    return assignments;
  }

  private record ClusterState(
      Map<TopicPartition, List<Integer>> currentAssignments,
      Set<Integer> availableBrokerIds) {
  }
}
