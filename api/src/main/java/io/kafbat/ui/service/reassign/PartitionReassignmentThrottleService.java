package io.kafbat.ui.service.reassign;

import io.kafbat.ui.exception.ValidationException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
@RequiredArgsConstructor
public class PartitionReassignmentThrottleService {

  public static final String LEADER_THROTTLED_RATE =
      "leader.replication.throttled.rate";
  public static final String FOLLOWER_THROTTLED_RATE =
      "follower.replication.throttled.rate";
  public static final String LEADER_THROTTLED_REPLICAS =
      "leader.replication.throttled.replicas";
  public static final String FOLLOWER_THROTTLED_REPLICAS =
      "follower.replication.throttled.replicas";

  private static final Comparator<PartitionAssignmentChange> CHANGE_ORDER = Comparator
      .comparing(PartitionAssignmentChange::topic)
      .thenComparingInt(PartitionAssignmentChange::partition);

  private final AdminClientService adminClientService;
  private final PartitionReassignmentOperationJournal journal;

  public Mono<PartitionReassignmentOperation> apply(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation) {
    if (operation.throttleBytesPerSecond() == null) {
      return Mono.just(operation);
    }
    if (operation.throttleBytesPerSecond() <= 0) {
      return Mono.error(new ValidationException(
          "Reassignment throttle must be a positive number of bytes per second"));
    }
    var resources = resources(operation.changes());
    return adminClientService.get(cluster)
        .flatMap(admin -> admin.describeConfigs(resources)
            .map(configs -> operation.withConfigSnapshots(snapshots(operation, configs)))
            .flatMap(this::persist)
            .flatMap(prepared -> admin.incrementalAlterConfigs(alterations(
                    prepared.configSnapshots(), false))
                .thenReturn(prepared))
            .map(prepared -> prepared.withConfigSnapshots(prepared.configSnapshots().stream()
                .map(snapshot -> snapshot.withState(PartitionReassignmentConfigState.APPLIED))
                .toList()))
            .map(prepared -> prepared.withStatus(
                PartitionReassignmentOperationStatus.THROTTLE_APPLIED))
            .flatMap(this::persist));
  }

  public Mono<PartitionReassignmentOperation> cleanup(
      KafkaCluster cluster,
      PartitionReassignmentOperation operation,
      PartitionReassignmentOperationStatus successfulStatus) {
    if (operation.configSnapshots().isEmpty()) {
      return persist(operation.withStatus(successfulStatus));
    }
    var resources = operation.configSnapshots().stream()
        .map(PartitionReassignmentThrottleService::resource)
        .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
    return adminClientService.get(cluster)
        .flatMap(admin -> admin.describeConfigs(resources)
            .map(configs -> cleanupPlan(operation, successfulStatus, configs))
            .flatMap(plan -> (plan.alterations().isEmpty()
                ? Mono.<Void>empty()
                : admin.incrementalAlterConfigs(plan.alterations()))
                .thenReturn(plan.operation()))
            .flatMap(this::persist));
  }

  private CleanupPlan cleanupPlan(
      PartitionReassignmentOperation operation,
      PartitionReassignmentOperationStatus successfulStatus,
      Map<ConfigResource, Config> configs) {
    var restored = new ArrayList<PartitionReassignmentConfigSnapshot>();
    var conflicts = new ArrayList<String>();
    var alterations = new LinkedHashMap<ConfigResource, List<AlterConfigOp>>();
    for (var snapshot : operation.configSnapshots()) {
      var resource = resource(snapshot);
      var currentValue = dynamicValue(
          configs.get(resource), resource.type(), snapshot.configName());
      if (Objects.equals(currentValue, snapshot.previousValue())) {
        restored.add(snapshot.withState(PartitionReassignmentConfigState.RESTORED));
      } else if (Objects.equals(currentValue, snapshot.appliedValue())) {
        restored.add(snapshot.withState(PartitionReassignmentConfigState.RESTORED));
        alterations.computeIfAbsent(resource, ignored -> new ArrayList<>())
            .add(restoreOperation(snapshot));
      } else {
        restored.add(snapshot.withState(PartitionReassignmentConfigState.CONFLICT));
        conflicts.add(snapshot.resourceType() + " " + snapshot.resourceName()
            + " " + snapshot.configName());
      }
    }
    var status = conflicts.isEmpty()
        ? successfulStatus
        : PartitionReassignmentOperationStatus.CLEANUP_CONFLICT;
    return new CleanupPlan(
        operation.withCleanupResult(status, restored, conflicts),
        alterations);
  }

  private List<PartitionReassignmentConfigSnapshot> snapshots(
      PartitionReassignmentOperation operation,
      Map<ConfigResource, Config> configs) {
    var snapshots = new ArrayList<PartitionReassignmentConfigSnapshot>();
    var rate = Long.toString(operation.throttleBytesPerSecond());
    var brokerIds = operation.changes().stream()
        .flatMap(change -> java.util.stream.Stream.concat(
            change.currentReplicas().stream(), change.targetReplicas().stream()))
        .collect(java.util.stream.Collectors.toCollection(TreeSet::new));
    for (var brokerId : brokerIds) {
      var resource = new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString());
      snapshots.add(snapshot(configs, resource, LEADER_THROTTLED_RATE, rate));
      snapshots.add(snapshot(configs, resource, FOLLOWER_THROTTLED_RATE, rate));
    }
    operation.changes().stream()
        .sorted(CHANGE_ORDER)
        .collect(java.util.stream.Collectors.groupingBy(
            PartitionAssignmentChange::topic,
            LinkedHashMap::new,
            java.util.stream.Collectors.toList()))
        .forEach((topic, changes) -> {
          var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
          snapshots.add(snapshot(
              configs,
              resource,
              LEADER_THROTTLED_REPLICAS,
              mergedReplicaList(configs.get(resource), LEADER_THROTTLED_REPLICAS,
                  removedReplicas(changes))));
          snapshots.add(snapshot(
              configs,
              resource,
              FOLLOWER_THROTTLED_REPLICAS,
              mergedReplicaList(configs.get(resource), FOLLOWER_THROTTLED_REPLICAS,
                  addedReplicas(changes))));
        });
    return List.copyOf(snapshots);
  }

  private static PartitionReassignmentConfigSnapshot snapshot(
      Map<ConfigResource, Config> configs,
      ConfigResource resource,
      String configName,
      String appliedValue) {
    return new PartitionReassignmentConfigSnapshot(
        resource.type().name(),
        resource.name(),
        configName,
        dynamicValue(configs.get(resource), resource.type(), configName),
        appliedValue,
        PartitionReassignmentConfigState.PENDING);
  }

  private static String mergedReplicaList(
      Config config,
      String configName,
      Collection<String> additions) {
    var values = new TreeSet<String>();
    var existing = dynamicValue(config, ConfigResource.Type.TOPIC, configName);
    if (existing != null && !existing.isBlank()) {
      java.util.Arrays.stream(existing.split(","))
          .map(String::trim)
          .filter(value -> !value.isEmpty())
          .forEach(values::add);
    }
    values.addAll(additions);
    return String.join(",", values);
  }

  private static Set<String> removedReplicas(List<PartitionAssignmentChange> changes) {
    var values = new TreeSet<String>();
    for (var change : changes) {
      change.currentReplicas().stream()
          .filter(broker -> !change.targetReplicas().contains(broker))
          .map(broker -> change.partition() + ":" + broker)
          .forEach(values::add);
    }
    return values;
  }

  private static Set<String> addedReplicas(List<PartitionAssignmentChange> changes) {
    var values = new TreeSet<String>();
    for (var change : changes) {
      change.targetReplicas().stream()
          .filter(broker -> !change.currentReplicas().contains(broker))
          .map(broker -> change.partition() + ":" + broker)
          .forEach(values::add);
    }
    return values;
  }

  private static Set<ConfigResource> resources(List<PartitionAssignmentChange> changes) {
    var resources = new LinkedHashSet<ConfigResource>();
    changes.stream()
        .flatMap(change -> java.util.stream.Stream.concat(
            change.currentReplicas().stream(), change.targetReplicas().stream()))
        .distinct()
        .sorted()
        .map(id -> new ConfigResource(ConfigResource.Type.BROKER, id.toString()))
        .forEach(resources::add);
    changes.stream()
        .map(PartitionAssignmentChange::topic)
        .distinct()
        .sorted()
        .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
        .forEach(resources::add);
    return resources;
  }

  private static Map<ConfigResource, List<AlterConfigOp>> alterations(
      List<PartitionReassignmentConfigSnapshot> snapshots,
      boolean restore) {
    var result = new LinkedHashMap<ConfigResource, List<AlterConfigOp>>();
    for (var snapshot : snapshots) {
      result.computeIfAbsent(resource(snapshot), ignored -> new ArrayList<>())
          .add(restore ? restoreOperation(snapshot) : setOperation(snapshot));
    }
    return result;
  }

  private static AlterConfigOp setOperation(PartitionReassignmentConfigSnapshot snapshot) {
    return new AlterConfigOp(
        new ConfigEntry(snapshot.configName(), snapshot.appliedValue()),
        AlterConfigOp.OpType.SET);
  }

  private static AlterConfigOp restoreOperation(PartitionReassignmentConfigSnapshot snapshot) {
    var type = snapshot.previousValue() == null
        ? AlterConfigOp.OpType.DELETE
        : AlterConfigOp.OpType.SET;
    return new AlterConfigOp(
        new ConfigEntry(snapshot.configName(), snapshot.previousValue()),
        type);
  }

  private static String dynamicValue(
      Config config,
      ConfigResource.Type resourceType,
      String configName) {
    if (config == null) {
      return null;
    }
    var entry = config.get(configName);
    if (entry == null) {
      return null;
    }
    var expectedSource = resourceType == ConfigResource.Type.BROKER
        ? ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG
        : ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG;
    return entry.source() == expectedSource ? entry.value() : null;
  }

  private static ConfigResource resource(PartitionReassignmentConfigSnapshot snapshot) {
    return new ConfigResource(
        ConfigResource.Type.valueOf(snapshot.resourceType()),
        snapshot.resourceName());
  }

  private Mono<PartitionReassignmentOperation> persist(
      PartitionReassignmentOperation operation) {
    return Mono.fromCallable(() -> journal.update(operation))
        .subscribeOn(Schedulers.boundedElastic());
  }

  private record CleanupPlan(
      PartitionReassignmentOperation operation,
      Map<ConfigResource, List<AlterConfigOp>> alterations) {
  }
}
