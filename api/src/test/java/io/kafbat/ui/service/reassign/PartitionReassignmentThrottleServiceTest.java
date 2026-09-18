package io.kafbat.ui.service.reassign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.AdminClientService;
import io.kafbat.ui.service.ReactiveAdminClient;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

class PartitionReassignmentThrottleServiceTest {

  private final KafkaCluster cluster = KafkaCluster.builder().name("dev").build();
  private final AdminClientService adminClientService = mock(AdminClientService.class);
  private final ReactiveAdminClient admin = mock(ReactiveAdminClient.class);
  private final PartitionReassignmentOperationJournal journal =
      mock(PartitionReassignmentOperationJournal.class);
  private PartitionReassignmentThrottleService service;

  @BeforeEach
  void setUp() {
    when(adminClientService.get(cluster)).thenReturn(Mono.just(admin));
    when(journal.update(any())).thenAnswer(invocation -> invocation.getArgument(0));
    service = new PartitionReassignmentThrottleService(adminClientService, journal);
  }

  @Test
  void appliesBrokerRatesAndMergesTopicReplicaLists() {
    var broker1 = broker("1");
    var broker2 = broker("2");
    var broker3 = broker("3");
    var topic = topic("orders");
    when(admin.describeConfigs(Set.of(broker1, broker2, broker3, topic)))
        .thenReturn(Mono.just(Map.of(
            broker1, config(),
            broker2, config(),
            broker3, config(),
            topic, config(dynamicTopicEntry(
                PartitionReassignmentThrottleService.LEADER_THROTTLED_REPLICAS,
                "9:4")))));
    when(admin.incrementalAlterConfigs(any())).thenReturn(Mono.empty());

    var result = service.apply(cluster, operation()).block();

    assertThat(result).isNotNull();
    assertThat(result.status()).isEqualTo(PartitionReassignmentOperationStatus.THROTTLE_APPLIED);
    assertThat(result.configSnapshots()).hasSize(8);
    var captor = alterationsCaptor();
    verify(admin).incrementalAlterConfigs(captor.capture());
    assertThat(value(captor.getValue(), topic,
        PartitionReassignmentThrottleService.LEADER_THROTTLED_REPLICAS))
        .isEqualTo("0:1,9:4");
    assertThat(value(captor.getValue(), topic,
        PartitionReassignmentThrottleService.FOLLOWER_THROTTLED_REPLICAS))
        .isEqualTo("0:3");
    assertThat(value(captor.getValue(), broker1,
        PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE))
        .isEqualTo("1000000");
  }

  @Test
  void restoresThePreviousDynamicValuesAfterCompletion() {
    var applied = operation().withConfigSnapshots(List.of(
        new PartitionReassignmentConfigSnapshot(
            "BROKER",
            "1",
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            "500000",
            "1000000",
            PartitionReassignmentConfigState.APPLIED)));
    when(admin.describeConfigs(Set.of(broker("1"))))
        .thenReturn(Mono.just(Map.of(broker("1"), config(dynamicBrokerEntry(
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            "1000000")))));
    when(admin.incrementalAlterConfigs(any())).thenReturn(Mono.empty());

    var result = service.cleanup(
        cluster,
        applied,
        PartitionReassignmentOperationStatus.COMPLETED).block();

    assertThat(result).isNotNull();
    assertThat(result.status()).isEqualTo(PartitionReassignmentOperationStatus.COMPLETED);
    var captor = alterationsCaptor();
    verify(admin).incrementalAlterConfigs(captor.capture());
    assertThat(value(captor.getValue(), broker("1"),
        PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE))
        .isEqualTo("500000");
  }

  @Test
  void deletesAThrottleConfigThatDidNotExistBeforeTheOperation() {
    var applied = operation().withConfigSnapshots(List.of(
        new PartitionReassignmentConfigSnapshot(
            "BROKER",
            "1",
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            null,
            "1000000",
            PartitionReassignmentConfigState.APPLIED)));
    when(admin.describeConfigs(Set.of(broker("1"))))
        .thenReturn(Mono.just(Map.of(broker("1"), config(dynamicBrokerEntry(
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            "1000000")))));
    when(admin.incrementalAlterConfigs(any())).thenReturn(Mono.empty());

    var result = service.cleanup(
        cluster,
        applied,
        PartitionReassignmentOperationStatus.COMPLETED).block();

    assertThat(result).isNotNull();
    var captor = alterationsCaptor();
    verify(admin).incrementalAlterConfigs(captor.capture());
    assertThat(alteration(captor.getValue(), broker("1"),
        PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE).opType())
        .isEqualTo(AlterConfigOp.OpType.DELETE);
  }

  @Test
  void preservesAnExternalChangeAndReportsCleanupConflict() {
    var applied = operation().withConfigSnapshots(List.of(
        new PartitionReassignmentConfigSnapshot(
            "BROKER",
            "1",
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            null,
            "1000000",
            PartitionReassignmentConfigState.APPLIED)));
    when(admin.describeConfigs(Set.of(broker("1"))))
        .thenReturn(Mono.just(Map.of(broker("1"), config(dynamicBrokerEntry(
            PartitionReassignmentThrottleService.LEADER_THROTTLED_RATE,
            "750000")))));

    var result = service.cleanup(
        cluster,
        applied,
        PartitionReassignmentOperationStatus.COMPLETED).block();

    assertThat(result).isNotNull();
    assertThat(result.status())
        .isEqualTo(PartitionReassignmentOperationStatus.CLEANUP_CONFLICT);
    assertThat(result.cleanupConflicts())
        .containsExactly("BROKER 1 leader.replication.throttled.rate");
    verify(admin, never()).incrementalAlterConfigs(any());
  }

  private static PartitionReassignmentOperation operation() {
    return new PartitionReassignmentOperation(
        "operation-1",
        "dev",
        PartitionReassignmentOperationKind.EXECUTE,
        "fingerprint",
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

  private static ConfigResource broker(String id) {
    return new ConfigResource(ConfigResource.Type.BROKER, id);
  }

  private static ConfigResource topic(String name) {
    return new ConfigResource(ConfigResource.Type.TOPIC, name);
  }

  private static Config config(ConfigEntry... entries) {
    return new Config(List.of(entries));
  }

  private static ConfigEntry dynamicBrokerEntry(String name, String value) {
    return new ConfigEntry(
        name,
        value,
        ConfigEntry.ConfigSource.DYNAMIC_BROKER_CONFIG,
        false,
        false,
        List.of(),
        ConfigEntry.ConfigType.STRING,
        null);
  }

  private static ConfigEntry dynamicTopicEntry(String name, String value) {
    return new ConfigEntry(
        name,
        value,
        ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG,
        false,
        false,
        List.of(),
        ConfigEntry.ConfigType.STRING,
        null);
  }

  private static String value(
      Map<ConfigResource, List<AlterConfigOp>> alterations,
      ConfigResource resource,
      String name) {
    return alteration(alterations, resource, name).configEntry().value();
  }

  private static AlterConfigOp alteration(
      Map<ConfigResource, List<AlterConfigOp>> alterations,
      ConfigResource resource,
      String name) {
    return alterations.get(resource).stream()
        .filter(operation -> operation.configEntry().name().equals(name))
        .findFirst()
        .orElseThrow();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ArgumentCaptor<Map<ConfigResource, List<AlterConfigOp>>>
      alterationsCaptor() {
    return ArgumentCaptor.forClass((Class) Map.class);
  }
}
