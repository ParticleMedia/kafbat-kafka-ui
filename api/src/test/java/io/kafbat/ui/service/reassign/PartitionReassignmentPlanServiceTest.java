package io.kafbat.ui.service.reassign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.kafbat.ui.config.ClusterOperationsProperties;
import io.kafbat.ui.exception.PartitionReassignmentConflictException;
import io.kafbat.ui.exception.ValidationException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PartitionReassignmentPlanServiceTest {

  private PartitionReassignmentPlanService service;

  @BeforeEach
  void setUp() {
    service = new PartitionReassignmentPlanService(new ClusterOperationsProperties());
  }

  @Test
  void createsSortedImmutablePlanWithCurrentAndTargetAssignments() {
    var topic0 = new TopicPartition("orders", 0);
    var topic1 = new TopicPartition("orders", 1);
    var current = new LinkedHashMap<TopicPartition, List<Integer>>();
    current.put(topic1, List.of(2, 1));
    current.put(topic0, List.of(1, 2));
    var target = new LinkedHashMap<TopicPartition, List<Integer>>();
    target.put(topic1, List.of(1, 2));
    target.put(topic0, List.of(2, 1));

    var plan = service.createPlan("local", current, target, Set.of(1, 2));

    assertThat(plan.changes()).containsExactly(
        new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(2, 1)),
        new PartitionAssignmentChange("orders", 1, List.of(2, 1), List.of(1, 2)));
    assertThat(plan.planHash()).matches("[0-9a-f]{64}");
    assertThatThrownBy(() -> plan.changes().add(plan.changes().getFirst()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createsSameHashRegardlessOfInputMapOrder() {
    var topic0 = new TopicPartition("orders", 0);
    var topic1 = new TopicPartition("payments", 0);
    var firstCurrent = new LinkedHashMap<TopicPartition, List<Integer>>();
    firstCurrent.put(topic0, List.of(1, 2));
    firstCurrent.put(topic1, List.of(2, 3));
    var firstTarget = new LinkedHashMap<TopicPartition, List<Integer>>();
    firstTarget.put(topic0, List.of(2, 1));
    firstTarget.put(topic1, List.of(3, 2));
    var secondCurrent = new LinkedHashMap<TopicPartition, List<Integer>>();
    secondCurrent.put(topic1, List.of(2, 3));
    secondCurrent.put(topic0, List.of(1, 2));
    var secondTarget = new LinkedHashMap<TopicPartition, List<Integer>>();
    secondTarget.put(topic1, List.of(3, 2));
    secondTarget.put(topic0, List.of(2, 1));

    var first = service.createPlan("local", firstCurrent, firstTarget, Set.of(1, 2, 3));
    var second = service.createPlan("local", secondCurrent, secondTarget, Set.of(1, 2, 3));
    var otherCluster = service.createPlan("remote", secondCurrent, secondTarget, Set.of(1, 2, 3));

    assertThat(first.planHash())
        .isEqualTo("e8a200d3bf49259236d2d6adc89eebb50da04114f3553f3252700fe5c7d946a9")
        .isEqualTo(second.planHash());
    assertThat(first.planHash()).isNotEqualTo(otherCluster.planHash());
    assertThat(first.changes()).isEqualTo(second.changes());
  }

  @Test
  void validatesPlanHashAndCurrentAssignmentsBeforeExecution() {
    var topicPartition = new TopicPartition("orders", 0);
    var plan = service.createPlan(
        "local",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(2, 3)),
        Set.of(1, 2, 3));

    service.validateForExecution(
        plan,
        Map.of(topicPartition, List.of(1, 2)),
        Set.of(1, 2, 3));

    var tamperedPlan = new PartitionReassignmentPlan(
        plan.clusterName(),
        List.of(new PartitionAssignmentChange("orders", 0, List.of(1, 2), List.of(3, 2))),
        plan.planHash());
    assertThatThrownBy(() -> service.validateForExecution(
        tamperedPlan,
        Map.of(topicPartition, List.of(1, 2)),
        Set.of(1, 2, 3)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Partition reassignment plan hash is invalid");
  }

  @Test
  void rejectsPlanWhenCurrentAssignmentsHaveChanged() {
    var topicPartition = new TopicPartition("orders", 0);
    var plan = service.createPlan(
        "local",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(2, 3)),
        Set.of(1, 2, 3));

    assertThatThrownBy(() -> service.validateForExecution(
        plan,
        Map.of(topicPartition, List.of(2, 1)),
        Set.of(1, 2, 3)))
        .isInstanceOf(PartitionReassignmentConflictException.class)
        .hasMessage("Partition reassignment plan is stale for orders-0");
  }

  @Test
  void rejectsUnknownOrDuplicateTargetBrokers() {
    var topicPartition = new TopicPartition("orders", 0);
    var current = Map.of(topicPartition, List.of(1, 2));

    assertThatThrownBy(() -> service.createPlan(
        "local", current, Map.of(topicPartition, List.of(2, 4)), Set.of(1, 2, 3)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Target assignment for orders-0 contains unavailable broker 4");

    assertThatThrownBy(() -> service.createPlan(
        "local", current, Map.of(topicPartition, List.of(2, 2)), Set.of(1, 2, 3)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Target assignment for orders-0 contains duplicate brokers");
  }

  @Test
  void enforcesConfiguredTopicAndPartitionLimits() {
    var properties = new ClusterOperationsProperties();
    properties.setMaxPartitions(1);
    service = new PartitionReassignmentPlanService(properties);
    var orders0 = new TopicPartition("orders", 0);
    var payments0 = new TopicPartition("payments", 0);
    var current = new HashMap<TopicPartition, List<Integer>>();
    current.put(orders0, List.of(1));
    current.put(payments0, List.of(1));
    var target = new HashMap<TopicPartition, List<Integer>>();
    target.put(orders0, List.of(2));
    target.put(payments0, List.of(2));

    assertThatThrownBy(() -> service.createPlan("local", current, target, Set.of(1, 2)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Partition reassignment request exceeds the 1 partition limit");

    properties.setMaxPartitions(2);
    properties.setMaxTopics(1);
    assertThatThrownBy(() -> service.createPlan("local", current, target, Set.of(1, 2)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Partition reassignment request exceeds the 1 topic limit");

    properties.setMaxReplicasPerPartition(1);
    assertThatThrownBy(() -> service.createPlan(
        "local",
        Map.of(orders0, List.of(1)),
        Map.of(orders0, List.of(1, 2)),
        Set.of(1, 2)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Target assignment for orders-0 exceeds the 1 replica limit");
  }

  @Test
  void rejectsNoOpAndMismatchedAssignmentSets() {
    var topicPartition = new TopicPartition("orders", 0);

    assertThatThrownBy(() -> service.createPlan(
        "local",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(topicPartition, List.of(1, 2)),
        Set.of(1, 2)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Partition reassignment plan contains no changes");

    assertThatThrownBy(() -> service.createPlan(
        "local",
        Map.of(topicPartition, List.of(1, 2)),
        Map.of(new TopicPartition("orders", 1), List.of(2, 1)),
        Set.of(1, 2)))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Current and target assignments must contain the same partitions");
  }
}
