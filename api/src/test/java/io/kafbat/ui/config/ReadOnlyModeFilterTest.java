package io.kafbat.ui.config;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.kafbat.ui.exception.ReadOnlyModeException;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.ClustersStorage;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ReadOnlyModeFilterTest {

  private final ClustersStorage clustersStorage = mock(ClustersStorage.class);
  private final WebFilterChain chain = mock(WebFilterChain.class);
  private ReadOnlyModeFilter filter;

  @BeforeEach
  void setUp() {
    var cluster = KafkaCluster.builder().name("dev").readOnly(true).build();
    when(clustersStorage.getClusterByName("dev")).thenReturn(Optional.of(cluster));
    when(chain.filter(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());
    filter = new ReadOnlyModeFilter(clustersStorage);
  }

  @Test
  void allowsPlanPreviewAndValidationForReadOnlyClusters() {
    for (var operation : new String[]{"plan", "validate"}) {
      var exchange = MockServerWebExchange.from(MockServerHttpRequest.post(
          "/api/clusters/dev/partition-reassignments/" + operation).build());

      StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

      verify(chain).filter(exchange);
    }
  }

  @Test
  void stillRejectsUnsafePostsForReadOnlyClusters() {
    var exchange = MockServerWebExchange.from(
        MockServerHttpRequest.post("/api/clusters/dev/topics").build());

    StepVerifier.create(filter.filter(exchange, chain))
        .expectError(ReadOnlyModeException.class)
        .verify();
  }

  @Test
  void rejectsPartitionReassignmentExecutionForReadOnlyClusters() {
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.post(
        "/api/clusters/dev/partition-reassignments/execute").build());

    StepVerifier.create(filter.filter(exchange, chain))
        .expectError(ReadOnlyModeException.class)
        .verify();
  }
}
