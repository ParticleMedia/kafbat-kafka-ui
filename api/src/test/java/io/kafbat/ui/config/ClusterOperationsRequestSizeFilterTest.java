package io.kafbat.ui.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ResponseStatusException;

class ClusterOperationsRequestSizeFilterTest {

  private final ClusterOperationsProperties properties = new ClusterOperationsProperties();
  private ClusterOperationsRequestSizeFilter filter;

  @BeforeEach
  void setUp() {
    properties.setMaxRequestBytes(4);
    filter = new ClusterOperationsRequestSizeFilter(properties);
  }

  @Test
  void rejectsADeclaredBodyLargerThanTheConfiguredLimit() {
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.post(
            "/api/clusters/dev/partition-reassignments/execute")
        .contentLength(5)
        .body("12345"));

    assertThatThrownBy(() -> filter.filter(exchange, ignored -> reactor.core.publisher.Mono.empty())
        .block())
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatusCode())
        .isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
  }

  @Test
  void countsAStreamingBodyWhenContentLengthIsUnknown() {
    var exchange = MockServerWebExchange.from(MockServerHttpRequest.post(
            "/api/clusters/dev/partition-reassignments/cancel")
        .body("12345"));

    assertThatThrownBy(() -> filter.filter(exchange, filtered ->
            DataBufferUtils.join(filtered.getRequest().getBody()).then())
        .block())
        .isInstanceOf(ResponseStatusException.class)
        .extracting(error -> ((ResponseStatusException) error).getStatusCode())
        .isEqualTo(HttpStatus.PAYLOAD_TOO_LARGE);
  }
}
