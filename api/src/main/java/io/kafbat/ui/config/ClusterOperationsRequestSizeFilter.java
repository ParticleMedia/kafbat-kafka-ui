package io.kafbat.ui.config;

import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

@Component
@Order(-100)
@RequiredArgsConstructor
public class ClusterOperationsRequestSizeFilter implements WebFilter {

  private static final Pattern PARTITION_REASSIGNMENT_ENDPOINT = Pattern.compile(
      "/api/clusters/[^/]+/partition-reassignments/(plan|validate|execute|cancel)");

  private final ClusterOperationsProperties properties;

  @Override
  public @NotNull Mono<Void> filter(
      ServerWebExchange exchange,
      @NotNull WebFilterChain chain) {
    var request = exchange.getRequest();
    var path = request.getPath().pathWithinApplication().value();
    if (request.getMethod() != HttpMethod.POST
        || !PARTITION_REASSIGNMENT_ENDPOINT.matcher(path).matches()) {
      return chain.filter(exchange);
    }
    var limit = properties.getMaxRequestBytes();
    if (request.getHeaders().getContentLength() > limit) {
      return Mono.error(payloadTooLarge(limit));
    }
    var received = new AtomicLong();
    var limitedRequest = new ServerHttpRequestDecorator(request) {
      @Override
      public reactor.core.publisher.Flux<org.springframework.core.io.buffer.DataBuffer> getBody() {
        return super.getBody().handle((buffer, sink) -> {
          if (received.addAndGet(buffer.readableByteCount()) > limit) {
            DataBufferUtils.release(buffer);
            sink.error(payloadTooLarge(limit));
          } else {
            sink.next(buffer);
          }
        });
      }
    };
    return chain.filter(exchange.mutate().request(limitedRequest).build());
  }

  private static ResponseStatusException payloadTooLarge(long limit) {
    return new ResponseStatusException(
        HttpStatus.PAYLOAD_TOO_LARGE,
        "Partition reassignment request exceeds the " + limit + " byte limit");
  }
}
