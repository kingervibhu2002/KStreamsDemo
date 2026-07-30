package org.example;

/**
 * @deprecated Kafka Streams lifecycle is now managed by Spring Boot via
 * {@code @EnableKafkaStreams}. Store queries are served by
 * {@link org.example.service.StoreQueryService} and exposed via
 * {@link org.example.web.StatsController} (GET /api/stats).
 */
@Deprecated
public final class StreamsApp {
    private StreamsApp() {}
}
