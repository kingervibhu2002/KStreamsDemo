package org.example.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.example.Topics;
import org.example.model.UserStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interactive Query facade over the RocksDB state store.
 *
 * <p>{@link StreamsBuilderFactoryBean} gives access to the underlying
 * {@code KafkaStreams} instance that Spring manages. We query it directly
 * without going back through Kafka.
 */
@Service
public class StoreQueryService {

    private static final Logger log = LoggerFactory.getLogger(StoreQueryService.class);

    private final StreamsBuilderFactoryBean factory;

    public StoreQueryService(StreamsBuilderFactoryBean factory) {
        this.factory = factory;
    }

    public Optional<UserStats> getForUser(String userId) {
        return withStore(store -> Optional.ofNullable(store.get(userId)));
    }

    public Map<String, UserStats> getAll() {
        return withStore(store -> {
            Map<String, UserStats> result = new LinkedHashMap<>();
            try (KeyValueIterator<String, UserStats> it = store.all()) {
                while (it.hasNext()) {
                    KeyValue<String, UserStats> kv = it.next();
                    result.put(kv.key, kv.value);
                }
            }
            return result;
        });
    }

    /** Logs the full store contents every 5 seconds — visible in the console like StreamsApp did. */
    @Scheduled(fixedDelay = 5_000)
    public void logStore() {
        if (!isRunning()) return;
        try {
            Map<String, UserStats> all = getAll();
            if (all.isEmpty()) {
                log.info("user-stats-store: (empty — POST /api/purchases/random to produce events)");
                return;
            }
            log.info("---- user-stats-store ----");
            all.forEach((user, s) -> log.info(String.format(
                    "  %-8s  count=%-3d  total=%8.2f  avg=%7.2f  max=%7.2f",
                    user, s.count(), s.total(), s.average(), s.maxAmount())));
        } catch (Exception e) {
            log.debug("Store not ready: {}", e.getMessage());
        }
    }

    /** Throws {@link IllegalStateException} when the streams app is not yet RUNNING. */
    private <R> R withStore(Function<ReadOnlyKeyValueStore<String, UserStats>, R> fn) {
        KafkaStreams streams = factory.getKafkaStreams();
        if (streams == null || streams.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("Kafka Streams is not in RUNNING state yet");
        }
        ReadOnlyKeyValueStore<String, UserStats> store = streams.store(
                StoreQueryParameters.fromNameAndType(
                        Topics.USER_STATS_STORE,
                        QueryableStoreTypes.keyValueStore()));
        return fn.apply(store);
    }

    private boolean isRunning() {
        KafkaStreams streams = factory.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }
}
