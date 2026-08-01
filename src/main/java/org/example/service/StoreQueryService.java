package org.example.service;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.StreamsMetadata;
import org.example.Topics;
import org.example.model.UserStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Interactive Query facade over the RocksDB state store.
 *
 * <p>Single instance: all keys are local — {@code getForUser} and {@code getAll}
 * read directly from the local RocksDB.
 *
 * <p>Multiple instances (K8s): Kafka Streams partitions the store across pods.
 * {@code getForUser} uses {@code queryMetadataForKey} to discover which pod owns
 * the requested key and forwards the HTTP request there if needed.
 * {@code getAll} fans out to every known instance and merges results.
 *
 * <p>The {@code local=true} flag on forwarded requests prevents infinite loops:
 * the receiving pod always reads its own store directly.
 */
@Service
public class StoreQueryService {

    private static final Logger log = LoggerFactory.getLogger(StoreQueryService.class);

    private final StreamsBuilderFactoryBean factory;
    private final RestTemplate restTemplate;

    // Set via APPLICATION_SERVER env var in Docker/K8s; defaults to single-node address.
    @Value("${app.instance-address:localhost:8080}")
    private String thisInstanceAddress;

    public StoreQueryService(StreamsBuilderFactoryBean factory, RestTemplate restTemplate) {
        this.factory = factory;
        this.restTemplate = restTemplate;
    }

    /**
     * Returns stats for one user.
     *
     * @param localOnly true when this call was already forwarded from another instance —
     *                  skip routing and read local RocksDB directly to prevent loops.
     */
    public Optional<UserStats> getForUser(String userId, boolean localOnly) {
        ensureRunning();

        if (localOnly) {
            return withStore(store -> Optional.ofNullable(store.get(userId)));
        }

        KeyQueryMetadata metadata = factory.getKafkaStreams().queryMetadataForKey(
                Topics.USER_STATS_STORE, userId, Serdes.String().serializer());

        if (isUnavailable(metadata)) {
            // Rebalance in progress — fall back to local (may return empty)
            log.warn("Metadata unavailable for key '{}' (rebalancing?) — local fallback", userId);
            return withStore(store -> Optional.ofNullable(store.get(userId)));
        }

        HostInfo owner = metadata.activeHost();
        if (isLocal(owner)) {
            return withStore(store -> Optional.ofNullable(store.get(userId)));
        }

        // Forward to the pod that owns this partition
        String url = "http://" + owner.host() + ":" + owner.port()
                + "/api/stats/" + userId + "?local=true";
        log.debug("Forwarding /stats/{} → {}", userId, url);
        try {
            ResponseEntity<UserStats> resp = restTemplate.getForEntity(url, UserStats.class);
            return Optional.ofNullable(resp.getBody());
        } catch (HttpClientErrorException.NotFound e) {
            return Optional.empty();
        } catch (Exception e) {
            log.warn("Forward to {} failed for '{}': {} — local fallback", owner, userId, e.getMessage());
            return withStore(store -> Optional.ofNullable(store.get(userId)));
        }
    }

    /**
     * Returns stats for all users across all instances.
     *
     * @param localOnly true when this call was forwarded — return only this pod's keys.
     */
    public Map<String, UserStats> getAll(boolean localOnly) {
        ensureRunning();

        if (localOnly) {
            return scanLocal();
        }

        // Fan out to every pod that hosts a partition of this store, merge results
        Collection<StreamsMetadata> allInstances =
                factory.getKafkaStreams().streamsMetadataForStore(Topics.USER_STATS_STORE);

        Map<String, UserStats> merged = new LinkedHashMap<>();
        for (StreamsMetadata sm : allInstances) {
            HostInfo host = sm.hostInfo();
            if (isLocal(host)) {
                merged.putAll(scanLocal());
            } else {
                String url = "http://" + host.host() + ":" + host.port() + "/api/stats?local=true";
                log.debug("Fetching stats from peer → {}", url);
                try {
                    ResponseEntity<Map<String, UserStats>> resp = restTemplate.exchange(
                            url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {});
                    if (resp.getBody() != null) {
                        merged.putAll(resp.getBody());
                    }
                } catch (Exception e) {
                    log.warn("Peer {} unreachable: {}", host, e.getMessage());
                }
            }
        }
        return merged;
    }

    /** Logs this pod's local partition contents every 5 seconds. */
    @Scheduled(fixedDelay = 5_000)
    public void logStore() {
        if (!isRunning()) return;
        try {
            Map<String, UserStats> local = scanLocal();
            if (local.isEmpty()) {
                log.info("[{}] user-stats-store: (empty — POST /api/purchases/random)", thisInstanceAddress);
                return;
            }
            log.info("[{}] ---- user-stats-store ----", thisInstanceAddress);
            local.forEach((user, s) -> log.info(String.format(
                    "  %-8s  count=%-3d  total=%8.2f  avg=%7.2f  max=%7.2f",
                    user, s.count(), s.total(), s.average(), s.maxAmount())));
        } catch (Exception e) {
            log.debug("Store not ready: {}", e.getMessage());
        }
    }

    private Map<String, UserStats> scanLocal() {
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

    private boolean isLocal(HostInfo host) {
        return (host.host() + ":" + host.port()).equals(thisInstanceAddress);
    }

    private boolean isUnavailable(KeyQueryMetadata metadata) {
        return metadata == null || metadata.activeHost().port() == -1;
    }

    private void ensureRunning() {
        if (!isRunning()) {
            throw new IllegalStateException("Kafka Streams is not in RUNNING state yet");
        }
    }

    private boolean isRunning() {
        KafkaStreams streams = factory.getKafkaStreams();
        return streams != null && streams.state() == KafkaStreams.State.RUNNING;
    }

    private <R> R withStore(Function<ReadOnlyKeyValueStore<String, UserStats>, R> fn) {
        ReadOnlyKeyValueStore<String, UserStats> store = factory.getKafkaStreams().store(
                StoreQueryParameters.fromNameAndType(
                        Topics.USER_STATS_STORE, QueryableStoreTypes.keyValueStore()));
        return fn.apply(store);
    }
}
