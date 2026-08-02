package org.example.service;

import org.example.model.UserStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Read facade over the Redis serving layer.
 *
 * <p>Redis is populated by {@link StatsSyncConsumer}, which consumes
 * {@code user-stats-topic} (the changelog stream emitted by the Kafka Streams
 * aggregation) and mirrors every update into Redis. Unlike the RocksDB state store —
 * which Kafka Streams partitions across instances, so any one pod only holds a slice
 * of the keyspace — Redis is a single shared store. Every instance reads the exact
 * same Redis, so any pod can answer a query for any {@code userId}, regardless of
 * which partition/pod originally processed that user's purchases. No cross-pod
 * routing needed.
 */
@Service
public class StoreQueryService {

    private static final Logger log = LoggerFactory.getLogger(StoreQueryService.class);
    static final String KEY_PREFIX = "user-stats:";

    private final RedisTemplate<String, UserStats> redisTemplate;

    public StoreQueryService(RedisTemplate<String, UserStats> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    static String keyFor(String userId) {
        return KEY_PREFIX + userId;
    }

    public Optional<UserStats> getForUser(String userId) {
        return Optional.ofNullable(redisTemplate.opsForValue().get(keyFor(userId)));
    }

    public Map<String, UserStats> getAll() {
        Map<String, UserStats> result = new LinkedHashMap<>();
        Set<String> keys = redisTemplate.keys(KEY_PREFIX + "*");
        if (keys == null || keys.isEmpty()) {
            return result;
        }
        for (String key : keys) {
            UserStats stats = redisTemplate.opsForValue().get(key);
            if (stats != null) {
                result.put(key.substring(KEY_PREFIX.length()), stats);
            }
        }
        return result;
    }

    /** Logs Redis's full contents every 5 seconds — same shared view from every instance. */
    @Scheduled(fixedDelay = 5_000)
    public void logStore() {
        try {
            Map<String, UserStats> all = getAll();
            if (all.isEmpty()) {
                log.info("user-stats (redis): (empty — POST /api/purchases/random to produce events)");
                return;
            }
            log.info("---- user-stats (redis) ----");
            all.forEach((user, s) -> log.info(String.format(
                    "  %-8s  count=%-3d  total=%8.2f  avg=%7.2f  max=%7.2f",
                    user, s.count(), s.total(), s.average(), s.maxAmount())));
        } catch (Exception e) {
            log.debug("Redis not reachable: {}", e.getMessage());
        }
    }
}
