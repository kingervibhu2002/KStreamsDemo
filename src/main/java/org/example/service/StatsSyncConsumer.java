package org.example.service;

import org.example.Topics;
import org.example.model.UserStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Mirrors every update on {@code user-stats-topic} into Redis.
 *
 * <p>This is a plain Kafka consumer (its own consumer group, {@code stats-sync-consumer}) —
 * not part of the Kafka Streams topology. It has no aggregation logic of its own; Kafka
 * Streams already did the aggregating and emitted the latest {@link UserStats} per user
 * via {@code userStats.toStream().to(Topics.USER_STATS, ...)} in
 * {@link org.example.config.PurchaseTopologyConfig}. This class just copies that stream
 * into Redis so every app instance can serve reads from one shared store instead of each
 * instance only holding its own partitioned slice of RocksDB.
 */
@Component
public class StatsSyncConsumer {

    private static final Logger log = LoggerFactory.getLogger(StatsSyncConsumer.class);

    private final RedisTemplate<String, UserStats> redisTemplate;

    public StatsSyncConsumer(RedisTemplate<String, UserStats> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = Topics.USER_STATS, groupId = "stats-sync-consumer")
    public void onUserStatsUpdate(@Payload UserStats stats,
                                   @Header(KafkaHeaders.RECEIVED_KEY) String userId) {
        redisTemplate.opsForValue().set(StoreQueryService.keyFor(userId), stats);
        log.debug("Synced {} -> Redis: {}", userId, stats);
    }
}
