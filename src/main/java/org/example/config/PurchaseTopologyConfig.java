package org.example.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;
import org.example.Topics;
import org.example.model.Purchase;
import org.example.model.UserStats;
import org.example.serde.JsonSerde;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Defines the Kafka Streams topology as a Spring bean.
 *
 * <p>Spring wires in the {@code StreamsBuilder} (created by {@code @EnableKafkaStreams})
 * and manages the {@code KafkaStreams} lifecycle — no manual start/stop needed.
 *
 * <p>Flow:
 * <pre>
 *   purchases topic (KStream, key=userId, value=Purchase)
 *       → groupByKey → aggregate → KTable materialized in RocksDB ("user-stats-store")
 *       → toStream  → user-stats topic
 * </pre>
 */
@Configuration
public class PurchaseTopologyConfig {

    @Bean
    public KStream<String, Purchase> purchaseAggregationStream(StreamsBuilder streamsBuilder) {
        JsonSerde<Purchase> purchaseSerde = JsonSerde.of(Purchase.class);
        JsonSerde<UserStats> statsSerde = JsonSerde.of(UserStats.class);

        KStream<String, Purchase> purchases = streamsBuilder.stream(
                Topics.PURCHASES,
                Consumed.with(Serdes.String(), purchaseSerde)
        );

        // Explicit persistent (RocksDB) store — naming it makes it queryable
        // via Interactive Queries and makes the on-disk nature explicit.
        KeyValueBytesStoreSupplier storeSupplier =
                Stores.persistentKeyValueStore(Topics.USER_STATS_STORE);

        KTable<String, UserStats> userStats = purchases
                .groupByKey(Grouped.with(Serdes.String(), purchaseSerde))
                .aggregate(
                        UserStats::empty,
                        (userId, purchase, agg) -> agg.add(purchase),
                        Materialized.<String, UserStats>as(storeSupplier)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(statsSerde)
                );

        userStats.toStream().to(
                Topics.USER_STATS,
                Produced.with(Serdes.String(), statsSerde)
        );

        return purchases;
    }
}
