package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.config.PurchaseTopologyConfig;
import org.example.model.Purchase;
import org.example.model.UserStats;
import org.example.serde.JsonSerde;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Exercises the topology in-process with {@link TopologyTestDriver} — no broker needed.
 *
 * <p>We instantiate {@link PurchaseTopologyConfig} directly (it's a plain class; no Spring
 * context required here) and pass it the builder, keeping the test fast and focused.
 */
class StreamsTopologyTest {

    private TopologyTestDriver driver;
    private TestInputTopic<String, Purchase> purchases;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new PurchaseTopologyConfig().purchaseAggregationStream(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");

        driver = new TopologyTestDriver(builder.build(), props);
        purchases = driver.createInputTopic(
                Topics.PURCHASES,
                Serdes.String().serializer(),
                JsonSerde.of(Purchase.class).serializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void aggregatesPurchasesPerUser() {
        purchases.pipeInput("alice", new Purchase("alice", "coffee", 5.00));
        purchases.pipeInput("alice", new Purchase("alice", "book", 15.00));
        purchases.pipeInput("bob",   new Purchase("bob",   "laptop", 1200.00));

        KeyValueStore<String, UserStats> store =
                driver.getKeyValueStore(Topics.USER_STATS_STORE);

        UserStats alice = store.get("alice");
        assertNotNull(alice);
        assertEquals(2,     alice.count());
        assertEquals(20.00, alice.total(),     0.001);
        assertEquals(15.00, alice.maxAmount(), 0.001);
        assertEquals(10.00, alice.average(),   0.001);

        UserStats bob = store.get("bob");
        assertNotNull(bob);
        assertEquals(1,       bob.count());
        assertEquals(1200.00, bob.total(), 0.001);
    }
}
