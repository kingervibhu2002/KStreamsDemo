package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.model.Purchase;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Produces random purchase events to the "purchases" topic, keyed by userId so
 * the Streams app aggregates per user.
 *
 * Run with:  mvn exec:java -Dexec.mainClass=org.example.ProducerApp
 * Optionally pass a count:  -Dexec.args="50"   (default 20, use 0 for endless)
 */
public class ProducerApp {

    private static final List<String> USERS = List.of("alice", "bob", "carol", "dave");
    private static final List<String> ITEMS = List.of("coffee", "book", "laptop", "pen", "monitor");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 20;
        boolean endless = count == 0;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Topics.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Producing " + (endless ? "endless" : count) + " purchase events...");
            for (long i = 0; endless || i < count; i++) {
                Purchase p = randomPurchase();
                String value = MAPPER.writeValueAsString(p);
                producer.send(new ProducerRecord<>(Topics.PURCHASES, p.userId(), value),
                        (metadata, ex) -> {
                            if (ex != null) {
                                ex.printStackTrace();
                            }
                        });
                System.out.println("  -> " + p.userId() + " : " + value);
                Thread.sleep(500);
            }
            producer.flush();
            System.out.println("Done.");
        }
    }

    private static Purchase randomPurchase() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String user = USERS.get(rnd.nextInt(USERS.size()));
        String item = ITEMS.get(rnd.nextInt(ITEMS.size()));
        double amount = Math.round(rnd.nextDouble(1.0, 1500.0) * 100.0) / 100.0;
        return new Purchase(user, item, amount);
    }
}
