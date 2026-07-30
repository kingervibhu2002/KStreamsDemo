package org.example.service;

import org.example.Topics;
import org.example.model.Purchase;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class PurchaseProducerService {

    private static final List<String> USERS = List.of("alice", "bob", "carol", "dave");
    private static final List<String> ITEMS = List.of("coffee", "book", "laptop", "pen", "monitor");

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PurchaseProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void send(Purchase purchase) {
        kafkaTemplate.send(Topics.PURCHASES, purchase.userId(), purchase);
    }

    public Purchase sendRandom() {
        Purchase p = random();
        kafkaTemplate.send(Topics.PURCHASES, p.userId(), p);
        return p;
    }

    private static Purchase random() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String user = USERS.get(rnd.nextInt(USERS.size()));
        String item = ITEMS.get(rnd.nextInt(ITEMS.size()));
        double amount = Math.round(rnd.nextDouble(1.0, 1500.0) * 100.0) / 100.0;
        return new Purchase(user, item, amount);
    }
}
