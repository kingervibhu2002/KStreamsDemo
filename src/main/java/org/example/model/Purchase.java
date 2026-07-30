package org.example.model;

/**
 * An incoming purchase event. Keyed in Kafka by userId.
 *
 * @param userId   who made the purchase (record key)
 * @param item     what was bought
 * @param amount   how much it cost
 */
public record Purchase(String userId, String item, double amount) {
}
