package org.example.model;

/**
 * Reference data for an item, keyed by item name in the {@code item-catalog} topic.
 *
 * @param category the item's product category
 */
public record ItemInfo(String category) {
}
