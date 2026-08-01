package org.example;

/** Shared topic / store names so producer, streams app, and consumer agree. */
public final class Topics {

    /** Input: one record per purchase, keyed by userId. */
    public static final String PURCHASES = "purchases";

    /** Output: the changelog of per-user stats emitted by the aggregation. */
    public static final String USER_STATS = "user-stats";

    /** Name of the materialized state store (lives in RocksDB on disk). */
    public static final String USER_STATS_STORE = "user-stats-store";

    /** GlobalKTable demo: reference data, keyed by item name (not userId). */
    public static final String ITEM_CATALOG = "item-catalog";

    /** Name of the GlobalKTable's materialized store — fully replicated on every instance. */
    public static final String ITEM_CATALOG_STORE = "item-catalog-store";

    /** GlobalKTable demo: purchases enriched with the item's category. */
    public static final String ENRICHED_PURCHASES = "enriched-purchases";

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private Topics() {
    }
}
