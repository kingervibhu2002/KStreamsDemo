package org.example;

/** Shared topic / store names so producer, streams app, and consumer agree. */
public final class Topics {

    /** Input: one record per purchase, keyed by userId. */
    public static final String PURCHASES = "purchases";

    /** Output: the changelog of per-user stats emitted by the aggregation. */
    public static final String USER_STATS = "user-stats";

    /** Name of the materialized state store (lives in RocksDB on disk). */
    public static final String USER_STATS_STORE = "user-stats-store";

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private Topics() {
    }
}
