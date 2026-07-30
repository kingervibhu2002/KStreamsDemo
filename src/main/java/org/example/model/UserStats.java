package org.example.model;

/**
 * The running aggregate we build per user. This is the value type of the
 * materialized KTable that lives in RocksDB.
 *
 * @param count      number of purchases seen for this user
 * @param total      sum of all purchase amounts
 * @param maxAmount  largest single purchase
 */
public record UserStats(long count, double total, double maxAmount) {

    /** Starting point for the aggregation (the "initializer"). */
    public static UserStats empty() {
        return new UserStats(0, 0.0, 0.0);
    }

    /** Fold one more purchase into this aggregate (the "aggregator"). */
    public UserStats add(Purchase p) {
        return new UserStats(
                count + 1,
                total + p.amount(),
                Math.max(maxAmount, p.amount())
        );
    }

    public double average() {
        return count == 0 ? 0.0 : total / count;
    }
}
