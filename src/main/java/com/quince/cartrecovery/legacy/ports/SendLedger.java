package com.quince.cartrecovery.legacy.ports;

/** Dedupe ledger keyed by cart id, version, offset index. Production: DynamoDB conditional put. */
public interface SendLedger {
    /** Returns true if the row was newly recorded, false if it already existed. */
    boolean recordIfAbsent(String cartId, long version, int offsetIndex);

    /** Highest offset index recorded for this cart and version, or -1 if none. */
    int highestOffsetIndex(String cartId, long version);

    int size();
}
