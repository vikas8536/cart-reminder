package com.quince.cartrecovery.ports;

import java.time.Instant;

/**
 * Per-partition event-time watermark: every cart event appended to the partition before current(p)
 * has been processed. Production: Redis hash "watermarks", timed by Redis TIME.
 */
public interface Watermark {
    /** Rejects a lower generation than stored; same generation keeps max(stored, eventTime); a higher one overwrites. */
    void publish(int partition, long generation, Instant eventTime);

    /** Instant.EPOCH if unknown or not written for more than 5 s; srcPartition -1 gives the minimum over all partitions. */
    Instant current(int srcPartition);

    /** The watermark's time source. */
    Instant now();
}
