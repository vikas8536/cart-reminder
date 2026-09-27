package com.quince.cartrecovery.model;

import java.time.Instant;

/** A retry-index row: its key, the cart's source partition, and the row's sendBy (review fix 3: known before any token). */
public record DueRetry(String key, int srcPartition, Instant sendBy) {}
