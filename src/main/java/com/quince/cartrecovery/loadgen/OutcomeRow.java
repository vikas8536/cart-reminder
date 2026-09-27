package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * Mirrors one {@code reminder-outcomes} record. {@code key} is null for {@code ABANDONED} rows,
 * exactly as the real {@code Outcome} model leaves it; {@code kind} is one of {@code ABANDONED},
 * {@code SENT}, {@code SKIPPED_LATE}, {@code CANCELLED}, {@code DEAD}, kept as a plain string so
 * this thread never depends on thread A's {@code OutcomeKind} enum.
 */
public record OutcomeRow(String key, String cartId, long version, String arm, String kind, Instant at, int attempts) {}
