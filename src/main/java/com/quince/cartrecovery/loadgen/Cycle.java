package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * One abandonment cycle in a cart's script: the version and last-activity time the real pipeline
 * would record when the cart falls quiet, and when (if ever) something cancels the pending
 * reminder sequence for this cycle. For every cycle but the last, that is the next cycle's resume
 * time; for the last cycle, it is the cart's purchase time, or null if the cart never purchases.
 */
public record Cycle(long version, Instant lastActivityAt, Instant cancelledAt) {}
