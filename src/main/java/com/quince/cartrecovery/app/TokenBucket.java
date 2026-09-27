package com.quince.cartrecovery.app;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.ports.SendBudget;
import java.util.function.LongSupplier;

/**
 * The per-replica send budget shared by both lanes (spec §6.3). Capacity is one second of rate.
 * FAST takes any whole token; SLOW takes one only while more than {@code fastReserve × capacity}
 * remain, so the next-day burst cannot starve the early reminders. Never blocks.
 */
public final class TokenBucket implements SendBudget {
    private final double ratePerNano;
    private final double capacity;
    private final double reserve;
    private final LongSupplier nanoTime;
    private double tokens;
    private long last;

    public TokenBucket(double ratePerSecond, double fastReserve, LongSupplier nanoTime) {
        if (!(ratePerSecond > 0)) throw new IllegalArgumentException("ratePerSecond must be positive");
        if (fastReserve < 0 || fastReserve >= 1) throw new IllegalArgumentException("fastReserve must be in [0, 1)");
        // ponytail: capacity floors at 1 token so a rate below 1/s still sends; burst = 1 s of rate.
        this.capacity = Math.max(1.0, ratePerSecond);
        this.ratePerNano = ratePerSecond / 1e9;
        this.reserve = fastReserve * capacity;
        this.nanoTime = nanoTime;
        this.tokens = capacity;
        this.last = nanoTime.getAsLong();
    }

    @Override
    public synchronized boolean tryAcquire(Lane lane) {
        refill();
        boolean ok = lane == Lane.FAST ? tokens >= 1 : slowOk();
        if (ok) tokens -= 1;
        return ok;
    }

    /** False while the bucket is at or below the fast reserve: the slow-lane consumer pauses. */
    public synchronized boolean slowAllowed() {
        refill();
        return slowOk();
    }

    /** False while the bucket is empty: the fast-lane consumer pauses. */
    public synchronized boolean anyAvailable() {
        refill();
        return tokens >= 1;
    }

    private boolean slowOk() {
        return tokens >= 1 && tokens > reserve;
    }

    private void refill() {
        long now = nanoTime.getAsLong();
        tokens = Math.min(capacity, tokens + (now - last) * ratePerNano);
        last = now;
    }
}
