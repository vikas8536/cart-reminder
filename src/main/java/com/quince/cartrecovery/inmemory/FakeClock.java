package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.ports.Clock;
import java.time.Duration;
import java.time.Instant;

public final class FakeClock implements Clock {
    private Instant now;

    public FakeClock(Instant start) { this.now = start; }

    @Override public Instant now() { return now; }

    /** Moves forward only. A target in the past is ignored. */
    public void set(Instant target) {
        if (target.isAfter(now)) now = target;
    }

    public void advance(Duration d) { now = now.plus(d); }
}
