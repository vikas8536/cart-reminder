package com.quince.cartrecovery.loadgen;

import java.time.Instant;
import java.util.List;

/** One cart's whole scripted life: every abandonment cycle it goes through, in order. */
public record CartScript(String cartId, String shopperKey, List<Cycle> cycles) {

    public CartScript {
        cycles = List.copyOf(cycles);
        if (cycles.isEmpty()) throw new IllegalArgumentException("a cart script needs at least one cycle");
    }

    /** The cart's purchase instant, or null if it never purchases. Only the last cycle can end in a purchase. */
    public Instant purchaseAt() {
        return cycles.get(cycles.size() - 1).cancelledAt();
    }
}
