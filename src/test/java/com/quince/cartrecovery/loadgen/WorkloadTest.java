package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkloadTest {
    private static final Instant START = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults();

    @Test void sameSeedProducesTheSameScript() {
        Workload.Result a = Workload.generateCarts(42L, "run1", 50, Duration.ofMinutes(5), START, CONFIG);
        Workload.Result b = Workload.generateCarts(42L, "run1", 50, Duration.ofMinutes(5), START, CONFIG);
        assertEquals(a.events(), b.events());
        assertEquals(a.scripts(), b.scripts());
    }

    @Test void generatesExactlyTheRequestedCartCount() {
        Workload.Result result = Workload.generateCarts(1L, "run2", 200, Duration.ofMinutes(5), START, CONFIG);
        assertEquals(200, result.scripts().size());
    }

    @Test void ratePacedGenerationSizesCartCountFromRateAndDuration() {
        Workload.Result result = Workload.generate(1L, "run3", 100.0, Duration.ofMinutes(1), START, CONFIG);
        // ~9.5 events per cart, 100 events/s * 60s = 6000 events, so roughly 6000/9.5 ~= 632 carts
        assertTrue(result.scripts().size() > 500 && result.scripts().size() < 800,
            "expected several hundred carts, got " + result.scripts().size());
    }

    @Test void eventsAreTimeOrdered() {
        Workload.Result result = Workload.generateCarts(7L, "run4", 300, Duration.ofMinutes(5), START, CONFIG);
        List<ScriptedEvent> events = result.events();
        for (int i = 1; i < events.size(); i++) {
            assertTrue(!events.get(i).occurredAt().isBefore(events.get(i - 1).occurredAt()),
                "events must be time-ordered");
        }
    }

    @Test void roughlySeventyPercentOfCartsNeverPurchase() {
        Workload.Result result = Workload.generateCarts(99L, "run5", 2000, Duration.ofMinutes(10), START, CONFIG);
        long neverPurchase = result.scripts().stream().filter(s -> s.purchaseAt() == null).count();
        double ratio = neverPurchase / (double) result.scripts().size();
        assertTrue(ratio > 0.60 && ratio < 0.80, "abandon ratio out of range: " + ratio);
    }

    @Test void noCartExceedsTheDefaultFrequencyCapOfThreeCycles() {
        Workload.Result result = Workload.generateCarts(5L, "run6", 1000, Duration.ofMinutes(10), START, CONFIG);
        for (CartScript script : result.scripts()) {
            assertTrue(script.cycles().size() <= CONFIG.frequencyCap(),
                script.cartId() + " has " + script.cycles().size() + " cycles");
        }
    }

    @Test void everyCartIdCarriesTheRunPrefix() {
        Workload.Result result = Workload.generateCarts(3L, "prefix-xyz", 20, Duration.ofMinutes(1), START, CONFIG);
        for (CartScript script : result.scripts()) {
            assertTrue(script.cartId().startsWith("prefix-xyz-"));
        }
    }

    @Test void anEarlierCyclesCancellationAlwaysLeadsIntoTheNextCycle() {
        Workload.Result result = Workload.generateCarts(11L, "run7", 500, Duration.ofMinutes(5), START, CONFIG);
        for (CartScript script : result.scripts()) {
            List<Cycle> cycles = script.cycles();
            for (int i = 0; i < cycles.size() - 1; i++) {
                if (cycles.get(i).cancelledAt() != null) {
                    assertTrue(!cycles.get(i + 1).lastActivityAt().isBefore(cycles.get(i).cancelledAt()));
                }
            }
        }
    }
}
