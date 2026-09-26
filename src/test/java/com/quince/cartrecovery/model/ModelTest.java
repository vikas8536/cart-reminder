package com.quince.cartrecovery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ModelTest {
    private static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");

    @Test
    void legacyConstructorsDefaultTheNewComponents() {
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, T0, List.of(), Arm.TREATMENT, List.of());
        assertNull(r.firstName());
        assertEquals(-1, r.srcPartition());
        assertEquals(-1, Timer.checkAbandon("c", 1, T0).srcPartition());
        assertEquals(-1, Timer.reminder("c", 1, 0, T0).srcPartition());
        assertNull(new CartEvent.CartEdited("c", "u", 1, T0, List.of()).firstName());
    }

    @Test
    void activityCapsItemsAtFiftyAndKeepsNameAndPartition() {
        List<CartItem> many = IntStream.range(0, 60).mapToObj(i -> new CartItem("S" + i, "n", 1, 1)).toList();
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, T0, List.of(), Arm.TREATMENT, List.of(), "Ada", 4)
            .activity(2, T0, many);
        assertEquals(50, r.items().size());
        assertEquals("Ada", r.firstName());
        assertEquals(4, r.srcPartition());
    }

    @Test
    void startsWithPrunesToTheFrequencyWindowAndAppendsTheStart() {
        Instant now = T0.plus(Duration.ofDays(10));
        CartRecord r = new CartRecord("c", "u", CartStatus.ACTIVE, 1, now, List.of(), Arm.TREATMENT,
            List.of(now.minus(Duration.ofDays(8)), now.minus(Duration.ofDays(7)), now.minus(Duration.ofDays(1))));

        assertEquals(List.of(now.minus(Duration.ofDays(7)), now.minus(Duration.ofDays(1)), now),
            r.startsWith(now, now, Duration.ofDays(7)));
    }

    @Test
    void laneSplitsAtFastOffsets() {
        assertEquals(Lane.FAST, Lane.of(0, 2));
        assertEquals(Lane.FAST, Lane.of(1, 2));
        assertEquals(Lane.SLOW, Lane.of(2, 2));
    }

    @Test
    void shardsIsFloorModOfTheHash() {
        assertEquals(Math.floorMod("cart-1".hashCode(), 8), Shards.of("cart-1", 8));
        for (String id : List.of("a", "zz", "cart-9", "ÿþ")) {
            int s = Shards.of(id, 8);
            assertEquals(true, s >= 0 && s < 8);
        }
    }

    @Test
    void dispatchConfigDefaultsAndLeaseRule() {
        DispatchConfig d = DispatchConfig.defaults();
        assertEquals(Duration.ofSeconds(90), d.lease());
        assertEquals(Duration.ofSeconds(30), d.gatewayTimeout());
        assertEquals(Duration.ofSeconds(5), d.clockSkew());
        assertEquals(2, d.fastOffsets());
        new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ZERO, 2);
        assertThrows(IllegalArgumentException.class, () ->
            new DispatchConfig(Duration.ofSeconds(89), Duration.ofSeconds(30), Duration.ofSeconds(5), 2));
    }
}
