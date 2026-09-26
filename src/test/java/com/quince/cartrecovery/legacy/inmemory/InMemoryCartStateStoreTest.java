package com.quince.cartrecovery.legacy.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.legacy.ports.CartStateStore;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryCartStateStoreTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");

    @Test
    void conditionalPutRejectsStaleExpectedVersion() {
        InMemoryCartStateStore store = new InMemoryCartStateStore();
        CartRecord v1 = CartRecord.fresh("a", "u1", Arm.TREATMENT).activity(1, T0, List.of());
        assertTrue(store.put(v1, CartStateStore.ABSENT));
        assertFalse(store.put(v1, CartStateStore.ABSENT));

        CartRecord v2 = v1.activity(2, T0.plusSeconds(1), List.of());
        assertFalse(store.put(v2, 5));
        assertTrue(store.put(v2, 1));
        assertEquals(2, store.get("a").orElseThrow().version());
    }

    @Test
    void scanOpenExcludesClosedRecords() {
        InMemoryCartStateStore store = new InMemoryCartStateStore();
        CartRecord open = CartRecord.fresh("a", "u1", Arm.TREATMENT).activity(1, T0, List.of());
        CartRecord closed = CartRecord.fresh("b", "u2", Arm.TREATMENT).activity(1, T0, List.of()).closed(2, T0);
        store.put(open, CartStateStore.ABSENT);
        store.put(closed, CartStateStore.ABSENT);

        List<CartRecord> scanned = store.scanOpen();
        assertEquals(1, scanned.size());
        assertEquals(CartStatus.ACTIVE, scanned.get(0).status());
    }
}
