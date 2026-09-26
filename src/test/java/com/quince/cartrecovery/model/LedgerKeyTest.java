package com.quince.cartrecovery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class LedgerKeyTest {

    @Test
    void formatsAsCartIdVersionOffset() {
        assertEquals("cart-1:3:0", new LedgerKey("cart-1", 3, 0).toString());
    }

    @Test
    void parsesFromTheRightSoACartIdMayContainColons() {
        LedgerKey k = new LedgerKey("shop:eu:cart:9", 12, 2);
        assertEquals(k, LedgerKey.parse(k.toString()));
        assertEquals(new LedgerKey("a:b", 1, 0), LedgerKey.parse("a:b:1:0"));
    }

    @Test
    void rejectsMalformedKeys() {
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:3"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse(":3:0"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:x:0"));
        assertThrows(IllegalArgumentException.class, () -> LedgerKey.parse("cart-1:3:"));
    }
}
