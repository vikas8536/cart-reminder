package com.quince.cartrecovery;

import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

public final class TestSupport {
    private TestSupport() {}

    public static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    public static final String CART = "cart-1";
    public static final String SHOPPER = "user-42";
    public static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    public static Instant at(Duration afterT0) { return T0.plus(afterT0); }
    public static Duration min(long m) { return Duration.ofMinutes(m); }
    public static Duration hrs(long h) { return Duration.ofHours(h); }

    public static CartEvent.CartEdited edited(long version, Duration afterT0) {
        return new CartEvent.CartEdited(CART, SHOPPER, version, at(afterT0), ITEMS);
    }
    public static CartEvent.CartResumed resumed(long version, Duration afterT0) {
        return new CartEvent.CartResumed(CART, SHOPPER, version, at(afterT0));
    }
    public static CartEvent.CartCleared cleared(long version, Duration afterT0) {
        return new CartEvent.CartCleared(CART, SHOPPER, version, at(afterT0));
    }
    public static CartEvent.CartPurchased purchased(long version, Duration afterT0) {
        return new CartEvent.CartPurchased(CART, SHOPPER, version, at(afterT0));
    }
}
