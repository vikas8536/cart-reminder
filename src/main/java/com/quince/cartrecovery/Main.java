package com.quince.cartrecovery;

import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/** Plays a scripted scenario through the pipeline on a fake clock and prints which triggers fired. */
public final class Main {
    private static final Instant T0 = Instant.parse("2026-01-01T09:00:00Z");
    private static final List<CartItem> ITEMS = List.of(
        new CartItem("SKU-1", "Linen Shirt", 1, 4990),
        new CartItem("SKU-2", "Cashmere Sweater", 1, 9900));

    public static void main(String[] args) {
        Pipeline p = new Pipeline(RecoveryConfig.defaults(), T0, key -> Arm.TREATMENT);
        int[] seen = {0};

        System.out.println("virtual start " + T0);
        System.out.println();

        step(p, seen, "cart A edited at +0m, cart B edited at +0m, cart C edited at +0m", () -> {
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("B", "user-b", 1, T0, ITEMS));
            p.ingest(new CartEvent.CartEdited("C", "user-c", 1, T0, ITEMS));
        });
        step(p, seen, "cart B edited again at +20m (clock reset)", () ->
            p.ingest(new CartEvent.CartEdited("B", "user-b", 2, T0.plus(Duration.ofMinutes(20)), ITEMS)));
        step(p, seen, "cart C purchased at +25m (cancels)", () ->
            p.ingest(new CartEvent.CartPurchased("C", "user-c", 2, T0.plus(Duration.ofMinutes(25)))));
        step(p, seen, "advance to +30m", () -> p.advanceTo(T0.plus(Duration.ofMinutes(30))));
        step(p, seen, "duplicate delivery of cart A's first edit (ignored)", () ->
            p.ingest(new CartEvent.CartEdited("A", "user-a", 1, T0, ITEMS)));
        step(p, seen, "advance to +1h", () -> p.advanceTo(T0.plus(Duration.ofHours(1))));
        step(p, seen, "restart: timer index lost and rebuilt from cart records", p::restart);
        step(p, seen, "cart A purchased at +1h10m (stops the 24h reminder)", () ->
            p.ingest(new CartEvent.CartPurchased("A", "user-a", 2, T0.plus(Duration.ofMinutes(70)))));
        step(p, seen, "advance to +25h", () -> p.advanceTo(T0.plus(Duration.ofHours(25))));

        System.out.println("metrics");
        for (Map.Entry<String, Long> e : p.metrics().snapshot().entrySet()) {
            System.out.printf("  %-28s %d%n", e.getKey(), e.getValue());
        }
    }

    private static void step(Pipeline p, int[] seen, String label, Runnable action) {
        System.out.println("== " + label);
        action.run();
        List<RecordingNotificationSink.Sent> sent = p.sink().sent();
        for (int i = seen[0]; i < sent.size(); i++) {
            RecordingNotificationSink.Sent s = sent.get(i);
            System.out.printf("   FIRED  %s  cart=%s offset=%d key=%s%n",
                s.sentAt(), s.message().cartId(), LedgerKey.parse(s.message().key()).offsetIndex(), s.message().key());
        }
        if (seen[0] == sent.size()) System.out.println("   (no sends)");
        seen[0] = sent.size();
        System.out.println("   clock now " + p.clock().now() + ", pending timers " + p.timers().size());
        System.out.println();
    }
}
