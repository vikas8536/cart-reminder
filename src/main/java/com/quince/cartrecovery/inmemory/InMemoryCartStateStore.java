package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/** Map-backed cart store with the same conditional semantics as the DynamoDB "carts" table. */
public final class InMemoryCartStateStore implements CartStateStore {
    private final RecoveryConfig config;
    private final int shards;
    private final Map<String, CartRecord> records = new HashMap<>();
    /** cartId → openUntil, present while the cart is open (the sparse open-by-shard index). */
    private final Map<String, Instant> openUntil = new HashMap<>();

    public InMemoryCartStateStore(RecoveryConfig config, int shards) {
        this.config = config;
        this.shards = shards;
    }

    @Override public synchronized Optional<CartRecord> get(String cartId) {
        return Optional.ofNullable(records.get(cartId));
    }

    @Override public synchronized List<CartRecord> getAll(Collection<String> cartIds) {
        List<CartRecord> out = new ArrayList<>();
        for (String id : new LinkedHashSet<>(cartIds)) {   // input order, duplicates once (controller ruling R4)
            CartRecord r = records.get(id);
            if (r != null) out.add(r);
        }
        return out;
    }

    @Override public synchronized Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition) {
        CartRecord stored = records.get(event.cartId());
        if (stored != null && stored.version() >= event.version()) return Optional.empty();
        String shopperKey = stored == null ? event.shopperKey() : stored.shopperKey();
        Arm keptArm = stored == null ? arm : stored.arm();
        List<CartItem> items = stored == null ? List.of() : stored.items();
        String firstName = stored == null ? null : stored.firstName();
        List<Instant> starts = stored == null ? List.of() : stored.sequenceStarts();
        CartRecord updated = switch (event) {
            case CartEvent.CartEdited e -> new CartRecord(e.cartId(), shopperKey, CartStatus.ACTIVE, e.version(),
                e.occurredAt(), capped(e.items()), keptArm, starts,
                e.firstName() != null ? e.firstName() : firstName, srcPartition);
            case CartEvent.CartResumed e -> new CartRecord(e.cartId(), shopperKey, CartStatus.ACTIVE, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
            case CartEvent.CartCleared e -> new CartRecord(e.cartId(), shopperKey, CartStatus.CLOSED, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
            case CartEvent.CartPurchased e -> new CartRecord(e.cartId(), shopperKey, CartStatus.CLOSED, e.version(),
                e.occurredAt(), items, keptArm, starts, firstName, srcPartition);
        };
        records.put(updated.cartId(), updated);
        if (updated.status() == CartStatus.ACTIVE) {
            openUntil.put(updated.cartId(), openUntil(updated.lastActivityAt()));
        } else {
            openUntil.remove(updated.cartId());
        }
        return Optional.of(updated);
    }

    @Override public synchronized boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible) {
        CartRecord stored = records.get(record.cartId());
        if (stored == null || stored.version() != record.version() || stored.status() != CartStatus.ACTIVE) return false;
        records.put(stored.cartId(), new CartRecord(stored.cartId(), stored.shopperKey(), CartStatus.ABANDONED,
            stored.version(), stored.lastActivityAt(), stored.items(), stored.arm(), sequenceStarts,
            stored.firstName(), stored.srcPartition()));
        if (!eligible) openUntil.remove(stored.cartId());
        return true;
    }

    @Override public synchronized boolean endSequence(String cartId, long version) {
        CartRecord stored = records.get(cartId);
        if (stored == null || stored.version() != version || stored.status() != CartStatus.ABANDONED) return false;
        openUntil.remove(cartId);
        return true;
    }

    @Override public synchronized Stream<String> openCartIds(int shard, Instant now) {
        return openUntil.entrySet().stream()
            .filter(e -> Shards.of(e.getKey(), shards) == shard && !e.getValue().isBefore(now))
            .map(Map.Entry::getKey)
            .sorted()
            .toList()
            .stream();
    }

    private Instant openUntil(Instant lastActivityAt) {
        int last = config.offsets().size() - 1;
        Instant until = lastActivityAt.plus(config.offsets().get(last)).plus(config.latenessBounds().get(last));
        Instant hour = until.truncatedTo(ChronoUnit.HOURS);
        return hour.equals(until) ? until : hour.plus(Duration.ofHours(1));
    }

    private static List<CartItem> capped(List<CartItem> items) {
        return items.size() > CartRecord.MAX_ITEMS ? items.subList(0, CartRecord.MAX_ITEMS) : items;
    }
}
