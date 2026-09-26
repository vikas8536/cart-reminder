package com.quince.cartrecovery.legacy.inmemory;

import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.CartStatus;
import com.quince.cartrecovery.legacy.ports.CartStateStore;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class InMemoryCartStateStore implements CartStateStore {
    private final Map<String, CartRecord> records = new HashMap<>();

    @Override public Optional<CartRecord> get(String cartId) {
        return Optional.ofNullable(records.get(cartId));
    }

    @Override public boolean put(CartRecord record, long expectedVersion) {
        CartRecord current = records.get(record.cartId());
        long currentVersion = current == null ? ABSENT : current.version();
        if (currentVersion != expectedVersion) return false;
        records.put(record.cartId(), record);
        return true;
    }

    @Override public List<CartRecord> scanOpen() {
        return records.values().stream()
            .filter(r -> r.status() != CartStatus.CLOSED)
            .sorted((a, b) -> a.cartId().compareTo(b.cartId()))
            .toList();
    }
}
