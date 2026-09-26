package com.quince.cartrecovery.legacy.inmemory;

import com.quince.cartrecovery.legacy.model.NotificationIntent;
import com.quince.cartrecovery.legacy.ports.SendLedger;
import java.util.HashMap;
import java.util.Map;

public final class InMemorySendLedger implements SendLedger {
    private final Map<String, Integer> highestByCartVersion = new HashMap<>();
    private final Map<String, Boolean> keys = new HashMap<>();

    @Override public boolean recordIfAbsent(String cartId, long version, int offsetIndex) {
        String key = NotificationIntent.key(cartId, version, offsetIndex);
        if (keys.putIfAbsent(key, Boolean.TRUE) != null) return false;
        highestByCartVersion.merge(cartId + ":" + version, offsetIndex, Math::max);
        return true;
    }

    @Override public int highestOffsetIndex(String cartId, long version) {
        return highestByCartVersion.getOrDefault(cartId + ":" + version, -1);
    }

    @Override public int size() { return keys.size(); }
}
