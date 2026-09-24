package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.OutboxEntry;
import com.quince.cartrecovery.ports.Outbox;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class InMemoryOutbox implements Outbox {
    private final Map<String, OutboxEntry> entries = new LinkedHashMap<>();

    @Override public void add(OutboxEntry entry) { entries.put(entry.key(), entry); }

    @Override public List<OutboxEntry> due(Instant now) {
        return entries.values().stream().filter(e -> !e.nextAttemptAt().isAfter(now)).toList();
    }

    @Override public void replace(OutboxEntry entry) { entries.put(entry.key(), entry); }

    @Override public void remove(String key) { entries.remove(key); }

    @Override public Optional<Instant> nextDueAt() {
        return entries.values().stream().map(OutboxEntry::nextAttemptAt).min(Instant::compareTo);
    }

    @Override public int size() { return entries.size(); }
}
