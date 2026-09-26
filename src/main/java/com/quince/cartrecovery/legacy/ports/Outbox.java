package com.quince.cartrecovery.legacy.ports;

import com.quince.cartrecovery.legacy.model.OutboxEntry;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface Outbox {
    void add(OutboxEntry entry);
    List<OutboxEntry> due(Instant now);
    void replace(OutboxEntry entry);
    void remove(String key);
    Optional<Instant> nextDueAt();
    int size();
}
