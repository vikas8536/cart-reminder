package com.quince.cartrecovery.legacy.ports;

import com.quince.cartrecovery.model.Timer;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/** Derived due-time index, one timer per cart. Production: Redis sorted set sharded by cart hash. */
public interface TimerStore {
    void upsert(Timer timer);
    void remove(String cartId);
    List<Timer> popDue(Instant upTo);
    Optional<Instant> nextDueAt();
    int size();
    void clear();
}
