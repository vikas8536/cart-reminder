package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.ports.IntentPublisher;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryIntentQueue implements IntentPublisher {
    private final List<ReminderIntent> intents = new ArrayList<>();

    @Override public synchronized void publish(ReminderIntent intent) { intents.add(intent); }

    /** Removes and returns everything, in publish order. */
    public synchronized List<ReminderIntent> drain() {
        List<ReminderIntent> out = List.copyOf(intents);
        intents.clear();
        return out;
    }

    public synchronized int size() { return intents.size(); }
}
