package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryDeadLetterQueue implements DeadLetterQueue {
    private final List<DeadLetter> letters = new ArrayList<>();

    @Override public synchronized void add(DeadLetter letter) { letters.add(letter); }

    /** Removes and returns everything, for replay. */
    public synchronized List<DeadLetter> drain() {
        List<DeadLetter> out = List.copyOf(letters);
        letters.clear();
        return out;
    }

    public synchronized int size() { return letters.size(); }
}
