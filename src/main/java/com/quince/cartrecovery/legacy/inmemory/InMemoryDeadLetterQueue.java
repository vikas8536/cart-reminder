package com.quince.cartrecovery.legacy.inmemory;

import com.quince.cartrecovery.legacy.model.DeadLetter;
import com.quince.cartrecovery.legacy.ports.DeadLetterQueue;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryDeadLetterQueue implements DeadLetterQueue {
    private final List<DeadLetter> letters = new ArrayList<>();

    @Override public void add(DeadLetter letter) { letters.add(letter); }

    @Override public List<DeadLetter> drain() {
        List<DeadLetter> out = List.copyOf(letters);
        letters.clear();
        return out;
    }

    @Override public int size() { return letters.size(); }
}
