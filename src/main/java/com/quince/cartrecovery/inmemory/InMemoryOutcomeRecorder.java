package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import java.util.ArrayList;
import java.util.List;

public final class InMemoryOutcomeRecorder implements OutcomeRecorder {
    private final List<Outcome> outcomes = new ArrayList<>();

    @Override public synchronized void record(Outcome outcome) { outcomes.add(outcome); }

    public synchronized List<Outcome> all() { return List.copyOf(outcomes); }
}
