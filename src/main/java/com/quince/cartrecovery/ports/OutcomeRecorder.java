package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Outcome;

public interface OutcomeRecorder {
    void record(Outcome outcome);
}
