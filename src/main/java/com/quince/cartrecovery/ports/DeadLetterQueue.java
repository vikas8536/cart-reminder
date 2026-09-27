package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.DeadLetter;

public interface DeadLetterQueue {
    void add(DeadLetter letter);
}
