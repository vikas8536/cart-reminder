package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.DeadLetter;
import java.util.List;

public interface DeadLetterQueue {
    void add(DeadLetter letter);
    /** Removes and returns everything, for replay. */
    List<DeadLetter> drain();
    int size();
}
