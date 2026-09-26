package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Lane;

public interface SendBudget {
    /** Takes one send token for the lane if available; never blocks. */
    boolean tryAcquire(Lane lane);
}
