package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Lane;

public interface SendBudget {
    /** Takes one send token for the lane if available; never blocks. */
    boolean tryAcquire(Lane lane);

    /** Returns a token that tryAcquire gave and no send used (review fix 3); never exceeds the budget's capacity. */
    void release(Lane lane);
}
