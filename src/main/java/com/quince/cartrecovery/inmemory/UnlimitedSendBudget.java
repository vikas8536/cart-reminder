package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.ports.SendBudget;

public final class UnlimitedSendBudget implements SendBudget {
    @Override public boolean tryAcquire(Lane lane) { return true; }
    @Override public void release(Lane lane) { }
}
