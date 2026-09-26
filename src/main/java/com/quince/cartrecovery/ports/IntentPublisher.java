package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ReminderIntent;

public interface IntentPublisher {
    /** Blocks until the intent is acknowledged; the lane is chosen from offsetIndex. */
    void publish(ReminderIntent intent);
}
