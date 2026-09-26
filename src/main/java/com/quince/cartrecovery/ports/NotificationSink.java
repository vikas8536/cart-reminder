package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;

/** The notification gateway boundary. No implementation in this repo performs a real send. */
public interface NotificationSink {
    SendResult send(ReminderMessage message);
}
