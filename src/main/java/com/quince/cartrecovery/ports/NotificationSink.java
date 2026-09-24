package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.NotificationIntent;
import com.quince.cartrecovery.model.SendResult;

/** The notification gateway boundary. No implementation in this repo performs a real send. */
public interface NotificationSink {
    SendResult send(NotificationIntent intent);
}
