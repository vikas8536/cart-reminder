package com.quince.cartrecovery.legacy.ports;

import com.quince.cartrecovery.legacy.model.NotificationIntent;
import com.quince.cartrecovery.model.SendResult;

/** The notification gateway boundary. No implementation in this repo performs a real send. */
public interface NotificationSink {
    SendResult send(NotificationIntent intent);
}
