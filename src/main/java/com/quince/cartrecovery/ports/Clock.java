package com.quince.cartrecovery.ports;

import java.time.Instant;

public interface Clock {
    Instant now();
}
