package com.quince.cartrecovery.model;

import java.time.Duration;

public record DispatchConfig(Duration lease, Duration gatewayTimeout, Duration clockSkew, int fastOffsets) {

    public DispatchConfig {
        if (lease.compareTo(gatewayTimeout.multipliedBy(3)) < 0)
            throw new IllegalArgumentException("lease " + lease + " must be >= 3 x gatewayTimeout " + gatewayTimeout);
    }

    public static DispatchConfig defaults() {
        return new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ofSeconds(5), 2);
    }
}
