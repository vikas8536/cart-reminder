package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;

/**
 * One deployable role, selected with {@code --role=<name>}. {@link #run} returns when its thread is interrupted:
 * Main's shutdown hook (SIGTERM) interrupts the role thread and waits up to 30 s for it to return.
 */
public interface Role {
    String name();

    void run(InfraConfig config, Health health, Metrics metrics) throws Exception;
}
