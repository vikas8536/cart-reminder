package com.quince.cartrecovery.app;

import com.quince.cartrecovery.core.Metrics;
import java.time.Duration;

/** Runs a role in-process on its own thread; close() interrupts it (the SIGTERM convention) and waits. */
public final class RoleThread implements AutoCloseable {
    private final Health health = new Health();
    private final Metrics metrics = new Metrics();
    private final Thread thread;
    private volatile Throwable failure;

    public RoleThread(Role role, InfraConfig config) {
        this.thread = Thread.ofPlatform().name("role-" + role.name()).start(() -> {
            try {
                role.run(config, health, metrics);
            } catch (Throwable e) {
                failure = e;
            }
        });
    }

    public Health health() { return health; }
    public Metrics metrics() { return metrics; }
    public Throwable failure() { return failure; }

    /** For one-off roles: true if the role returned within the timeout. */
    public boolean join(Duration timeout) throws InterruptedException {
        return thread.join(timeout);
    }

    @Override
    public void close() {
        thread.interrupt();
        try {
            thread.join(Duration.ofSeconds(40));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) throw new AssertionError(thread.getName() + " did not stop within 40 s");
    }
}
