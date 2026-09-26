package com.quince.cartrecovery.e2e;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/** Proves the JVM flag is on and the guard sees pinning reports. */
class PinningGuardSelfIT {
    @Test
    void detectsAVirtualThreadPinnedByAMonitor() throws Exception {
        Object lock = new Object();
        Thread t = Thread.ofVirtual().start(() -> {
            synchronized (lock) {
                try {
                    Thread.sleep(20);   // parks while holding a monitor: pinned on JDK 21
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        t.join();
        assertTrue(PinningGuard.drain().contains("onPinned"), "no pinning report captured; is -Djdk.tracePinnedThreads=full set?");
    }
}
