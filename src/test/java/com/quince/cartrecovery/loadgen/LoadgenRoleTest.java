package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LoadgenRoleTest {
    @Test void drainWaitIsWindowPlusLastOffsetPlusLastLatenessPlusOneReconcileInterval() {
        RecoveryConfig config = RecoveryConfig.defaults(); // window 30m, last offset 24h, last lateness 30m
        Duration reconcileInterval = Duration.ofMinutes(5);

        Duration wait = LoadgenRole.drainWait(config, reconcileInterval);

        Duration expected = config.window()
            .plus(config.offsets().get(config.offsets().size() - 1))
            .plus(config.latenessBounds().get(config.latenessBounds().size() - 1))
            .plus(reconcileInterval);
        assertEquals(expected, wait);
    }
}
