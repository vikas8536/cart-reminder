package com.quince.cartrecovery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class RecoveryConfigTest {

    @Test
    void defaultsMatchTheBrief() {
        RecoveryConfig c = RecoveryConfig.defaults();
        assertEquals(Duration.ofMinutes(30), c.window());
        assertEquals(List.of(Duration.ofMinutes(30), Duration.ofHours(1), Duration.ofHours(24)), c.offsets());
        assertEquals(List.of(Duration.ofMinutes(5), Duration.ofMinutes(5), Duration.ofMinutes(30)), c.latenessBounds());
        assertEquals(3, c.frequencyCap());
        assertEquals(Duration.ofDays(7), c.frequencyWindow());
        assertEquals(10, c.holdoutPercent());
        assertEquals(5, c.maxSendAttempts());
        assertEquals(Duration.ofMinutes(1), c.retryBase());
    }

    @Test
    void rejectsFirstOffsetSmallerThanWindow() {
        assertThrows(IllegalArgumentException.class, () ->
            RecoveryConfig.defaults().withWindow(Duration.ofMinutes(45)));
    }

    @Test
    void rejectsOffsetsThatAreNotIncreasing() {
        assertThrows(IllegalArgumentException.class, () ->
            RecoveryConfig.defaults().withOffsets(List.of(Duration.ofHours(1), Duration.ofMinutes(30))));
    }

    @Test
    void rejectsLatenessBoundsOfDifferentLength() {
        assertThrows(IllegalArgumentException.class, () ->
            RecoveryConfig.defaults().withLatenessBounds(List.of(Duration.ofMinutes(5))));
    }

    @Test
    void withOffsetsResizesLatenessBoundsToMatch() {
        RecoveryConfig c = RecoveryConfig.defaults()
            .withOffsets(List.of(Duration.ofMinutes(30), Duration.ofHours(2)));
        assertEquals(2, c.latenessBounds().size());
    }
}
