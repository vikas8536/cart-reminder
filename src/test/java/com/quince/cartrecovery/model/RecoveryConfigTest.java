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
    void rejectsNonPositiveDurationsAndNegativeLatenessBounds() {
        Duration zero = Duration.ZERO;
        Duration m30 = Duration.ofMinutes(30);
        List<Duration> offsets = List.of(m30, Duration.ofHours(1));
        List<Duration> bounds = List.of(Duration.ofMinutes(5), Duration.ofMinutes(5));
        Duration week = Duration.ofDays(7);
        Duration minute = Duration.ofMinutes(1);

        assertThrows(IllegalArgumentException.class, () ->
            new RecoveryConfig(zero, offsets, bounds, 3, week, 10, 5, minute), "window");
        assertThrows(IllegalArgumentException.class, () ->
            new RecoveryConfig(m30, offsets, List.of(Duration.ofMinutes(-1), Duration.ofMinutes(5)), 3, week, 10, 5, minute),
            "lateness bound");
        assertThrows(IllegalArgumentException.class, () ->
            new RecoveryConfig(m30, offsets, bounds, 3, zero, 10, 5, minute), "frequencyWindow");
        assertThrows(IllegalArgumentException.class, () ->
            new RecoveryConfig(m30, offsets, bounds, 3, week, 10, 5, zero), "retryBase");
    }

    @Test
    void acceptsAZeroLatenessBound() {
        RecoveryConfig c = RecoveryConfig.defaults()
            .withLatenessBounds(List.of(Duration.ZERO, Duration.ZERO, Duration.ZERO));
        assertEquals(Duration.ZERO, c.latenessBounds().get(0));
    }

    @Test
    void withOffsetsResizesLatenessBoundsToMatch() {
        RecoveryConfig c = RecoveryConfig.defaults()
            .withOffsets(List.of(Duration.ofMinutes(30), Duration.ofHours(2)));
        assertEquals(2, c.latenessBounds().size());
    }
}
