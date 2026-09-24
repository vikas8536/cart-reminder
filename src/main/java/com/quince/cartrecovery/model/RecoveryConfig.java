package com.quince.cartrecovery.model;

import java.time.Duration;
import java.util.List;

public record RecoveryConfig(Duration window, List<Duration> offsets, List<Duration> latenessBounds,
                             int frequencyCap, Duration frequencyWindow, int holdoutPercent,
                             int maxSendAttempts, Duration retryBase) {

    public RecoveryConfig {
        offsets = List.copyOf(offsets);
        latenessBounds = List.copyOf(latenessBounds);
        if (offsets.isEmpty()) throw new IllegalArgumentException("at least one offset required");
        if (offsets.get(0).compareTo(window) < 0)
            throw new IllegalArgumentException("first offset " + offsets.get(0) + " must be >= window " + window);
        for (int i = 1; i < offsets.size(); i++) {
            if (offsets.get(i).compareTo(offsets.get(i - 1)) <= 0)
                throw new IllegalArgumentException("offsets must be strictly increasing");
        }
        if (latenessBounds.size() != offsets.size())
            throw new IllegalArgumentException("one lateness bound per offset required");
        if (frequencyCap < 0 || holdoutPercent < 0 || holdoutPercent > 100 || maxSendAttempts < 1)
            throw new IllegalArgumentException("invalid numeric config");
    }

    public static RecoveryConfig defaults() {
        return new RecoveryConfig(
            Duration.ofMinutes(30),
            List.of(Duration.ofMinutes(30), Duration.ofHours(1), Duration.ofHours(24)),
            List.of(Duration.ofMinutes(5), Duration.ofMinutes(5), Duration.ofMinutes(30)),
            3, Duration.ofDays(7), 10, 5, Duration.ofMinutes(1));
    }

    public RecoveryConfig withWindow(Duration w) {
        return new RecoveryConfig(w, offsets, latenessBounds, frequencyCap, frequencyWindow, holdoutPercent, maxSendAttempts, retryBase);
    }

    /** Replaces offsets and resets lateness bounds to 5 minutes each so the sizes stay in step. */
    public RecoveryConfig withOffsets(List<Duration> o) {
        List<Duration> bounds = o.stream().map(x -> Duration.ofMinutes(5)).toList();
        return new RecoveryConfig(window, o, bounds, frequencyCap, frequencyWindow, holdoutPercent, maxSendAttempts, retryBase);
    }

    public RecoveryConfig withLatenessBounds(List<Duration> b) {
        return new RecoveryConfig(window, offsets, b, frequencyCap, frequencyWindow, holdoutPercent, maxSendAttempts, retryBase);
    }

    public RecoveryConfig withFrequencyCap(int cap) {
        return new RecoveryConfig(window, offsets, latenessBounds, cap, frequencyWindow, holdoutPercent, maxSendAttempts, retryBase);
    }

    public RecoveryConfig withMaxSendAttempts(int n) {
        return new RecoveryConfig(window, offsets, latenessBounds, frequencyCap, frequencyWindow, holdoutPercent, n, retryBase);
    }
}
