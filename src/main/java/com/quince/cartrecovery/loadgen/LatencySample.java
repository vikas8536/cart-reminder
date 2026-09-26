package com.quince.cartrecovery.loadgen;

import java.time.Duration;
import java.time.Instant;

/** One scheduled-to-sent latency observation, timestamped by when the reminder was scheduled for. */
public record LatencySample(Instant scheduledFor, Duration latency) {}
