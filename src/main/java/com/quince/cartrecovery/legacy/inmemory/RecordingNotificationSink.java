package com.quince.cartrecovery.legacy.inmemory;

import com.quince.cartrecovery.legacy.model.NotificationIntent;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.legacy.ports.NotificationSink;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Records what would have been sent. Never sends. Outcomes can be scripted for failure scenarios. */
public final class RecordingNotificationSink implements NotificationSink {
    public record Sent(NotificationIntent intent, Instant sentAt) {}

    private final Clock clock;
    private final List<Sent> sent = new ArrayList<>();
    private final Deque<SendResult> scripted = new ArrayDeque<>();
    private int attempts = 0;

    public RecordingNotificationSink(Clock clock) { this.clock = clock; }

    /** The next calls to send return these results in order, then SENT. */
    public void scriptOutcomes(SendResult... results) {
        scripted.addAll(List.of(results));
    }

    @Override public SendResult send(NotificationIntent intent) {
        attempts++;
        SendResult result = scripted.isEmpty() ? SendResult.SENT : scripted.poll();
        if (result == SendResult.SENT) sent.add(new Sent(intent, clock.now()));
        return result;
    }

    public List<Sent> sent() { return List.copyOf(sent); }
    public int attempts() { return attempts; }
}
