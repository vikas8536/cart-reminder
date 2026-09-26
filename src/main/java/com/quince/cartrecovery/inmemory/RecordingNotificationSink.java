package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.Clock;
import com.quince.cartrecovery.ports.NotificationSink;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/** Records what would have been sent. Never sends. Outcomes can be scripted for failure scenarios. */
public final class RecordingNotificationSink implements NotificationSink {
    public record Sent(ReminderMessage message, Instant sentAt) {}

    private final Clock clock;
    private final List<Sent> sent = new ArrayList<>();
    private final Deque<SendResult> scripted = new ArrayDeque<>();
    private int attempts = 0;

    public RecordingNotificationSink(Clock clock) { this.clock = clock; }

    /** The next calls to send return these results in order, then SENT. */
    public synchronized void scriptOutcomes(SendResult... results) {
        scripted.addAll(List.of(results));
    }

    @Override public synchronized SendResult send(ReminderMessage message) {
        attempts++;
        SendResult result = scripted.isEmpty() ? SendResult.SENT : scripted.poll();
        if (result == SendResult.SENT) sent.add(new Sent(message, clock.now()));
        return result;
    }

    /** Successful sends only, in order. */
    public synchronized List<Sent> sent() { return List.copyOf(sent); }

    /** Every send call, including failures. */
    public synchronized int attempts() { return attempts; }
}
