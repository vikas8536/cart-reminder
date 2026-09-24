package com.quince.cartrecovery;

import com.quince.cartrecovery.core.AbandonmentDetector;
import com.quince.cartrecovery.core.Dispatcher;
import com.quince.cartrecovery.core.Metrics;
import com.quince.cartrecovery.core.Reconciler;
import com.quince.cartrecovery.core.ReminderScheduler;
import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.HashArmAssigner;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryDeadLetterQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutbox;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.ports.ArmAssigner;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Wires the in-memory adapters to the core stages and drives them from a fake clock.
 * Single-threaded: production ordering per cart comes from stream partitioning, here it
 * comes from processing one input at a time.
 */
public final class Pipeline {
    private final FakeClock clock;
    private final InMemoryCartStateStore store = new InMemoryCartStateStore();
    private final PriorityQueueTimerStore timers = new PriorityQueueTimerStore();
    private final InMemorySendLedger ledger = new InMemorySendLedger();
    private final InMemoryOutbox outbox = new InMemoryOutbox();
    private final RecordingNotificationSink sink;
    private final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue();
    private final Metrics metrics = new Metrics();
    private final AbandonmentDetector detector;
    private final ReminderScheduler scheduler;
    private final Dispatcher dispatcher;
    private final Reconciler reconciler;

    public Pipeline(RecoveryConfig config, Instant start, ArmAssigner arms) {
        this.clock = new FakeClock(start);
        this.sink = new RecordingNotificationSink(clock);
        this.detector = new AbandonmentDetector(config, store, timers, arms, metrics);
        this.scheduler = new ReminderScheduler(config, store, timers, ledger, outbox, clock, metrics);
        this.dispatcher = new Dispatcher(config, store, outbox, sink, dlq, clock, metrics);
        this.reconciler = new Reconciler(config, store, timers, ledger, clock, metrics);
    }

    public static Pipeline withDefaults(Instant start) {
        RecoveryConfig config = RecoveryConfig.defaults();
        return new Pipeline(config, start, new HashArmAssigner("cart-recovery-v1", config.holdoutPercent()));
    }

    /**
     * Feeds one event through the detector at its own time: first advances the clock to the event,
     * firing every timer and retry due before it, then handles the event. An event older than the
     * clock (a late or duplicate delivery) is handled at the current time without moving the clock.
     */
    public void ingest(CartEvent event) {
        advanceTo(event.occurredAt());
        detector.handle(event);
        dispatcher.drain();
    }

    /**
     * Advances the fake clock to target, stopping at every timer due time and outbox retry time
     * in order so each fire runs at its own virtual time. Timers scheduled during a fire are
     * picked up in the same pass. Never moves the clock backwards.
     */
    public void advanceTo(Instant target) {
        while (true) {
            Optional<Instant> next = earliest(timers.nextDueAt(), outbox.nextDueAt());
            if (next.isEmpty() || next.get().isAfter(target)) break;
            clock.set(next.get());
            for (Timer t : timers.popDue(clock.now())) {
                scheduler.onTimer(t);
            }
            dispatcher.drain();
        }
        clock.set(target);
        dispatcher.drain();
    }

    /** Time passes with nothing running, as during an outage. Timers due meanwhile fire late on the next advanceTo. */
    public void outage(Duration downFor) {
        clock.advance(downFor);
    }

    /** Simulates losing the timer index and rebuilding it from durable state. */
    public void restart() {
        timers.clear();
        reconciler.rebuildTimers();
    }

    /** Simulates at-least-once timer delivery by handing a timer to the scheduler again. */
    public void redeliver(Timer timer) {
        scheduler.onTimer(timer);
        dispatcher.drain();
    }

    public void replayDeadLetters() {
        dispatcher.replayDeadLetters();
        dispatcher.drain();
    }

    private static Optional<Instant> earliest(Optional<Instant> a, Optional<Instant> b) {
        if (a.isEmpty()) return b;
        if (b.isEmpty()) return a;
        return a.get().isBefore(b.get()) ? a : b;
    }

    public FakeClock clock() { return clock; }
    public InMemoryCartStateStore store() { return store; }
    public PriorityQueueTimerStore timers() { return timers; }
    public InMemorySendLedger ledger() { return ledger; }
    public InMemoryOutbox outbox() { return outbox; }
    public RecordingNotificationSink sink() { return sink; }
    public InMemoryDeadLetterQueue dlq() { return dlq; }
    public Metrics metrics() { return metrics; }
}
