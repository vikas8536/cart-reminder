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
import com.quince.cartrecovery.inmemory.InMemoryIntentQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.inmemory.PriorityQueueTimerStore;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Timer;
import com.quince.cartrecovery.model.TimerDecision;
import com.quince.cartrecovery.ports.ArmAssigner;
import com.quince.cartrecovery.ports.SendBudget;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Wires the in-memory adapters to the core stages and drives them from a fake clock. Single-threaded: production
 * ordering per cart comes from partitioning, here from handling one input at a time. One cart-events partition (0),
 * whose watermark is the clock because the detector is synchronous, so CLOCK_SKEW is zero; stallDetector() makes it
 * lag. Backoff uses the full jitter cap, so retry times are deterministic.
 */
public final class Pipeline {
    public static final int SHARDS = 4;
    private static final int PARTITION = 0;
    private static final DispatchConfig DISPATCH =
        new DispatchConfig(Duration.ofSeconds(90), Duration.ofSeconds(30), Duration.ZERO, 2);

    private record Pending(ReminderIntent intent, Instant readyAt) {}

    private final FakeClock clock;
    private final InMemoryCartStateStore store;
    private final PriorityQueueTimerStore timers;
    private final InMemoryWatermark watermark;
    private final InMemoryIntentQueue intents = new InMemoryIntentQueue();
    private final InMemorySendLedger ledger = new InMemorySendLedger(DISPATCH.lease(), SHARDS);
    private final InMemoryOutcomeRecorder outcomes = new InMemoryOutcomeRecorder();
    private final RecordingNotificationSink sink;
    private final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue();
    private final Metrics metrics = new Metrics();
    private final AbandonmentDetector detector;
    private final ReminderScheduler scheduler;
    private final Dispatcher dispatcher;
    private final Reconciler reconciler;

    private long tokensTaken;
    private Duration dispatchDelay = Duration.ZERO;
    private boolean stalled;
    private final List<CartEvent> buffered = new ArrayList<>();
    private final List<Pending> pending = new ArrayList<>();
    private final List<ReminderIntent> held = new ArrayList<>();

    public Pipeline(RecoveryConfig config, Instant start, ArmAssigner arms) {
        this.clock = new FakeClock(start);
        this.store = new InMemoryCartStateStore(config, SHARDS);
        this.timers = new PriorityQueueTimerStore(clock, DISPATCH.lease());
        this.watermark = new InMemoryWatermark(clock);
        this.sink = new RecordingNotificationSink(clock);
        SendBudget budget = lane -> {
            tokensTaken++;
            return true;
        };
        this.detector = new AbandonmentDetector(config, store, timers, arms, metrics);
        this.scheduler = new ReminderScheduler(config, DISPATCH, store, timers, watermark, intents, outcomes, metrics);
        this.dispatcher = new Dispatcher(config, DISPATCH, store, ledger, watermark, budget, sink, outcomes, dlq,
            clock, metrics, () -> 1.0);
        this.reconciler = new Reconciler(config, store, timers, ledger, clock, metrics);
    }

    public static Pipeline withDefaults(Instant start) {
        RecoveryConfig config = RecoveryConfig.defaults();
        return new Pipeline(config, start, new HashArmAssigner(HashArmAssigner.SALT, config.holdoutPercent()));
    }

    /**
     * Feeds one event at its own time: first advances the clock to the event, running everything due before it,
     * then hands the event to the detector (or buffers it while the detector is stalled). An event older than the
     * clock (a late or duplicate delivery) is handled at the current time without moving the clock.
     */
    public void ingest(CartEvent event) {
        advanceTo(event.occurredAt());
        if (stalled) {
            buffered.add(event);
        } else {
            detector.handle(event, PARTITION);
        }
        step();
    }

    /**
     * Advances the fake clock to target, stopping at every timer due time, lease expiry, retry time, and delayed
     * dispatch in order, so each runs at its own virtual time. Never moves the clock backwards.
     */
    public void advanceTo(Instant target) {
        step();
        while (true) {
            Optional<Instant> next = nextStop();
            if (next.isEmpty() || next.get().isAfter(target)) break;
            clock.set(next.get());
            step();
        }
        clock.set(target);
        step();
    }

    /** Time passes with nothing running, as during an outage. Work due meanwhile runs late on the next advanceTo. */
    public void outage(Duration downFor) {
        clock.advance(downFor);
    }

    /** Simulates losing the timer index (Redis) and the reconciler rebuilding it from durable state. */
    public void restart() {
        timers.clear();
        for (int shard = 0; shard < SHARDS; shard++) reconciler.reconcileShard(shard);
    }

    /** Simulates at-least-once timer delivery by handing a timer to the scheduler again. */
    public void redeliver(Timer timer) {
        settle(timer, scheduler.onTimer(timer));
        step();
    }

    /** Runs the replay role over everything dead-lettered so far; the retry loop then sends or skips each key. */
    public void replayDeadLetters() {
        dispatcher.replay(dlq.drain());
        step();
    }

    /** Intents reach the dispatcher this long after they are published (a consumer backlog). */
    public void setDispatchDelay(Duration delay) {
        this.dispatchDelay = delay;
    }

    /** The detector stops handling events: they are buffered and the watermark stays at the current time. */
    public void stallDetector() {
        stalled = true;
        watermark.setLagging(PARTITION, clock.now());
    }

    /** The detector handles every buffered event in order and the watermark returns to the clock. */
    public void catchUpDetector() {
        stalled = false;
        for (CartEvent e : buffered) detector.handle(e, PARTITION);
        buffered.clear();
        watermark.clearLag();
        step();
    }

    /** Everything due at the current time: one detector loop iteration, timers, dispatch, and the retry loop. */
    private void step() {
        watermark.publish(PARTITION, 1, clock.now());
        List<Timer> due;
        while (!(due = timers.claimDue(Integer.MAX_VALUE)).isEmpty()) {
            for (Timer t : due) settle(t, scheduler.onTimer(t));
        }
        for (ReminderIntent i : intents.drain()) pending.add(new Pending(i, clock.now().plus(dispatchDelay)));
        List<ReminderIntent> ready = new ArrayList<>(held);
        held.clear();
        for (Iterator<Pending> it = pending.iterator(); it.hasNext(); ) {
            Pending p = it.next();
            if (!p.readyAt().isAfter(clock.now())) {
                ready.add(p.intent());
                it.remove();
            }
        }
        for (ReminderIntent i : ready) {
            if (dispatcher.handle(i) == HandleResult.HOLD) held.add(i);
        }
        for (int shard = 0; shard < SHARDS; shard++) dispatcher.retryDue(shard, Integer.MAX_VALUE);
    }

    private void settle(Timer timer, TimerDecision decision) {
        switch (decision) {
            case TimerDecision.Ack a -> timers.ack(timer);
            case TimerDecision.Release r -> timers.release(timer, r.delay());
        }
    }

    /**
     * The earliest future time something becomes due. Work already due but held (a lagging watermark) is retried
     * at every step instead. ponytail: nextRetryAt is only the earliest row, so a held due retry hides later ones
     * until the next other stop; fine while only stallDetector() holds retries.
     */
    private Optional<Instant> nextStop() {
        Instant now = clock.now();
        return Stream.of(timers.nextDueAt(), ledger.nextRetryAt(),
                pending.stream().map(Pending::readyAt).min(Instant::compareTo))
            .flatMap(Optional::stream)
            .filter(t -> t.isAfter(now))
            .min(Instant::compareTo);
    }

    public FakeClock clock() { return clock; }
    public InMemoryCartStateStore store() { return store; }
    public PriorityQueueTimerStore timers() { return timers; }
    public InMemorySendLedger ledger() { return ledger; }
    public InMemoryOutcomeRecorder outcomes() { return outcomes; }
    public RecordingNotificationSink sink() { return sink; }
    public InMemoryDeadLetterQueue dlq() { return dlq; }
    public Metrics metrics() { return metrics; }
    /** Send tokens taken from the (unlimited) budget. */
    public long tokensTaken() { return tokensTaken; }
}
