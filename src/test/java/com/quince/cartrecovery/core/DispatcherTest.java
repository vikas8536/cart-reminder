package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryDeadLetterQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutbox;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.NotificationIntent;
import com.quince.cartrecovery.model.OutboxEntry;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.ports.CartStateStore;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DispatcherTest {
    private FakeClock clock;
    private InMemoryCartStateStore store;
    private InMemoryOutbox outbox;
    private RecordingNotificationSink sink;
    private InMemoryDeadLetterQueue dlq;
    private Metrics metrics;
    private Dispatcher dispatcher;
    private CartRecord abandoned;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(at(min(30)));
        store = new InMemoryCartStateStore();
        outbox = new InMemoryOutbox();
        sink = new RecordingNotificationSink(clock);
        dlq = new InMemoryDeadLetterQueue();
        metrics = new Metrics();
        dispatcher = new Dispatcher(RecoveryConfig.defaults().withMaxSendAttempts(3),
            store, outbox, sink, dlq, clock, metrics);
        abandoned = CartRecord.fresh(CART, SHOPPER, Arm.TREATMENT).activity(1, T0, ITEMS).abandoned();
        store.put(abandoned, CartStateStore.ABSENT);
    }

    private NotificationIntent intent(int offsetIndex) {
        return new NotificationIntent(NotificationIntent.key(CART, 1, offsetIndex), CART, SHOPPER, 1,
            offsetIndex, at(min(30)), ITEMS);
    }

    @Test
    void sendsDueEntryOnceAndRemovesIt() {
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));

        dispatcher.drain();
        dispatcher.drain();

        assertEquals(1, sink.sent().size());
        assertEquals(at(min(30)), sink.sent().get(0).sentAt());
        assertEquals(0, outbox.size());
        assertEquals(1, metrics.get("dispatch.sent"));
    }

    @Test
    void doesNotSendEntriesThatAreNotYetDue() {
        outbox.add(new OutboxEntry(intent(0), 0, at(min(31))));
        dispatcher.drain();
        assertEquals(0, sink.sent().size());
        assertEquals(1, outbox.size());
    }

    @Test
    void cancelsEntryWhenCartWasPurchasedBeforeTheSend() {
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));
        store.put(abandoned.closed(2, clock.now()), 1);

        dispatcher.drain();

        assertEquals(0, sink.sent().size());
        assertEquals(0, outbox.size());
        assertEquals(1, metrics.get("dispatch.cancelled"));
    }

    @Test
    void transientFailureRetriesWithExponentialBackoffThenSucceedsOnce() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));

        dispatcher.drain();
        assertEquals(Optional.of(at(min(31))), outbox.nextDueAt());
        clock.set(at(min(31)));
        dispatcher.drain();
        assertEquals(Optional.of(at(min(33))), outbox.nextDueAt());
        clock.set(at(min(33)));
        dispatcher.drain();

        assertEquals(1, sink.sent().size());
        assertEquals(3, sink.attempts());
        assertEquals(0, outbox.size());
        assertEquals(2, metrics.get("dispatch.retry"));
    }

    @Test
    void purchaseDuringBackoffCancelsTheRetry() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));
        dispatcher.drain();
        store.put(abandoned.closed(2, clock.now()), 1);
        clock.set(at(min(31)));

        dispatcher.drain();

        assertEquals(0, sink.sent().size());
        assertEquals(0, outbox.size());
        assertEquals(1, metrics.get("dispatch.cancelled"));
    }

    @Test
    void exhaustedRetriesGoToDeadLetter() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));

        dispatcher.drain();
        clock.set(at(min(31)));
        dispatcher.drain();
        clock.set(at(min(33)));
        dispatcher.drain();

        assertEquals(0, sink.sent().size());
        assertEquals(0, outbox.size());
        assertEquals(1, dlq.size());
        assertEquals("retries_exhausted", dlq.drain().get(0).reason());
    }

    @Test
    void permanentFailureGoesStraightToDeadLetterAndReplaySendsOnce() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        outbox.add(new OutboxEntry(intent(0), 0, clock.now()));

        dispatcher.drain();
        assertEquals(1, dlq.size());
        assertEquals(1, metrics.get("dispatch.dead_lettered"));

        dispatcher.replayDeadLetters();
        dispatcher.drain();

        assertEquals(1, sink.sent().size());
        assertEquals(0, dlq.size());
        assertEquals(1, metrics.get("dispatch.replayed"));
    }
}
