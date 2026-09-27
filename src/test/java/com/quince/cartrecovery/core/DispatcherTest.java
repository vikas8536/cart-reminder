package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.quince.cartrecovery.inmemory.FakeClock;
import com.quince.cartrecovery.inmemory.InMemoryCartStateStore;
import com.quince.cartrecovery.inmemory.InMemoryDeadLetterQueue;
import com.quince.cartrecovery.inmemory.InMemoryOutcomeRecorder;
import com.quince.cartrecovery.inmemory.InMemorySendLedger;
import com.quince.cartrecovery.inmemory.InMemoryWatermark;
import com.quince.cartrecovery.inmemory.RecordingNotificationSink;
import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DispatchConfig;
import com.quince.cartrecovery.model.HandleResult;
import com.quince.cartrecovery.model.Lane;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.Outcome;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.ReminderMessage;
import com.quince.cartrecovery.model.SendResult;
import com.quince.cartrecovery.model.Shards;
import com.quince.cartrecovery.ports.CartStateStore;
import com.quince.cartrecovery.ports.DeadLetterQueue;
import com.quince.cartrecovery.ports.NotificationSink;
import com.quince.cartrecovery.ports.OutcomeRecorder;
import com.quince.cartrecovery.ports.SendBudget;
import com.quince.cartrecovery.ports.SendLedger;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.DoubleSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DispatcherTest {
    private static final int SHARDS = 4;
    private static final int SHARD = Shards.of(CART, SHARDS);
    private static final String KEY = "cart-1:1:0";

    private FakeClock clock;
    private InMemoryCartStateStore store;
    private InMemorySendLedger ledger;
    private InMemoryWatermark watermark;
    private RecordingNotificationSink sink;
    private InMemoryOutcomeRecorder outcomes;
    private InMemoryDeadLetterQueue dlq;
    private Metrics metrics;
    private final List<Lane> tokens = new ArrayList<>();
    private final List<Lane> refunds = new ArrayList<>();
    private boolean tokensAvailable = true;
    private final SendBudget budget = new SendBudget() {
        @Override public boolean tryAcquire(Lane lane) {
            if (!tokensAvailable) return false;
            tokens.add(lane);
            return true;
        }
        @Override public void release(Lane lane) { refunds.add(lane); }
    };
    private Dispatcher dispatcher;

    @BeforeEach
    void setUp() {
        clock = new FakeClock(at(min(30)));
        store = new InMemoryCartStateStore(RecoveryConfig.defaults(), SHARDS);
        ledger = new InMemorySendLedger(Duration.ofSeconds(90), SHARDS);
        watermark = new InMemoryWatermark(clock);
        sink = new RecordingNotificationSink(clock);
        outcomes = new InMemoryOutcomeRecorder();
        dlq = new InMemoryDeadLetterQueue();
        metrics = new Metrics();
        dispatcher = dispatcher(RecoveryConfig.defaults().withMaxSendAttempts(3));
        CartRecord active = store.applyEvent(new CartEvent.CartEdited(CART, SHOPPER, 1, T0, ITEMS, "Ada"), Arm.TREATMENT, 0)
            .orElseThrow();
        store.markAbandoned(active, List.of(T0), true);
        moveTo(at(min(30)));
    }

    private Dispatcher dispatcher(RecoveryConfig config) {
        return dispatcher(config, store, ledger, sink, outcomes, dlq, () -> 1.0);
    }

    private Dispatcher dispatcher(RecoveryConfig config, CartStateStore s, SendLedger l, NotificationSink n,
                                  OutcomeRecorder o, DeadLetterQueue d, DoubleSupplier jitter) {
        return new Dispatcher(config, DispatchConfig.defaults(), s, l, watermark, budget, n, o, d, clock, metrics, jitter);
    }

    /** Moves the clock and marks partition 0 caught up to it. */
    private void moveTo(Instant t) {
        clock.set(t);
        watermark.publish(0, 1, t);
    }

    private static ReminderIntent intent(int offsetIndex) {
        Instant scheduled = offsetIndex == 0 ? at(min(30)) : offsetIndex == 1 ? at(hrs(1)) : at(hrs(24));
        Duration bound = offsetIndex == 2 ? min(30) : min(5);
        return new ReminderIntent(new LedgerKey(CART, 1, offsetIndex).toString(), CART, 1, offsetIndex, 0,
            scheduled, scheduled.plus(bound));
    }

    private List<OutcomeKind> outcomeKinds() {
        return outcomes.all().stream().map(Outcome::kind).toList();
    }

    @SuppressWarnings("unchecked")
    private static <T> T around(Class<T> port, T target, List<String> log, String... logged) {
        return (T) Proxy.newProxyInstance(port.getClassLoader(), new Class<?>[] {port}, (proxy, method, args) -> {
            if (List.of(logged).contains(method.getName())) log.add(method.getName());
            try {
                return method.invoke(target, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    void sendsOnceWithTheMessageBuiltFromTheCartAndFinishesSent() {
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));

        assertEquals(List.of(new RecordingNotificationSink.Sent(
            new ReminderMessage(KEY, CART, SHOPPER, "Ada", ITEMS), at(min(30)))), sink.sent());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SENT, at(min(30)), 1)), outcomes.all());
        assertEquals(1, metrics.get("dispatch.sent"));
        assertEquals(1, metrics.get("dispatch.duplicate"));
        assertEquals(List.of(Lane.FAST, Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the duplicate returns its token");
    }

    @Test
    void cancelsWhenTheCartWasPurchasedBeforeTheSend() {
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(29))), Arm.TREATMENT, 0);

        dispatcher.handle(intent(0));

        assertEquals(0, sink.attempts());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(List.of(OutcomeKind.CANCELLED), outcomeKinds());
        assertEquals(1, metrics.get("dispatch.cancelled"));
        assertEquals(List.of(Lane.FAST), refunds, "a cancel after the cart re-read returns the token");
        assertEquals(1, metrics.get("dispatch.token_taken"));
        assertEquals(1, metrics.get("dispatch.token_refunded"));
    }

    @Test
    void transientFailureRetriesWithExponentialBackoffThenSucceedsOnce() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        assertEquals(Optional.of(at(min(31))), ledger.nextRetryAt());
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(Optional.of(at(min(33))), ledger.nextRetryAt());
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(at(min(33))), sink.sent().stream().map(RecordingNotificationSink.Sent::sentAt).toList());
        assertEquals(3, sink.attempts());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
        assertEquals(3, outcomes.all().get(0).attempts());
        assertEquals(2, metrics.get("dispatch.retry"));
    }

    @Test
    void theBackoffIsAJitteredFractionOfTheExponentialCap() {
        dispatcher = dispatcher(RecoveryConfig.defaults(), store, ledger, sink, outcomes, dlq, () -> 0.25);
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));

        assertEquals(Optional.of(at(min(30)).plusSeconds(15)), ledger.nextRetryAt());
    }

    @Test
    void purchaseDuringBackoffCancelsTheRetry() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));
        store.applyEvent(new CartEvent.CartPurchased(CART, SHOPPER, 2, at(min(30))), Arm.TREATMENT, 0);
        moveTo(at(min(31)));

        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("CANCELLED"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.cancelled"));
        assertEquals(List.of(Lane.FAST, Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the cancelled retry returns its token");
    }

    @Test
    void exhaustedRetriesAreDeadLetteredWithTheOriginalIntent() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("DEAD"), ledger.status(KEY));
        assertEquals(List.of(new DeadLetter(intent(0), "retries_exhausted", at(min(33)))), dlq.drain());
        assertEquals(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.DEAD, at(min(33)), 3), outcomes.all().get(0));
    }

    @Test
    void permanentFailureIsDeadLetteredAndReplaySendsOnce() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        List<DeadLetter> letters = dlq.drain();
        assertEquals(List.of(new DeadLetter(intent(0), "permanent_failure", at(min(30)))), letters);
        assertEquals(1, metrics.get("dispatch.dead_lettered"));

        moveTo(at(min(32)));
        dispatcher.replay(letters);
        dispatcher.retryDue(SHARD, 10);
        dispatcher.replay(letters);
        dispatcher.retryDue(SHARD, 10);

        assertEquals(1, sink.sent().size());
        assertEquals(at(min(32)), sink.sent().get(0).sentAt());
        assertEquals(1, metrics.get("dispatch.replayed"));
        assertEquals(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SENT, at(min(32)), 1), outcomes.all().get(1));
    }

    @Test
    void replaySkipsPoisonLetters() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        dlq.drain();

        dispatcher.replay(List.of(new DeadLetter(intent(0), DeadLetter.REASON_POISON, at(min(30)))));

        assertEquals(Optional.of("DEAD"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.replay_poison_skipped"));
        assertEquals(0, metrics.get("dispatch.replayed"));
    }

    @Test
    void aReplayPastSendByIsSkippedAsLate() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        dispatcher.handle(intent(0));
        moveTo(at(min(36)));

        dispatcher.replay(dlq.drain());
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.replayed"));
        assertEquals(1, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void aRetryLandingAfterSendByIsSkippedNotSent() {
        dispatcher = dispatcher(RecoveryConfig.defaults());
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE, SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        moveTo(at(min(33)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(Optional.of(at(min(37))), ledger.nextRetryAt());
        moveTo(at(min(37)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(0, sink.sent().size());
        assertEquals(3, sink.attempts());
        assertEquals(0, dlq.size());
        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(1, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void aLateRetryIsSkippedWithoutATokenOrTheGate() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));                  // retry due at 31m, sendBy 35m
        clock.set(at(min(36)));                        // late, and partition 0's watermark is now stale too

        dispatcher.retryDue(SHARD, 10);

        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(List.of(Lane.FAST), tokens, "only the first attempt took a token");
        assertEquals(List.of(), refunds);
        assertEquals(0, metrics.get("dispatch.retry_held"));
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, at(min(36)), 2)),
            outcomes.all());
    }

    @Test
    void sendByExactlyNowIsNotLate() {
        moveTo(at(min(35)));

        dispatcher.handle(intent(0));

        assertEquals(1, sink.sent().size());
        assertEquals(0, metrics.get("dispatch.skipped_late_precheck"));
        assertEquals(0, metrics.get("dispatch.skipped_late"));
    }

    @Test
    void aLateIntentIsSkippedAtThePreCheckWithoutATokenOrALedgerRow() {
        moveTo(at(min(35)).plusMillis(1));

        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));

        assertEquals(List.of(), tokens);
        assertEquals(0, ledger.size());
        assertEquals(List.of(new Outcome(KEY, CART, 1, Arm.TREATMENT, OutcomeKind.SKIPPED_LATE, clock.now(), 0)),
            outcomes.all());
        assertEquals(1, metrics.get("dispatch.skipped_late_precheck"));
    }

    @Test
    void noTokenHoldsTheIntentWithoutClaiming() {
        tokensAvailable = false;

        assertEquals(HandleResult.HOLD, dispatcher.handle(intent(0)));
        assertEquals(0, ledger.size());
        assertEquals(1, metrics.get("dispatch.no_token"));

        tokensAvailable = true;
        assertEquals(HandleResult.DONE, dispatcher.handle(intent(0)));
        assertEquals(1, sink.sent().size());
    }

    @Test
    void aLaggingPartitionHoldsTheIntentAfterTakingTheTokenAndBeforeTheClaim() {
        clock.set(at(min(30)).plusSeconds(6));

        assertEquals(HandleResult.HOLD, dispatcher.handle(intent(0)));

        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(List.of(Lane.FAST), refunds, "the gate hold returns the token");
        assertEquals(0, ledger.size());
        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.held"));
    }

    @Test
    void theGateReadsTheIntentsRecordedPartition() {
        clock.set(at(min(30)).plusSeconds(6));
        watermark.publish(7, 1, clock.now());
        ReminderIntent onSeven = new ReminderIntent(KEY, CART, 1, 0, 7, at(min(30)), at(min(35)));

        assertEquals(HandleResult.DONE, dispatcher.handle(onSeven));
        assertEquals(1, sink.sent().size());
    }

    @Test
    void theLaneFollowsFastOffsetsOnBothPaths() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(1));
        dispatcher.handle(intent(2));
        moveTo(at(min(31)));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(Lane.FAST, Lane.SLOW, Lane.FAST), tokens);
    }

    @Test
    void theRetryLoopSkipsALaggingPartitionOrAMissingTokenAndLeavesTheRowDue() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);
        dispatcher.handle(intent(0));

        clock.set(at(min(31)));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, metrics.get("dispatch.retry_held"));
        assertEquals(Optional.of("RETRYING"), ledger.status(KEY));

        moveTo(at(min(31)));
        tokensAvailable = false;
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, metrics.get("dispatch.retry_no_token"));
        assertEquals(Optional.of("RETRYING"), ledger.status(KEY));

        tokensAvailable = true;
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, sink.sent().size());
        assertEquals(2, outcomes.all().get(0).attempts());
    }

    @Test
    void theLeaseCheckStopsASendWithLessThanTheGatewayTimeoutLeftAndTheRetryLoopTakesOverAtExactlyLeaseUntil() {
        CartStateStore slow = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("get")) clock.advance(Duration.ofSeconds(61));
                return method.invoke(store, args);
            });

        dispatcher(RecoveryConfig.defaults(), slow, ledger, sink, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(0, sink.attempts());
        assertEquals(1, metrics.get("dispatch.lease_expiring"));
        assertEquals(List.of(Lane.FAST), refunds, "a lease too short to send returns the token");
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        Instant leaseUntil = at(min(30)).plusSeconds(90);
        assertEquals(Optional.of(leaseUntil), ledger.nextRetryAt());

        moveTo(leaseUntil.minusMillis(1));
        dispatcher.retryDue(SHARD, 10);
        assertEquals(0, sink.attempts());

        moveTo(leaseUntil);
        dispatcher.retryDue(SHARD, 10);
        assertEquals(1, sink.sent().size());
        assertEquals(2, outcomes.all().get(0).attempts());
    }

    @Test
    void aHolderWhoseLeaseWasTakenOverDuringTheSendCannotFinish() {
        NotificationSink slowGateway = message -> {
            clock.advance(Duration.ofSeconds(91));
            ledger.claim(message.key(), at(min(35)), 0, clock.now());
            return SendResult.SENT;
        };

        dispatcher(RecoveryConfig.defaults(), store, ledger, slowGateway, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(1, metrics.get("dispatch.lease_lost"));
        assertEquals(List.of(OutcomeKind.SENT), outcomeKinds());
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
    }

    @Test
    void outcomesAndDeadLettersAreProducedBeforeTheLedgerFinish() {
        List<String> log = new ArrayList<>();
        SendLedger logged = around(SendLedger.class, ledger, log, "finish");
        OutcomeRecorder recorder = o -> log.add("outcome:" + o.kind());
        DeadLetterQueue letters = l -> log.add("dlq");
        sink.scriptOutcomes(SendResult.SENT, SendResult.PERMANENT_FAILURE);
        Dispatcher d = dispatcher(RecoveryConfig.defaults(), store, logged, sink, recorder, letters, () -> 1.0);

        d.handle(intent(0));
        d.handle(intent(1));

        assertEquals(List.of("outcome:SENT", "finish", "dlq", "outcome:DEAD", "finish"), log);
    }

    @Test
    void aCrashWhileProducingTheSentOutcomeLeavesTheRowForAResendUnderTheSameKey() {
        OutcomeRecorder down = o -> { throw new IllegalStateException("broker unavailable"); };

        assertThrows(IllegalStateException.class, () ->
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, down, dlq, () -> 1.0).handle(intent(0)));
        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        assertEquals(List.of(), refunds, "a token is never returned after a send");

        moveTo(at(min(30)).plusSeconds(90));
        dispatcher.retryDue(SHARD, 10);

        assertEquals(List.of(KEY, KEY), sink.sent().stream().map(s -> s.message().key()).toList());
        assertEquals(Optional.of("SENT"), ledger.status(KEY));
    }

    @Test
    void aCrashBeforeTheDeadLetterIsProducedNeverLeavesAnUnreplayableDeadRow() {
        sink.scriptOutcomes(SendResult.PERMANENT_FAILURE);
        DeadLetterQueue down = l -> { throw new IllegalStateException("broker unavailable"); };

        assertThrows(IllegalStateException.class, () ->
            dispatcher(RecoveryConfig.defaults(), store, ledger, sink, outcomes, down, () -> 1.0).handle(intent(0)));

        assertEquals(Optional.of("SENDING"), ledger.status(KEY));
        assertEquals(List.of(), outcomes.all());
    }

    @Test
    void aReminderFoundLateAfterTheCartReadReturnsTheToken() {
        CartStateStore slowRead = (CartStateStore) Proxy.newProxyInstance(CartStateStore.class.getClassLoader(),
            new Class<?>[] {CartStateStore.class}, (proxy, method, args) -> {
                if (method.getName().equals("get")) clock.advance(Duration.ofMinutes(6));
                return method.invoke(store, args);
            });

        dispatcher(RecoveryConfig.defaults(), slowRead, ledger, sink, outcomes, dlq, () -> 1.0).handle(intent(0));

        assertEquals(Optional.of("SKIPPED_LATE"), ledger.status(KEY));
        assertEquals(List.of(Lane.FAST), refunds);
    }

    @Test
    void aFailedSendKeepsItsToken() {
        sink.scriptOutcomes(SendResult.TRANSIENT_FAILURE);

        dispatcher.handle(intent(0));

        assertEquals(List.of(Lane.FAST), tokens);
        assertEquals(List.of(), refunds);
    }
}
