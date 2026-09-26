package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.Await;
import com.quince.cartrecovery.infra.dynamo.RecoveryMetaStore;
import com.quince.cartrecovery.infra.kafka.JsonCodec;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.infra.redis.RedisWatermark;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartItem;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.ReminderIntent;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class DispatcherRoleIT {
    static final Duration WAIT = Duration.ofSeconds(25);
    static final List<CartItem> ITEMS = List.of(new CartItem("SKU-1", "Linen Shirt", 1, 4990));

    @BeforeAll
    static void infra() { RoleInfra.start(); }

    /**
     * The detector reads Redis time T before a poll and publishes it after the poll returns, so an idle
     * partition's watermark trails Redis time by one idle poll (500 ms) plus overhead: measured 520 to 1010 ms.
     * RoleInfra's demo CLOCK_SKEW of 0.5 s sits below that floor and the dispatcher gate would never open.
     */
    static InfraConfig config() {
        return RoleInfra.config(Map.of("CLOCK_SKEW", "PT2S"));
    }

    static List<String> sentKeys(TopicTail sends, String prefix) {
        return sends.records(prefix).stream().map(r -> JsonCodec.decodeSinkSend(r.value()).key()).toList();
    }

    @Test
    void sendsTheFirstReminderAndShowsBreakerAndPauseOnReady() throws Exception {
        InfraConfig c = config();
        String cart = RoleInfra.prefix("disp") + "cart";
        int port;
        try (ServerSocket s = new ServerSocket(0)) { port = s.getLocalPort(); }
        try (TopicTail sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c);
             RoleThread dispatcher = new RoleThread(new DispatcherRole(), c);
             HealthServer server = new HealthServer(port, dispatcher.health(), dispatcher.metrics(), Duration.ofSeconds(15))) {
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, Instant.now(), ITEMS, "Ada"));
            Await.until(() -> sentKeys(sends, cart).contains(new LedgerKey(cart, 1, 0).toString()), WAIT);
            String ready = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/ready")).build(),
                HttpResponse.BodyHandlers.ofString()).body();
            assertTrue(ready.contains("breaker"), ready);
            assertTrue(ready.contains("paused"), ready);
            assertNull(dispatcher.failure());
        }
    }

    @Test
    void guardrailPauseStopsAndResumesSending() throws Exception {
        InfraConfig c = config();
        String cart = RoleInfra.prefix("pause") + "cart";
        String firstKey = new LedgerKey(cart, 1, 0).toString();
        RecoveryMetaStore meta = new RecoveryMetaStore(RoleInfra.ctx().dynamo());
        meta.setPaused(true);
        try (TopicTail sends = new TopicTail(RoleInfra.bootstrap(), Topics.SINK_SENDS);
             TopicTail fast = new TopicTail(RoleInfra.bootstrap(), Topics.INTENTS_FAST);
             RoleThread detector = new RoleThread(new DetectorRole(), c);
             RoleThread scheduler = new RoleThread(new SchedulerRole(), c);
             RoleThread dispatcher = new RoleThread(new DispatcherRole(), c)) {
            Instant t0 = Instant.now();
            RoleInfra.produce(new CartEvent.CartEdited(cart, "shopper", 1, t0, ITEMS, "Ada"));
            // The intent must be on the fast lane, so the silence below is the pause and not a slow scheduler.
            Await.until(() -> fast.records(cart).stream().anyMatch(r -> JsonCodec.decodeIntent(r.value()).key().equals(firstKey)), WAIT);
            Instant quietUntil = Instant.now().plusSeconds(2);
            if (quietUntil.isBefore(t0.plusSeconds(7))) quietUntil = t0.plusSeconds(7);   // reminder 0 due at +3 s
            Thread.sleep(Duration.between(Instant.now(), quietUntil).toMillis());
            assertEquals(List.of(), sentKeys(sends, cart));
            meta.setPaused(false);
            Await.until(() -> sentKeys(sends, cart).contains(firstKey), WAIT);
            assertNull(dispatcher.failure());
        } finally {
            meta.setPaused(false);
        }
    }

    /**
     * Controller ruling F4 (spec §5.4, §6.2): a partition held by the watermark gate stays paused until its
     * source partition catches up, instead of being re-polled every poll timeout and taking a send token each
     * time (the Dispatcher takes the token before the gate, so each dispatch.held is one token spent).
     */
    @Test
    void gateHeldPartitionStaysPausedAndSpendsNoTokensUntilCaughtUp() throws Exception {
        InfraConfig c = config();
        String cart = RoleInfra.prefix("held") + "cart";
        int src = 99;   // a source partition no detector owns: its watermark reads EPOCH until published below
        Instant now = Instant.now();
        ReminderIntent intent = new ReminderIntent(new LedgerKey(cart, 1, 0).toString(), cart, 1, 0, src,
            now, now.plusSeconds(60));
        RedisWatermark watermark = new RedisWatermark(RoleInfra.ctx().redis(), c.partitions());
        try (RoleThread dispatcher = new RoleThread(new DispatcherRole(), c)) {
            RoleInfra.send(Topics.INTENTS_FAST, cart, JsonCodec.encode(intent));
            Await.until(() -> dispatcher.metrics().get("dispatch.held") >= 1, WAIT);
            Thread.sleep(3000);   // six poll timeouts: an unpaused partition would be redelivered about six times
            assertEquals(1, dispatcher.metrics().get("dispatch.held"), "held partition kept consuming tokens");
            // Catch the source partition up: the partition resumes and the intent completes (no cart, so cancelled).
            Await.until(() -> {
                watermark.publish(src, 1, watermark.now());
                return dispatcher.metrics().get("dispatch.cancelled") >= 1;
            }, WAIT);
            assertNull(dispatcher.failure());
        }
    }
}
