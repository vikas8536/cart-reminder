package com.quince.cartrecovery.app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.infra.dynamo.DynamoSendLedger;
import com.quince.cartrecovery.infra.dynamo.DynamoTables;
import com.quince.cartrecovery.infra.kafka.KafkaDeadLetterQueue;
import com.quince.cartrecovery.infra.kafka.Topics;
import com.quince.cartrecovery.model.ClaimResult;
import com.quince.cartrecovery.model.DeadLetter;
import com.quince.cartrecovery.model.DueRetry;
import com.quince.cartrecovery.model.LedgerKey;
import com.quince.cartrecovery.model.OutcomeKind;
import com.quince.cartrecovery.model.ReminderIntent;
import com.quince.cartrecovery.model.Shards;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers(disabledWithoutDocker = true)
class ReplayRoleIT {
    @BeforeAll
    static void infra() { RoleInfra.start(); }

    static ReminderIntent intent(String cartId, Instant now) {
        return new ReminderIntent(new LedgerKey(cartId, 1, 0).toString(), cartId, 1, 0, 0, now, now.plus(Duration.ofMinutes(10)));
    }

    static Set<String> dueKeys(DynamoSendLedger ledger, String cartId, int shards) {
        return ledger.dueRetries(Shards.of(cartId, shards), Instant.now().plusSeconds(1), 1000).stream()
            .map(DueRetry::key).collect(Collectors.toSet());
    }

    @Test
    void reopensDeadRowsAndSkipsPoisonAndUndecodableRecords() throws Exception {
        InfraConfig c = RoleInfra.config(Map.of());
        DynamoSendLedger ledger = new DynamoSendLedger(RoleInfra.ctx().dynamo(), DynamoTables.SEND_LEDGER, c.dispatch().lease(), c.shards());
        KafkaDeadLetterQueue dlq = new KafkaDeadLetterQueue(RoleInfra.ctx().producer());
        String prefix = RoleInfra.prefix("replay");
        Instant now = Instant.now();
        ReminderIntent dead = intent(prefix + "dead", now);
        ReminderIntent poison = intent(prefix + "poison", now);
        for (ReminderIntent i : List.of(dead, poison)) {
            ClaimResult.Claimed claim = (ClaimResult.Claimed) ledger.claim(i.key(), i.sendBy(), i.srcPartition(), now);
            assertTrue(ledger.finish(i.key(), claim.token(), OutcomeKind.DEAD, "permanent"));
        }
        dlq.add(new DeadLetter(dead, "permanent", now));
        dlq.add(new DeadLetter(poison, DeadLetter.REASON_POISON, now));
        RoleInfra.send(Topics.REMINDER_DLQ, prefix + "garbage", "not json".getBytes(UTF_8));

        RoleThread replay = new RoleThread(new ReplayRole(), c);
        assertTrue(replay.join(Duration.ofSeconds(25)), "replay is a one-off and returns when caught up");
        assertNull(replay.failure());

        assertTrue(dueKeys(ledger, dead.cartId(), c.shards()).contains(dead.key()));
        assertFalse(dueKeys(ledger, poison.cartId(), c.shards()).contains(poison.key()));
    }
}
