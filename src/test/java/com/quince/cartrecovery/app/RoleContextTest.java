package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.infra.kafka.Topics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RoleContextTest {
    static Map<String, Integer> topics(int partitions) {
        Map<String, Integer> m = new HashMap<>();
        for (String t : Topics.ALL) m.put(t, partitions);
        return m;
    }

    @Test void matchingSetupHasNoProblem() {
        assertEquals(Optional.empty(), RoleContext.startupProblem(8, 8, 8, 8, topics(8)));
    }
    @Test void missingMetaAsksForInit() {
        assertTrue(RoleContext.startupProblem(8, 8, null, null, topics(8)).orElseThrow().contains("--role=init"));
    }
    @Test void shardMismatchRefuses() {
        assertTrue(RoleContext.startupProblem(4, 8, 8, 8, topics(8)).orElseThrow().contains("SHARDS=4"));
    }
    @Test void partitionMismatchRefuses() {
        assertTrue(RoleContext.startupProblem(8, 4, 8, 8, topics(8)).orElseThrow().contains("PARTITIONS=4"));
    }
    @Test void topicPartitionMismatchRefuses() {
        Map<String, Integer> t = topics(8);
        t.put(Topics.INTENTS_SLOW, 4);
        assertTrue(RoleContext.startupProblem(8, 8, 8, 8, t).orElseThrow().contains(Topics.INTENTS_SLOW));
    }
    @Test void missingTopicRefuses() {
        Map<String, Integer> t = topics(8);
        t.remove(Topics.CART_EVENTS);
        assertTrue(RoleContext.startupProblem(8, 8, 8, 8, t).orElseThrow().contains("missing"));
    }
}
