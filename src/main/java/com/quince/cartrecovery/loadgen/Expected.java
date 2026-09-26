package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Computes, from each cart's script alone, the set of reminder keys ({@code cartId:version:offsetIndex},
 * the same format as the real ledger key) a correct pipeline should send: holdout carts send nothing;
 * within a cycle, offset {@code i} is expected unless something (a resume starting the next cycle, or
 * the cart's purchase on the last cycle) cancelled it at or before its due time; a cycle beyond the
 * configured frequency cap sends nothing.
 */
public final class Expected {
    private Expected() {}

    public static Set<String> keys(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) {
        Set<String> keys = new LinkedHashSet<>();
        for (CartScript script : scripts) {
            if (assigner.assign(script.shopperKey()) == Arm.HOLDOUT) continue;
            List<Cycle> cycles = script.cycles();
            for (int cycleIndex = 0; cycleIndex < cycles.size(); cycleIndex++) {
                if (cycleIndex + 1 > config.frequencyCap()) break;
                Cycle cycle = cycles.get(cycleIndex);
                keys.addAll(keysForCycle(script.cartId(), cycle, config));
            }
        }
        return keys;
    }

    private static Set<String> keysForCycle(String cartId, Cycle cycle, RecoveryConfig config) {
        Set<String> keys = new LinkedHashSet<>();
        List<Duration> offsets = config.offsets();
        for (int i = 0; i < offsets.size(); i++) {
            Instant dueAt = cycle.lastActivityAt().plus(offsets.get(i));
            if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(dueAt)) break;
            keys.add(cartId + ":" + cycle.version() + ":" + i);
        }
        return keys;
    }
}
