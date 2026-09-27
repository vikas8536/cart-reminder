package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
        return new LinkedHashSet<>(sendBy(scripts, config, assigner).keySet());
    }

    /** Every expected key with its nominal sendBy: the offset's scheduled time plus its lateness bound. */
    public static Map<String, Instant> sendBy(List<CartScript> scripts, RecoveryConfig config, ArmAssigner assigner) {
        Map<String, Instant> out = new LinkedHashMap<>();
        for (CartScript script : scripts) {
            if (assigner.assign(script.shopperKey()) == Arm.HOLDOUT) continue;
            List<Cycle> cycles = script.cycles();
            for (int cycleIndex = 0; cycleIndex < cycles.size() && cycleIndex < config.frequencyCap(); cycleIndex++) {
                Cycle cycle = cycles.get(cycleIndex);
                for (int i = 0; i < config.offsets().size(); i++) {
                    Instant dueAt = cycle.lastActivityAt().plus(config.offsets().get(i));
                    if (cycle.cancelledAt() != null && !cycle.cancelledAt().isAfter(dueAt)) break;
                    out.put(script.cartId() + ":" + cycle.version() + ":" + i, dueAt.plus(config.latenessBounds().get(i)));
                }
            }
        }
        return out;
    }
}
