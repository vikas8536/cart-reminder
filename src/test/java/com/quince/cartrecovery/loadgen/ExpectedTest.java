package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.ArmAssigner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExpectedTest {
    private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
    private static final RecoveryConfig CONFIG = RecoveryConfig.defaults(); // offsets 30m, 1h, 24h

    private static final ArmAssigner ALL_TREATMENT = shopperKey -> Arm.TREATMENT;
    private static final ArmAssigner ALL_HOLDOUT = shopperKey -> Arm.HOLDOUT;

    @Test void aCartThatNeverPurchasesGetsEveryOffset() {
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0", "cart-1:1:1", "cart-1:1:2"), keys);
    }

    @Test void holdoutCartsGetNoKeysAtAll() {
        Cycle cycle = new Cycle(1L, T0, null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_HOLDOUT);

        assertTrue(keys.isEmpty());
    }

    @Test void aPurchaseBeforeTheFirstOffsetIsDueCancelsEverything() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0).dividedBy(2));
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertTrue(keys.isEmpty());
    }

    @Test void aPurchaseBetweenTheFirstAndSecondOffsetLeavesOnlyTheFirstExpected() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0)).plusSeconds(1);
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0"), keys);
    }

    @Test void aPurchaseExactlyAtTheDueInstantCancelsThatOffsetToo() {
        Instant purchaseAt = T0.plus(CONFIG.offsets().get(0));
        Cycle cycle = new Cycle(1L, T0, purchaseAt);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(cycle));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertTrue(keys.isEmpty());
    }

    @Test void aResumeCancelsOnlyTheEarlierCycleNotTheNewOne() {
        Instant resumeAt = T0.plus(CONFIG.offsets().get(0)).plusSeconds(1);
        Cycle first = new Cycle(1L, T0, resumeAt);
        Cycle second = new Cycle(2L, resumeAt.plusSeconds(1), null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(first, second));

        Set<String> keys = Expected.keys(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Set.of("cart-1:1:0", "cart-1:2:0", "cart-1:2:1", "cart-1:2:2"), keys);
    }

    @Test void aCycleBeyondTheFrequencyCapSendsNothing() {
        RecoveryConfig capOfOne = CONFIG.withFrequencyCap(1);
        Cycle first = new Cycle(1L, T0, T0.plusSeconds(1));
        Cycle second = new Cycle(2L, T0.plusSeconds(2), null);
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(first, second));

        Set<String> keys = Expected.keys(List.of(script), capOfOne, ALL_TREATMENT);

        assertTrue(keys.stream().noneMatch(k -> k.startsWith("cart-1:2:")));
    }

    @Test void sendByIsEachExpectedKeysDueTimePlusItsLatenessBound() {
        CartScript script = new CartScript("cart-1", "shopper-1", List.of(new Cycle(1L, T0, null)));

        Map<String, Instant> sendBy = Expected.sendBy(List.of(script), CONFIG, ALL_TREATMENT);

        assertEquals(Map.of(
            "cart-1:1:0", T0.plus(Duration.ofMinutes(35)),
            "cart-1:1:1", T0.plus(Duration.ofMinutes(65)),
            "cart-1:1:2", T0.plus(Duration.ofHours(24)).plus(Duration.ofMinutes(30))), sendBy);
        assertEquals(sendBy.keySet(), Expected.keys(List.of(script), CONFIG, ALL_TREATMENT));
    }
}
