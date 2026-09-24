package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ReminderPolicyTest {
    private ReminderPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new ReminderPolicy(RecoveryConfig.defaults());
    }

    @Test
    void holdoutArmIsNeverEligible() {
        CartRecord record = CartRecord.fresh(CART, SHOPPER, Arm.HOLDOUT);
        assertFalse(policy.eligible(record, T0));
    }

    @Test
    void treatmentArmWithNoSequenceStartsIsEligible() {
        CartRecord record = CartRecord.fresh(CART, SHOPPER, Arm.TREATMENT);
        assertTrue(policy.eligible(record, T0));
    }

    @Test
    void capOfThreeAllowsExactlyThreeSequencesWithinWindow() {
        // Build a record with exactly 3 sequences within the 7-day window (cap is 3)
        CartRecord record = CartRecord.fresh(CART, SHOPPER, Arm.TREATMENT)
            .activity(1, at(min(0)), ITEMS)
            .abandoned()   // 1st sequence start at T0
            .activity(2, at(hrs(24)), ITEMS)
            .abandoned()   // 2nd sequence start at T0+24h
            .activity(3, at(hrs(48)), ITEMS)
            .abandoned();  // 3rd sequence start at T0+48h

        assertTrue(policy.eligible(record, at(hrs(50))));
    }

    @Test
    void fourSequencesWithinWindowExceedsCapAndIsNotEligible() {
        // Build a record with 4 sequences within the 7-day window (exceeds cap of 3)
        CartRecord record = CartRecord.fresh(CART, SHOPPER, Arm.TREATMENT)
            .activity(1, at(min(0)), ITEMS)
            .abandoned()   // 1st sequence start at T0
            .activity(2, at(hrs(24)), ITEMS)
            .abandoned()   // 2nd sequence start at T0+24h
            .activity(3, at(hrs(48)), ITEMS)
            .abandoned()   // 3rd sequence start at T0+48h
            .activity(4, at(hrs(72)), ITEMS)
            .abandoned();  // 4th sequence start at T0+72h

        assertFalse(policy.eligible(record, at(hrs(74))));
    }

    @Test
    void excludedJustOutsideWindow() {
        // Window is 7 days (168 hours). Default cap is 3.
        // now = T0, windowStart = T0 - 168h, so starts must be >= windowStart to count.
        // starts: [T0 - 169h (outside), T0 - 48h (in), T0 - 24h (in), T0 (in)]
        // Recent = 3, exactly at cap → eligible TRUE
        Instant now = T0;
        CartRecord record = new CartRecord(
            CART, SHOPPER,
            com.quince.cartrecovery.model.CartStatus.ABANDONED,
            4L,
            now,
            ITEMS,
            Arm.TREATMENT,
            List.of(
                now.minus(Duration.ofHours(169)), // just outside 7-day window
                now.minus(Duration.ofHours(48)),  // within window
                now.minus(Duration.ofHours(24)),  // within window
                now                               // within window
            )
        );

        assertTrue(policy.eligible(record, now));
    }

    @Test
    void includedAtWindowBoundary() {
        // Window is 7 days (168 hours). Default cap is 3.
        // now = T0, windowStart = T0 - 168h, so starts must be >= windowStart to count.
        // starts: [T0 - 168h (at boundary, IN), T0 - 48h (in), T0 - 24h (in), T0 (in)]
        // Recent = 4, exceeds cap of 3 → eligible FALSE
        // (If filter were inverted, recent = 0, would be TRUE, failing this test)
        Instant now = T0;
        CartRecord record = new CartRecord(
            CART, SHOPPER,
            com.quince.cartrecovery.model.CartStatus.ABANDONED,
            4L,
            now,
            ITEMS,
            Arm.TREATMENT,
            List.of(
                now.minus(Duration.ofHours(168)), // exactly at 7-day window boundary
                now.minus(Duration.ofHours(48)),  // within window
                now.minus(Duration.ofHours(24)),  // within window
                now                               // within window
            )
        );

        assertFalse(policy.eligible(record, now));
    }
}
