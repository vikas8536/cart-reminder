package com.quince.cartrecovery.core;

import static com.quince.cartrecovery.TestSupport.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartRecord;
import com.quince.cartrecovery.model.RecoveryConfig;
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
    void oldSequenceStartsOutsideWindowDoNotCountTowardsCap() {
        // Build a record with 3 old starts (> 7 days ago) and 1 recent (within window)
        Instant eightDaysAgo = at(hrs(-192)); // 8 days before T0
        Instant sevenDaysAgo = at(hrs(-168)); // 7 days before T0 (still outside window from `now`)
        Instant threeDaysAgo = at(hrs(-72)); // 3 days before T0

        // Use canonical constructor to create record with explicit sequenceStarts
        CartRecord record = new CartRecord(
            CART, SHOPPER,
            com.quince.cartrecovery.model.CartStatus.ABANDONED,
            4L,
            at(min(0)), // lastActivityAt is current (recently abandoned)
            ITEMS,
            Arm.TREATMENT,
            List.of(eightDaysAgo, sevenDaysAgo, threeDaysAgo, at(min(0))) // 4 starts total
        );

        Instant now = at(hrs(0));
        // Window is 7 days, so starts before (now - 7 days) = (T0 - 7 days) don't count
        // eightDaysAgo and sevenDaysAgo are outside, only threeDaysAgo and current count = 2
        assertTrue(policy.eligible(record, now));
    }
}
