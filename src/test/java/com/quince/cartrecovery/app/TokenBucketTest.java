package com.quince.cartrecovery.app;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.quince.cartrecovery.model.Lane;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TokenBucketTest {
    private final AtomicLong nanos = new AtomicLong();
    private final TokenBucket bucket = new TokenBucket(10, 0.3, nanos::get);   // capacity 10, reserve 3

    @Test
    void slowLaneStopsAtTheFastReserveAndFastTakesTheRest() {
        int slow = 0;
        while (bucket.tryAcquire(Lane.SLOW)) slow++;
        assertEquals(7, slow);
        assertFalse(bucket.slowAllowed());
        assertTrue(bucket.anyAvailable());

        int fast = 0;
        while (bucket.tryAcquire(Lane.FAST)) fast++;
        assertEquals(3, fast);
        assertFalse(bucket.anyAvailable());
        assertFalse(bucket.slowAllowed());
    }

    @Test
    void refillsAtTheRateUpToOneSecondOfCapacity() {
        while (bucket.tryAcquire(Lane.FAST)) { }
        nanos.addAndGet(100_000_000L);                 // 100 ms at 10/s = 1 token
        assertTrue(bucket.tryAcquire(Lane.FAST));
        assertFalse(bucket.tryAcquire(Lane.FAST));

        nanos.addAndGet(60_000_000_000L);              // a minute idle still caps at 10
        int n = 0;
        while (bucket.tryAcquire(Lane.FAST)) n++;
        assertEquals(10, n);
    }

    @Test
    void slowLaneResumesOnlyAboveTheReserve() {
        while (bucket.tryAcquire(Lane.FAST)) { }
        nanos.addAndGet(300_000_000L);                 // 3 tokens: exactly the reserve
        assertTrue(bucket.anyAvailable());
        assertFalse(bucket.slowAllowed());
        assertFalse(bucket.tryAcquire(Lane.SLOW));

        nanos.addAndGet(100_000_000L);                 // 4 tokens
        assertTrue(bucket.slowAllowed());
        assertTrue(bucket.tryAcquire(Lane.SLOW));
    }

    @Test
    void releaseNeverRaisesTheBucketAboveCapacity() {
        bucket.release(Lane.FAST);                     // full bucket: stays at 10
        int n = 0;
        while (bucket.tryAcquire(Lane.FAST)) n++;
        assertEquals(10, n);

        bucket.release(Lane.FAST);
        assertTrue(bucket.tryAcquire(Lane.FAST));
        assertFalse(bucket.tryAcquire(Lane.FAST));
    }

    @Test
    void rejectsInvalidSettings() {
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(0, 0.3, nanos::get));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(10, 1.0, nanos::get));
        assertThrows(IllegalArgumentException.class, () -> new TokenBucket(10, -0.1, nanos::get));
    }
}
