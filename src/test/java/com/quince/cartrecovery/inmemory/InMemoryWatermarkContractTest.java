package com.quince.cartrecovery.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.contract.WatermarkContract;
import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class InMemoryWatermarkContractTest extends WatermarkContract {
    private final FakeClock clock = new FakeClock(Instant.parse("2026-01-01T09:00:00Z"));

    @Override protected Watermark newWatermark() { return new InMemoryWatermark(clock); }
    @Override protected void advance(Duration d) { clock.advance(d); }

    @Test
    void stalenessStartsStrictlyAfterFiveSeconds() {
        watermark.publish(0, 1, clock.now());
        Instant written = clock.now();

        clock.advance(Duration.ofSeconds(5));
        assertEquals(written, watermark.current(0));
        clock.advance(Duration.ofMillis(1));
        assertEquals(Instant.EPOCH, watermark.current(0));
    }

    @Test
    void setLaggingPinsAPartitionUntilClearLag() {
        InMemoryWatermark w = (InMemoryWatermark) watermark;
        Instant stalledAt = clock.now();
        w.setLagging(0, stalledAt);
        clock.advance(Duration.ofMinutes(10));
        w.publish(0, 1, clock.now());
        w.publish(1, 1, clock.now());

        assertEquals(stalledAt, w.current(0));
        assertEquals(clock.now(), w.current(1));
        assertEquals(stalledAt, w.current(-1));

        w.clearLag();
        assertEquals(clock.now(), w.current(0));
    }

    @Test
    void nowIsTheClock() {
        assertEquals(clock.now(), watermark.now());
    }
}
