package com.quince.cartrecovery.contract;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.quince.cartrecovery.ports.Watermark;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Behaviour every Watermark must share. Staleness is measured on the watermark's own time source, so a subclass
 * supplies advance(); a real-time subclass may sleep. The factory returns a watermark with no partitions written
 * (a Redis subclass flushes first and reads partitions 0 to 2, the ones this contract writes, for current(-1)).
 * Millisecond-exact time boundaries (for example staleness starting strictly after 5 s) are tested only in the
 * in-memory subclass, never here (controller ruling R3); infra subclasses add none.
 */
public abstract class WatermarkContract {
    private static final Instant E = Instant.parse("2026-01-01T09:00:00Z");

    protected Watermark watermark;

    protected abstract Watermark newWatermark();

    /** Moves the watermark's time source forward by at least d. */
    protected abstract void advance(Duration d);

    @BeforeEach
    void createWatermark() {
        watermark = newWatermark();
    }

    @Test
    void anUnknownPartitionReadsAsEpoch() {
        assertEquals(Instant.EPOCH, watermark.current(0));
        assertEquals(Instant.EPOCH, watermark.current(-1));
    }

    @Test
    void aPublishedValueIsCurrent() {
        watermark.publish(0, 1, E);
        assertEquals(E, watermark.current(0));
        assertEquals(Instant.EPOCH, watermark.current(1));
    }

    @Test
    void theSameGenerationKeepsTheMaximum() {
        watermark.publish(0, 4, E.plusSeconds(10));
        watermark.publish(0, 4, E);
        assertEquals(E.plusSeconds(10), watermark.current(0));

        watermark.publish(0, 4, E.plusSeconds(11));
        assertEquals(E.plusSeconds(11), watermark.current(0));
    }

    @Test
    void aLowerGenerationIsRejected() {
        watermark.publish(0, 5, E);
        watermark.publish(0, 4, E.plusSeconds(60));
        assertEquals(E, watermark.current(0));
    }

    @Test
    void aHigherGenerationOverwritesEvenWithAnEarlierTime() {
        watermark.publish(0, 4, E.plusSeconds(60));
        watermark.publish(0, 5, E);
        assertEquals(E, watermark.current(0));
    }

    @Test
    void minusOneReadsTheMinimumOverAllPartitions() {
        watermark.publish(0, 1, E.plusSeconds(10));
        watermark.publish(1, 1, E.plusSeconds(5));
        watermark.publish(2, 1, E.plusSeconds(20));
        assertEquals(E.plusSeconds(5), watermark.current(-1));
    }

    @Test
    void aPartitionNotWrittenForMoreThanFiveSecondsReadsAsEpoch() {
        watermark.publish(0, 1, E);
        watermark.publish(1, 1, E);

        advance(Duration.ofSeconds(6));
        watermark.publish(1, 1, E.plusSeconds(1));

        assertEquals(Instant.EPOCH, watermark.current(0));
        assertEquals(E.plusSeconds(1), watermark.current(1));
        assertEquals(Instant.EPOCH, watermark.current(-1));

        watermark.publish(0, 1, E.plusSeconds(2));
        assertEquals(E.plusSeconds(2), watermark.current(0));
    }
}
