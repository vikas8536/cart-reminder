package com.quince.cartrecovery.loadgen;

import com.quince.cartrecovery.model.RecoveryConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Generates a deterministic (seeded) simulated shopper workload: about 10 events per cart
 * session, about 70% of carts abandon and never purchase, the rest purchase at a random point
 * (some before any pipeline reminder would fire, some after one or more would already have been
 * sent), and some carts resume after abandoning, starting a new cycle, capped at 3 cycles so no
 * cart ever exceeds the default frequency cap. Every {@code Instant} on the returned events is on
 * a nominal timeline anchored at {@code start}; the publisher preserves each event's offset from
 * {@code start} when replaying against a real clock, so the cancellation math in {@link Expected}
 * (computed from the same nominal timeline) matches what the real pipeline will decide.
 */
public final class Workload {
    private Workload() {}

    private static final double ABANDON_PROBABILITY = 0.70;
    private static final double RESUME_ONE_PROBABILITY = 0.25;
    private static final double RESUME_TWO_PROBABILITY = 0.05;
    private static final double APPROX_EVENTS_PER_CART = 9.5;

    public record Result(List<ScriptedEvent> events, List<CartScript> scripts) {}

    /** Sizes the cart count from the target rate and duration so the script density approximates {@code ratePerSecond}. */
    public static Result generate(long seed, String runPrefix, double ratePerSecond, Duration duration,
                                   Instant start, RecoveryConfig config) {
        int cartCount = Math.max(1, (int) Math.round(ratePerSecond * duration.toSeconds() / APPROX_EVENTS_PER_CART));
        return generateCarts(seed, runPrefix, cartCount, duration, start, config);
    }

    /** Generates exactly {@code cartCount} cart scripts, evenly staggered across {@code duration}. */
    public static Result generateCarts(long seed, String runPrefix, int cartCount, Duration duration,
                                        Instant start, RecoveryConfig config) {
        Random rnd = new Random(seed);
        Duration lastOffset = config.offsets().get(config.offsets().size() - 1);
        List<ScriptedEvent> events = new ArrayList<>();
        List<CartScript> scripts = new ArrayList<>();
        long spacingMillis = cartCount <= 1 ? 0 : Math.max(1, duration.toMillis() / cartCount);

        for (int i = 0; i < cartCount; i++) {
            String cartId = runPrefix + "-" + i;
            String shopperKey = cartId + "-shopper";
            Instant t = start.plusMillis(i * spacingMillis);

            int resumes = pickResumeCount(rnd);
            boolean purchases = rnd.nextDouble() >= ABANDON_PROBABILITY;
            int cycleCount = resumes + 1;

            List<Cycle> cycles = new ArrayList<>();
            long version = 0;
            for (int cycleIndex = 0; cycleIndex < cycleCount; cycleIndex++) {
                boolean isLast = cycleIndex == cycleCount - 1;
                int editCount = 2 + rnd.nextInt(4);
                for (int e = 0; e < editCount; e++) {
                    version++;
                    t = t.plusMillis(50 + rnd.nextInt(250));
                    events.add(new ScriptedEvent(cartId, shopperKey, EventType.EDIT, version, t, 1 + rnd.nextInt(3)));
                }
                Instant lastActivityAt = t;
                long cycleVersion = version;
                Instant cancelledAt = null;
                if (isLast) {
                    if (purchases) {
                        Instant purchaseAt = lastActivityAt.plus(randomCancelGap(rnd, lastOffset));
                        version++;
                        events.add(new ScriptedEvent(cartId, shopperKey, EventType.PURCHASE, version, purchaseAt, 0));
                        cancelledAt = purchaseAt;
                        t = purchaseAt;
                    }
                } else {
                    Instant resumeAt = lastActivityAt.plus(randomCancelGap(rnd, lastOffset));
                    version++;
                    events.add(new ScriptedEvent(cartId, shopperKey, EventType.RESUME, version, resumeAt, 0));
                    cancelledAt = resumeAt;
                    t = resumeAt;
                }
                cycles.add(new Cycle(cycleVersion, lastActivityAt, cancelledAt));
            }
            scripts.add(new CartScript(cartId, shopperKey, cycles));
        }

        events.sort((a, b) -> a.occurredAt().compareTo(b.occurredAt()));
        return new Result(events, scripts);
    }

    private static int pickResumeCount(Random rnd) {
        double r = rnd.nextDouble();
        if (r < RESUME_TWO_PROBABILITY) return 2;
        if (r < RESUME_TWO_PROBABILITY + RESUME_ONE_PROBABILITY) return 1;
        return 0;
    }

    private static Duration randomCancelGap(Random rnd, Duration lastOffset) {
        long maxMillis = Math.max(1, lastOffset.toMillis() * 2);
        return Duration.ofMillis(rnd.nextLong(maxMillis));
    }
}
