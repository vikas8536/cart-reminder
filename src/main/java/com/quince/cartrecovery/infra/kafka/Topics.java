package com.quince.cartrecovery.infra.kafka;

import com.quince.cartrecovery.model.Lane;
import java.time.Duration;
import java.util.List;

/** The seven topics of spec §5.1. All share one partition count P and are keyed by cartId. */
public final class Topics {
    public static final String CART_EVENTS = "cart-events";
    public static final String CART_EVENTS_DLQ = "cart-events-dlq";
    public static final String INTENTS_FAST = "reminder-intents-fast";
    public static final String INTENTS_SLOW = "reminder-intents-slow";
    public static final String REMINDER_DLQ = "reminder-dlq";
    public static final String OUTCOMES = "reminder-outcomes";
    public static final String SINK_SENDS = "sink-sends";

    public static final List<String> ALL =
            List.of(CART_EVENTS, CART_EVENTS_DLQ, INTENTS_FAST, INTENTS_SLOW, REMINDER_DLQ, OUTCOMES, SINK_SENDS);

    private Topics() {}

    public static String intents(Lane lane) {
        return lane == Lane.FAST ? INTENTS_FAST : INTENTS_SLOW;
    }

    /** DLQs keep 30 days, everything else 7 days. */
    public static Duration retention(String topic) {
        return topic.endsWith("-dlq") ? Duration.ofDays(30) : Duration.ofDays(7);
    }
}
