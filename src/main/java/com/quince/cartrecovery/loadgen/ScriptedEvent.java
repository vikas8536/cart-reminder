package com.quince.cartrecovery.loadgen;

import java.time.Instant;

/**
 * One scripted cart event, ready to publish to {@code cart-events}. {@code occurredAt} is the
 * script's nominal timeline (relative ordering and gaps only); the publisher anchors the whole
 * script to a real start instant and uses each event's own send time as the real {@code occurredAt}.
 */
public record ScriptedEvent(String cartId, String shopperKey, EventType type, long version,
                             Instant occurredAt, int itemCount) {}
