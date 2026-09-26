package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.Arm;
import com.quince.cartrecovery.model.CartEvent;
import com.quince.cartrecovery.model.CartRecord;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * Durable per-cart state. Production: DynamoDB "carts" with field-scoped conditional updates.
 * A cart is "open" (listed by openCartIds) while it has a next step: from an edit or resume until it
 * closes, is abandoned but ineligible, or ends its sequence. openUntil = lastActivityAt + last offset
 * + last lateness bound, rounded up to the next whole hour.
 */
public interface CartStateStore {
    /** Consistent read. */
    Optional<CartRecord> get(String cartId);

    /** Consistent reads in input order; absent ids are omitted and a duplicate id appears once. */
    List<CartRecord> getAll(Collection<String> cartIds);

    /**
     * Applies the event if the cart is absent or its stored version is lower; empty when stale.
     * Edit or resume: status ACTIVE, version, lastActivityAt = occurredAt, items (edit only, capped at 50),
     * firstName (edit only, and only when non-null), srcPartition, shopperKey and arm only if the cart is new;
     * opens the cart. Purchase or clear: status CLOSED, version, lastActivityAt = occurredAt, srcPartition;
     * closes the cart. sequenceStarts are never touched here.
     */
    Optional<CartRecord> applyEvent(CartEvent event, Arm arm, int srcPartition);

    /**
     * Condition: stored version = record.version() AND status = ACTIVE. Sets ABANDONED and sequenceStarts;
     * an ineligible cart is closed in the open index. Returns whether it wrote.
     */
    boolean markAbandoned(CartRecord record, List<Instant> sequenceStarts, boolean eligible);

    /** Condition: stored version = version AND status = ABANDONED. Removes the cart from the open index. */
    boolean endSequence(String cartId, long version);

    /** Open carts of this shard with openUntil >= now. Close the stream after use. */
    Stream<String> openCartIds(int shard, Instant now);
}
