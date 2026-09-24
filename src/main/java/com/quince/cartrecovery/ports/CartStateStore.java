package com.quince.cartrecovery.ports;

import com.quince.cartrecovery.model.CartRecord;
import java.util.List;
import java.util.Optional;

/** Durable per-cart state. Production: DynamoDB with a conditional write on version. */
public interface CartStateStore {
    long ABSENT = -1L;

    Optional<CartRecord> get(String cartId);

    /**
     * Writes the record only if the stored version equals expectedVersion,
     * or if expectedVersion is ABSENT and no record exists. Returns whether it wrote.
     */
    boolean put(CartRecord record, long expectedVersion);

    /** All ACTIVE and ABANDONED records, for reconciliation. */
    List<CartRecord> scanOpen();
}
