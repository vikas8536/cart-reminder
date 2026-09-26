package com.quince.cartrecovery.inmemory;

import com.quince.cartrecovery.contract.CartStateStoreContract;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.CartStateStore;

class InMemoryCartStateStoreContractTest extends CartStateStoreContract {
    @Override protected CartStateStore newStore(RecoveryConfig config, int shards) {
        return new InMemoryCartStateStore(config, shards);
    }
}
