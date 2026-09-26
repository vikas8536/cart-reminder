package com.quince.cartrecovery.infra.dynamo;

import com.quince.cartrecovery.contract.CartStateStoreContract;
import com.quince.cartrecovery.model.RecoveryConfig;
import com.quince.cartrecovery.ports.CartStateStore;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's cart store contract against DynamoDB Local, one fresh table per store. */
@Testcontainers(disabledWithoutDocker = true)
class DynamoCartStateStoreContractTest extends CartStateStoreContract {
    @Override
    protected CartStateStore newStore(RecoveryConfig config, int shards) {
        String table = TestDynamo.table("carts");
        DynamoTables.createCarts(TestDynamo.client(), table);
        TestDynamo.noExpiry(table);
        return new DynamoCartStateStore(TestDynamo.client(), table, config, shards);
    }
}
