package com.quince.cartrecovery.infra.dynamo;

import com.quince.cartrecovery.contract.SendLedgerContract;
import com.quince.cartrecovery.ports.SendLedger;
import java.time.Duration;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs thread A's send ledger contract against DynamoDB Local, one fresh table per ledger. */
@Testcontainers(disabledWithoutDocker = true)
class DynamoSendLedgerContractTest extends SendLedgerContract {
    @Override
    protected SendLedger newLedger(Duration lease, int shards) {
        String table = TestDynamo.table("send-ledger");
        DynamoTables.createLedger(TestDynamo.client(), table);
        TestDynamo.noExpiry(table);
        return new DynamoSendLedger(TestDynamo.client(), table, lease, shards);
    }
}
