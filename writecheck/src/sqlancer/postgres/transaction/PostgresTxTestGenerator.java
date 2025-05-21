package sqlancer.postgres.transaction;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxTestGenerator;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.transaction.PostgresTransactionProvider;

import java.sql.SQLException;

public class PostgresTxTestGenerator extends TxTestGenerator<PostgresGlobalState> {

    public PostgresTxTestGenerator(PostgresGlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Transaction generateTransaction() throws SQLException {
        return PostgresTransactionProvider.generateTransaction(globalState);
    }
}
