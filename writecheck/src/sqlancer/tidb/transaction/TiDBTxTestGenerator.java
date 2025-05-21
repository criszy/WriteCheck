package sqlancer.tidb.transaction;

import java.sql.SQLException;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxTestGenerator;
import sqlancer.tidb.gen.transaction.TiDBTransactionProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;

public class TiDBTxTestGenerator extends TxTestGenerator<TiDBGlobalState> {

    public TiDBTxTestGenerator(TiDBGlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Transaction generateTransaction() throws SQLException {
        return TiDBTransactionProvider.generateTransaction(globalState);
    }
}
