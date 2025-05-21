package sqlancer.mysql.transaction;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxTestGenerator;
import sqlancer.mysql.gen.transaction.MySQLTransactionProvider;
import sqlancer.mysql.MySQLGlobalState;

import java.sql.SQLException;

public class MySQLTxTestGenerator extends TxTestGenerator<MySQLGlobalState> {

    public MySQLTxTestGenerator(MySQLGlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Transaction generateTransaction() throws SQLException {
        return MySQLTransactionProvider.generateTransaction(globalState);
    }
}
