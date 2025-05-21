package sqlancer.sqlite3.transaction;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxTestGenerator;
import sqlancer.sqlite3.gen.transaction.SQLite3TransactionProvider;
import sqlancer.sqlite3.SQLite3GlobalState;

import java.sql.SQLException;

public class SQLite3TxTestGenerator extends TxTestGenerator<SQLite3GlobalState> {

    public SQLite3TxTestGenerator(SQLite3GlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Transaction generateTransaction() throws SQLException {
        return SQLite3TransactionProvider.generateTransaction(globalState);
    }

}
