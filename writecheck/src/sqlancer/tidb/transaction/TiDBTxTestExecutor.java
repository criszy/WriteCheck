package sqlancer.tidb.transaction;

import java.sql.SQLException;
import java.util.List;

import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxTestExecutor;
import sqlancer.tidb.gen.transaction.TiDBIsolationLevelGenerator;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;

public class TiDBTxTestExecutor extends TxTestExecutor<TiDBGlobalState> {

    public TiDBTxTestExecutor(TiDBGlobalState globalState, List<Transaction> transactions,
                              List<TxStatement> schedule, TiDBIsolationLevel isoLevel) {
        super(globalState, transactions, schedule, isoLevel);
    }

    @Override
    protected void generateIsolationLevel() throws SQLException {
        TiDBIsolationLevelGenerator isoLevelGenerator = new TiDBIsolationLevelGenerator(isoLevel);
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter isoQuery = new TxSQLQueryAdapter(isoLevelGenerator.getQuery());
            isoQuery.execute(tx);
        }
    }

    @Override
    protected void setTimeout() throws SQLException {
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter timeoutQuery = new TxSQLQueryAdapter("set innodb_lock_wait_timeout = 100");
            timeoutQuery.execute(tx);
        }
    }

    @Override
    protected SQLancerResultSet showWarnings(Transaction transaction) throws SQLException {
        TxSQLQueryAdapter showSql = new TxSQLQueryAdapter("SHOW WARNINGS");
        return showSql.executeAndGet(transaction);
    }

    @Override
    protected void handleAbortedTxn(Transaction transaction) throws SQLException {

    }
}