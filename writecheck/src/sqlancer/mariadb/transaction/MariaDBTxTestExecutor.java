package sqlancer.mariadb.transaction;

import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxTestExecutor;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.gen.transaction.MariaDBIsolationLevelGenerator;

import java.sql.SQLException;
import java.util.List;

public class MariaDBTxTestExecutor extends TxTestExecutor<MariaDBProvider.MariaDBGlobalState> {

    public MariaDBTxTestExecutor(MariaDBProvider.MariaDBGlobalState globalState, List<Transaction> transactions,
                              List<TxStatement> schedule, MariaDBIsolation.MariaDBIsolationLevel isoLevel) {
        super(globalState, transactions, schedule, isoLevel);
    }

    @Override
    protected void generateIsolationLevel() throws SQLException {
        MariaDBIsolationLevelGenerator isoLevelGenerator = new MariaDBIsolationLevelGenerator(isoLevel);
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter isoQuery = new TxSQLQueryAdapter(isoLevelGenerator.getQuery());
            isoQuery.execute(tx);
        }
    }

    @Override
    protected void setTimeout() throws SQLException {
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter timeoutQuery = new TxSQLQueryAdapter("set innodb_lock_wait_timeout = 400");
            timeoutQuery.execute(tx);
        }
    }

    @Override
    protected void handleAbortedTxn(Transaction transaction) throws SQLException {
        TxSQLQueryAdapter rollbackStmt = new TxSQLQueryAdapter("ROLLBACK");
        try {
            rollbackStmt.execute(transaction);
        } catch (SQLException e) {
            throw new RuntimeException("Transaction returning rollback exception: ", e);
        }
    }

    @Override
    protected SQLancerResultSet showWarnings(Transaction transaction) throws SQLException {
        TxSQLQueryAdapter showSql = new TxSQLQueryAdapter("SHOW WARNINGS");
        return showSql.executeAndGet(transaction);
    }

}
