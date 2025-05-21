package sqlancer.sqlite3.gen.transaction;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxStatement;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.gen.SQLite3TransactionGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3DeleteGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3InsertGenerator;
import sqlancer.sqlite3.gen.dml.SQLite3UpdateGenerator;
import sqlancer.sqlite3.gen.SQLite3SelectGenerator;
import sqlancer.sqlite3.transaction.SQLite3TxStatement;

import java.sql.SQLException;
import java.util.Arrays;


public class SQLite3TransactionProvider {

    public enum SQLite3TransactionAction {

        BEGIN(SQLite3TransactionGenerator::generateBeginTransaction),
        COMMIT(SQLite3TransactionGenerator::generateCommit),
        ROLLBACK(SQLite3TransactionGenerator::generateRollbackTransaction),
        INSERT(SQLite3InsertGenerator::insertRow),
        SELECT(SQLite3SelectGenerator::getQuery),
        DELETE(SQLite3DeleteGenerator::deleteContent),
        UPDATE(SQLite3UpdateGenerator::updateRow);

        private final SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider;

        SQLite3TransactionAction(SQLQueryProvider<SQLite3GlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(SQLite3GlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // TODO: to be configurable
    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(SQLite3GlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);

        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = SQLite3TransactionProvider.SQLite3TransactionAction.BEGIN.getQuery(state);
            TxStatement beginStmt = new SQLite3TxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            SQLite3TransactionProvider.SQLite3TransactionAction[] availActions = Arrays.copyOfRange(SQLite3TransactionProvider.SQLite3TransactionAction.values(), 3,
                    SQLite3TransactionProvider.SQLite3TransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                SQLite3TransactionProvider.SQLite3TransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("Deadlock found");
                    errors.add("database is locked");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new SQLite3TxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = SQLite3TransactionProvider.SQLite3TransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = SQLite3TransactionProvider.SQLite3TransactionAction.ROLLBACK.getQuery(state);
            }
            TxStatement endStmt = new SQLite3TxStatement(tx, endQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }
}
