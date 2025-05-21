package sqlancer.mysql.gen.transaction;

import java.sql.SQLException;
import java.util.Arrays;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxStatement;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.gen.MySQLDeleteGenerator;
import sqlancer.mysql.gen.MySQLInsertGenerator;
import sqlancer.mysql.gen.MySQLSelectGenerator;
import sqlancer.mysql.gen.MySQLSelectForShareGenerator;
import sqlancer.mysql.gen.MySQLSelectForUpdateGenerator;
import sqlancer.mysql.gen.MySQLUpdateGenerator;
import sqlancer.mysql.transaction.MySQLTxStatement;

public class MySQLTransactionProvider {

    public enum MySQLTransactionAction {
        BEGIN(MySQLBeginGenerator::getQuery),
        COMMIT(MySQLCommitGenerator::getQuery),
        ROLLBACK(MySQLRollbackGenerator::getQuery),
        INSERT(MySQLInsertGenerator::insertRow),
        SELECT(MySQLSelectGenerator::getQuery),
        SELECT_SHARE(MySQLSelectForShareGenerator::getQuery),
        SELECT_UPDATE(MySQLSelectForUpdateGenerator::getQuery),
        DELETE(MySQLDeleteGenerator::delete),
        UPDATE(MySQLUpdateGenerator::getQuery);

        private final SQLQueryProvider<MySQLGlobalState> sqlQueryProvider;

        MySQLTransactionAction(SQLQueryProvider<MySQLGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(MySQLGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(MySQLGlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);

        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = MySQLTransactionProvider.MySQLTransactionAction.BEGIN.getQuery(state);
            TxStatement beginStmt = new MySQLTxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            MySQLTransactionProvider.MySQLTransactionAction[] availActions = Arrays.copyOfRange(MySQLTransactionProvider.
                            MySQLTransactionAction.values(), 3,
                    MySQLTransactionProvider.MySQLTransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                MySQLTransactionProvider.MySQLTransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("Deadlock found");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new MySQLTxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = MySQLTransactionProvider.MySQLTransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = MySQLTransactionProvider.MySQLTransactionAction.ROLLBACK.getQuery(state);
            }
            TxStatement endStmt = new MySQLTxStatement(tx, endQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }
}
