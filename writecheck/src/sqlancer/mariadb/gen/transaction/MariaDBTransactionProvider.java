package sqlancer.mariadb.gen.transaction;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxStatement;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.gen.*;
import sqlancer.mariadb.transaction.MariaDBTxStatement;

import java.sql.SQLException;
import java.util.Arrays;

public class MariaDBTransactionProvider {

    public enum MariaDBTransactionAction {

        BEGIN(MariaDBBeginGenerator::getQuery),
        COMMIT(MariaDBCommitGenerator::getQuery),
        ROLLBACK(MariaDBRollbackGenerator::getQuery),
        INSERT(MariaDBInsertGenerator::getQuery),
        SELECT(MariaDBSelectGenerator::getQuery),
        SELECT_UPDATE(MariaDBSelectForUpdateGenerator::getQuery),
        SELECT_SHARE(MariaDBSelectForShareGenerator::getQuery),
        DELETE(MariaDBDeleteGenerator::getQuery),
        UPDATE(MariaDBUpdateGenerator::getQuery);

        private final SQLQueryProvider<MariaDBProvider.MariaDBGlobalState> sqlQueryProvider;

        MariaDBTransactionAction(SQLQueryProvider<MariaDBProvider.MariaDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(MariaDBProvider.MariaDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // TODO: to be configurable
    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(MariaDBProvider.MariaDBGlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);

        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = MariaDBTransactionProvider.MariaDBTransactionAction.BEGIN.getQuery(state);
            TxStatement beginStmt = new MariaDBTxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            MariaDBTransactionProvider.MariaDBTransactionAction[] availActions = Arrays.copyOfRange(MariaDBTransactionProvider.MariaDBTransactionAction.values(), 3,
                    MariaDBTransactionProvider.MariaDBTransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                MariaDBTransactionProvider.MariaDBTransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("Deadlock found");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new MariaDBTxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = MariaDBTransactionProvider.MariaDBTransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = MariaDBTransactionProvider.MariaDBTransactionAction.ROLLBACK.getQuery(state);
            }
            TxStatement endStmt = new MariaDBTxStatement(tx, endQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }

}
