package sqlancer.postgres.gen.transaction;

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
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.gen.PostgresDeleteGenerator;
import sqlancer.postgres.gen.PostgresInsertGenerator;
import sqlancer.postgres.gen.PostgresSelectForShareGenerator;
import sqlancer.postgres.gen.PostgresSelectForUpdateGenerator;
import sqlancer.postgres.gen.PostgresSelectGenerator;
import sqlancer.postgres.gen.PostgresUpdateGenerator;
import sqlancer.postgres.transaction.PostgresTxStatement;

public class PostgresTransactionProvider {

    public enum PostgresTransactionAction {
        BEGIN(PostgresBeginGenerator::getQuery),
        COMMIT(PostgresCommitGenerator::getQuery),
        ROLLBACK(PostgresRollbackGenerator::getQuery),
        INSERT(PostgresInsertGenerator::insert),
        SELECT(PostgresSelectGenerator::getQuery),
        SELECT_SHARE(PostgresSelectForShareGenerator::getQuery),
        SELECT_UPDATE(PostgresSelectForUpdateGenerator::getQuery),
        UPDATE(PostgresUpdateGenerator::create),
        DELETE(PostgresDeleteGenerator::create);

        private final SQLQueryProvider<PostgresGlobalState> sqlQueryProvider;

        PostgresTransactionAction(SQLQueryProvider<PostgresGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(PostgresGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // TODO: to be configurable
    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(PostgresGlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);
        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = PostgresTransactionAction.BEGIN.getQuery(state);
            TxStatement beginStmt = new PostgresTxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            PostgresTransactionAction[] availActions = Arrays.copyOfRange(PostgresTransactionAction.values(), 3,
                    PostgresTransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                PostgresTransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("deadlock detected");
                    errors.add("current transaction is aborted");
                    errors.add("does not exist");
                    errors.add("permission denied for");
                    errors.add("could not serialize");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new PostgresTxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = PostgresTransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = PostgresTransactionAction.ROLLBACK.getQuery(state);
            }
            ExpectedErrors errors = endQuery.getExpectedErrors();
            SQLQueryAdapter finalQuery = new SQLQueryAdapter(endQuery.getQueryString(), errors);
            TxStatement endStmt = new PostgresTxStatement(tx, finalQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }
}
