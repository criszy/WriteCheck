package sqlancer.tidb.gen.transaction;

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
import sqlancer.tidb.TiDBOptions;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBSelectForUpdateGenerator;
import sqlancer.tidb.gen.TiDBSelectGenerator;
import sqlancer.tidb.gen.TiDBUpdateGenerator;
import sqlancer.tidb.transaction.TiDBTxStatement;

public class TiDBTransactionProvider {

    public enum TiDBTransactionAction {

        BEGIN(TiDBBeginGenerator::getQuery),
        BEGIN_OPTIMISTIC(TiDBBeginOptimisticGenerator::getQuery),
        COMMIT(TiDBCommitGenerator::getQuery),
        ROLLBACK(TiDBRollbackGenerator::getQuery),
        INSERT(TiDBInsertGenerator::getQuery),
        SELECT(TiDBSelectGenerator::getQuery),
        SELECT_UPDATE(TiDBSelectForUpdateGenerator::getQuery),
        DELETE(TiDBDeleteGenerator::getQuery),
        UPDATE(TiDBUpdateGenerator::getQuery);

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        TiDBTransactionAction(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // TODO: to be configurable
    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(TiDBGlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);

        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = TiDBTransactionAction.BEGIN.getQuery(state);
            if (state.getDbmsSpecificOptions().oracle.stream().anyMatch(o -> o
                    == TiDBOptions.TiDBOracleFactory.OPT_WRITE_CHECK)) {
                beginQuery = TiDBTransactionAction.BEGIN_OPTIMISTIC.getQuery(state);
            }
            TxStatement beginStmt = new TiDBTxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            TiDBTransactionAction[] availActions = Arrays.copyOfRange(TiDBTransactionAction.values(), 4,
                    TiDBTransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                TiDBTransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("Deadlock found");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new TiDBTxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = TiDBTransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = TiDBTransactionAction.ROLLBACK.getQuery(state);
            }
            ExpectedErrors errors = endQuery.getExpectedErrors();
            errors.add("try again later");
            SQLQueryAdapter finalQuery = new SQLQueryAdapter(endQuery.getQueryString(), errors);
            TxStatement endStmt = new TiDBTxStatement(tx, finalQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }
}
