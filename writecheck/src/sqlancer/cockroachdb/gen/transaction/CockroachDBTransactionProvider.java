package sqlancer.cockroachdb.gen.transaction;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.gen.CockroachDBDeleteGenerator;
import sqlancer.cockroachdb.gen.CockroachDBInsertGenerator;
import sqlancer.cockroachdb.gen.CockroachDBSelectGenerator;
import sqlancer.cockroachdb.gen.CockroachDBUpdateGenerator;
import sqlancer.cockroachdb.transaction.CockroachDBTxStatement;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxStatement;

import java.sql.SQLException;
import java.util.Arrays;

public class CockroachDBTransactionProvider {

    public enum CockroachDBTransactionAction {

        BEGIN(CockroachDBBeginGenerator::getQuery),
        COMMIT(CockroachDBCommitGenerator::getQuery),
        ROLLBACK(CockroachDBRollbackGenerator::getQuery),
        INSERT(CockroachDBInsertGenerator::insert),
        SELECT(CockroachDBSelectGenerator::getQuery),
        DELETE(CockroachDBDeleteGenerator::delete),
        UPDATE(CockroachDBUpdateGenerator::gen);

        private final SQLQueryProvider<CockroachDBProvider.CockroachDBGlobalState> sqlQueryProvider;

        CockroachDBTransactionAction(SQLQueryProvider<CockroachDBProvider.CockroachDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(CockroachDBProvider.CockroachDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    // TODO: to be configurable
    private static final int TX_SIZE_MIN = 1;
    private static final int TX_SIZE_MAX = 5;

    public static Transaction generateTransaction(CockroachDBProvider.CockroachDBGlobalState state) throws SQLException {
        SQLConnection con = state.createConnection();
        Transaction tx = new Transaction(con);

        int stmtNum = (int) Randomly.getNotCachedInteger(TX_SIZE_MIN, TX_SIZE_MAX + 1);

        try {
            SQLQueryAdapter beginQuery = CockroachDBTransactionProvider.CockroachDBTransactionAction.BEGIN.getQuery(state);
            TxStatement beginStmt = new CockroachDBTxStatement(tx, beginQuery);
            tx.addStatement(beginStmt);

            CockroachDBTransactionProvider.CockroachDBTransactionAction[] availActions = Arrays.copyOfRange(CockroachDBTransactionProvider.CockroachDBTransactionAction.values(), 3,
                    CockroachDBTransactionProvider.CockroachDBTransactionAction.values().length);

            for (int i = 1; i <= stmtNum; i++) {
                int actionId = (int) Randomly.getNotCachedInteger(0, availActions.length);
                CockroachDBTransactionProvider.CockroachDBTransactionAction action = availActions[actionId];
                try {
                    SQLQueryAdapter query = action.getQuery(state);
                    ExpectedErrors errors = query.getExpectedErrors();
                    errors.add("TransactionAbortedError");
                    errors.add("WriteTooOldError");
                    errors.add("RETRY_SERIALIZABLE");
                    SQLQueryAdapter finalQuery = new SQLQueryAdapter(query.getQueryString(), errors);
                    TxStatement txStmt = new CockroachDBTxStatement(tx, finalQuery);
                    tx.addStatement(txStmt);
                } catch (IgnoreMeException e) {
                    i--;
                }
            }

            SQLQueryAdapter endQuery = CockroachDBTransactionProvider.CockroachDBTransactionAction.COMMIT.getQuery(state);
            if (Randomly.getBoolean()) {
                endQuery = CockroachDBTransactionProvider.CockroachDBTransactionAction.ROLLBACK.getQuery(state);
            }
            ExpectedErrors errors = endQuery.getExpectedErrors();
            SQLQueryAdapter finalQuery = new SQLQueryAdapter(endQuery.getQueryString(), errors);
            TxStatement endStmt = new CockroachDBTxStatement(tx, finalQuery);
            tx.addStatement(endStmt);
        } catch (Exception e) {
            throw new IgnoreMeException();
        }
        return tx;
    }

}
