package sqlancer.sqlite3.oracle.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.TxBase;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementExecutionResult;
import sqlancer.common.transaction.TxTestExecutionResult;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.transaction.SQLite3Isolation;
import sqlancer.sqlite3.transaction.SQLite3TxStatement;
import sqlancer.sqlite3.transaction.SQLite3TxTestExecutor;
import sqlancer.sqlite3.transaction.SQLite3TxTestGenerator;


public class SQLite3WriteCheckOracle extends TxBase<SQLite3GlobalState> {

    public SQLite3WriteCheckOracle(SQLite3GlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        logger.writeCurrent("\n================= Generate new transaction list =================");
        SQLite3TxTestGenerator txTestGenerator = new SQLite3TxTestGenerator(state);
        List<Transaction> transactions = txTestGenerator.generateTransactions();
        for (Transaction tx : transactions) {
            logger.writeCurrent(tx.toString());
        }
        List<List<TxStatement>> schedules = txTestGenerator.genSchedules(transactions);

        try {
            for (List<TxStatement> schedule : schedules) {
                logger.writeCurrent("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                        collect(Collectors.joining(", ", "[", "]")));
                SQLite3Isolation.SQLite3IsolationLevel isoLevel = Randomly.fromOptions(SQLite3Isolation.SQLite3IsolationLevel.values());
                logger.writeCurrent("Isolation level: " + isoLevel);
                SQLite3TxTestExecutor testExecutor = new SQLite3TxTestExecutor(state, transactions, schedule, isoLevel);

                TxTestExecutionResult testResult = testExecutor.execute();
                dropTables();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleSchedule = genOracleSchedule(testResult);
                SQLite3TxTestExecutor oracleExecutor = new SQLite3TxTestExecutor(state, transactions, oracleSchedule, isoLevel);
                TxTestExecutionResult oracleResult = oracleExecutor.execute();
                dropTables();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleWithoutCommitAndRollbackSchedule = genOracleWithoutCommitAndRollbackSchedule(testResult);
                SQLite3TxTestExecutor oracleWithoutCommitAndRollbackExecutor = new SQLite3TxTestExecutor(state, transactions,
                        oracleWithoutCommitAndRollbackSchedule, isoLevel);
                TxTestExecutionResult oracleWithoutCommitAndRollbackResult = oracleWithoutCommitAndRollbackExecutor.execute();
                dropTables();
                reproduceDatabase(state.getState().getStatements());

                String compareResultInfo = compareWriteTxResults(testResult, oracleResult);
                if (!compareResultInfo.equals("")) {
                    state.getState().getLocalState().log("============Bug Report============");
                    state.getState().getLocalState().log("==Oracle With Commit And Rollback==");
                    for (Transaction tx : transactions) {
                        state.getState().getLocalState().log(tx.toString());
                    }
                    state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                            collect(Collectors.joining(", ", "[", "]")));
                    state.getState().getLocalState().log(compareResultInfo);
                    state.getState().getLocalState().log("Execution Result:");
                    state.getState().getLocalState().log(testResult.toString());
                    state.getState().getLocalState().log("Oracle Result:");
                    state.getState().getLocalState().log(oracleResult.toString());
                    throw new AssertionError("Transaction execution mismatches its oracles");
                } else {
                    state.getLogger().writeCurrent("============Is Same============");
                }

                compareResultInfo = compareWriteTxResults(testResult, oracleWithoutCommitAndRollbackResult);
                if (!compareResultInfo.equals("")) {
                    state.getState().getLocalState().log("============Bug Report============");
                    state.getState().getLocalState().log("==Oracle Without Commit And Rollback==");
                    for (Transaction tx : transactions) {
                        state.getState().getLocalState().log(tx.toString());
                    }
                    state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                            collect(Collectors.joining(", ", "[", "]")));
                    state.getState().getLocalState().log(compareResultInfo);
                    state.getState().getLocalState().log("Execution Result:");
                    state.getState().getLocalState().log(testResult.toString());
                    state.getState().getLocalState().log("Oracle Result:");
                    state.getState().getLocalState().log(oracleWithoutCommitAndRollbackResult.toString());
                    throw new AssertionError("Transaction execution mismatches its oracles");
                } else {
                    state.getLogger().writeCurrent("============Is Same============");
                }
            }
        } finally {
            for (Transaction tx : transactions) {
                tx.closeConnection();
            }
        }
    }

    public List<TxStatement> genOracleSchedule(TxTestExecutionResult testResult) {
        List<TxStatement> oracleSchedule = new ArrayList<>();
        for (TxStatementExecutionResult stmtResult : testResult.getStatementExecutionResults()) {
            Transaction stmtTx = stmtResult.getStatement().getTransaction();
            if (!stmtResult.isBlocked()) {
                if (stmtResult.reportDeadlock()) {
                    for (TxStatement stmt : stmtTx.getStatements()) {
                        if (!stmt.isEndTxType()) {
                            oracleSchedule.add(stmt);
                        }
                        if (stmtResult.getStatement().equals(stmt)) {
                            break;
                        }
                    }
                    // If tx reports deadlock, tx should be rollbacked.
                    TxStatement rollbackStmt = new SQLite3TxStatement(stmtTx, new TxSQLQueryAdapter("ROLLBACK"));
                    oracleSchedule.add(rollbackStmt);
                } else if (stmtResult.getStatement().getType() == SQLite3TxStatement.SQLite3StatementType.COMMIT
                        || stmtResult.getStatement().getType() == SQLite3TxStatement.SQLite3StatementType.ROLLBACK) {
                    oracleSchedule.addAll(stmtTx.getStatements());
                }
            }
        }
        return oracleSchedule;
    }

    public List<TxStatement> genOracleWithoutCommitAndRollbackSchedule(TxTestExecutionResult testResult) {
        List<TxStatement> oracleSchedule = new ArrayList<>();
        for (TxStatementExecutionResult stmtResult : testResult.getStatementExecutionResults()) {
            Transaction stmtTx = stmtResult.getStatement().getTransaction();
            if (!stmtResult.isBlocked() && !stmtResult.reportDeadlock() && stmtResult.getStatement().getType() == SQLite3TxStatement.SQLite3StatementType.COMMIT) {
                // We remove BEGIN and COMMIT statements for COMMIT transactions
                List<TxStatement> txStatements = new ArrayList<>(stmtTx.getStatements());
                txStatements.remove(0);
                txStatements.remove(txStatements.size() - 1);
                oracleSchedule.addAll(txStatements);
            }
        }
        return oracleSchedule;
    }

    protected void dropTables() throws SQLException {
        List<SQLite3Table> tables = state.getSchema().getTables().getTables();
        for (SQLite3Table table : tables) {
            SQLQueryAdapter queryAdapter;
            if (table.isView()) {
                queryAdapter = new SQLQueryAdapter("DROP VIEW " + table.getName(), true);
            } else {
                queryAdapter = new SQLQueryAdapter("DROP TABLE " + table.getName(), true);
            }
            queryAdapter.execute(state);
        }
        try {
            state.updateSchema();
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }
}
