package sqlancer.tidb.oracle.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementExecutionResult;
import sqlancer.common.transaction.TxTestExecutionResult;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;
import sqlancer.tidb.transaction.TiDBTxStatement;
import sqlancer.tidb.transaction.TiDBTxTestExecutor;
import sqlancer.tidb.transaction.TiDBTxTestGenerator;

public class TiDBOptWriteCheckOracle extends TiDBWriteCheckOracle {

    public TiDBOptWriteCheckOracle(TiDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        logger.writeCurrent("\n================= Generate new transaction list =================");
        TiDBTxTestGenerator txTestGenerator = new TiDBTxTestGenerator(state);
        List<Transaction> transactions = txTestGenerator.generateTransactions();
        for (Transaction tx : transactions) {
            logger.writeCurrent(tx.toString());
        }
        List<List<TxStatement>> schedules = txTestGenerator.genSchedules(transactions);

        try {
            for (List<TxStatement> schedule : schedules) {
                logger.writeCurrent("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                        collect(Collectors.joining(", ", "[", "]")));
                TiDBIsolationLevel isoLevel = TiDBIsolationLevel.REPEATABLE_READ;
                logger.writeCurrent("Isolation level: " + isoLevel);
                TiDBTxTestExecutor testExecutor = new TiDBTxTestExecutor(state, transactions, schedule, isoLevel);

                TxTestExecutionResult testResult = testExecutor.execute();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleSchedule = genOracleSchedule(testResult);
                TiDBTxTestExecutor oracleExecutor = new TiDBTxTestExecutor(state, transactions, oracleSchedule, isoLevel);
                TxTestExecutionResult oracleResult = oracleExecutor.execute();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleWithoutCommitAndRollbackSchedule = genOracleWithoutCommitAndRollbackSchedule(testResult);
                TiDBTxTestExecutor oracleWithoutCommitAndRollbackExecutor = new TiDBTxTestExecutor(state, transactions,
                        oracleWithoutCommitAndRollbackSchedule, isoLevel);
                TxTestExecutionResult oracleWithoutCommitAndRollbackResult = oracleWithoutCommitAndRollbackExecutor.execute();
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
            if (stmtResult.reportDeadlock()) {
                List<TxStatement> txStatements = new ArrayList<>(stmtTx.getStatements());
                txStatements.remove(txStatements.size() - 1);
                TxStatement rollbackStmt = new TiDBTxStatement(stmtTx, new TxSQLQueryAdapter("ROLLBACK"));
                txStatements.add(rollbackStmt);
                oracleSchedule.addAll(txStatements);
            } else if (stmtResult.getStatement().getType() == TiDBTxStatement.TiDBStatementType.COMMIT
                    || stmtResult.getStatement().getType() == TiDBTxStatement.TiDBStatementType.ROLLBACK) {
                oracleSchedule.addAll(stmtTx.getStatements());
            }
        }
        return oracleSchedule;
    }

    public List<TxStatement> genOracleWithoutCommitAndRollbackSchedule(TxTestExecutionResult testResult) {
        List<TxStatement> oracleSchedule = new ArrayList<>();
        for (TxStatementExecutionResult stmtResult : testResult.getStatementExecutionResults()) {
            Transaction stmtTx = stmtResult.getStatement().getTransaction();
            if (!stmtResult.reportDeadlock() && stmtResult.getStatement().getType() == TiDBTxStatement.TiDBStatementType.COMMIT) {
                // We remove BEGIN and COMMIT statements for COMMIT transactions
                List<TxStatement> txStatements = new ArrayList<>(stmtTx.getStatements());
                txStatements.remove(0);
                txStatements.remove(txStatements.size() - 1);
                oracleSchedule.addAll(txStatements);
            }
        }
        return oracleSchedule;
    }
}
