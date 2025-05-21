package sqlancer.mariadb.oracle.transaction;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.oracle.TxBase;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementExecutionResult;
import sqlancer.common.transaction.TxTestExecutionResult;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.transaction.MariaDBIsolation;
import sqlancer.mariadb.transaction.MariaDBTxStatement;
import sqlancer.mariadb.transaction.MariaDBTxTestExecutor;
import sqlancer.mariadb.transaction.MariaDBTxTestGenerator;

public class MariaDBWriteCheckOracle extends TxBase<MariaDBGlobalState> {

    public MariaDBWriteCheckOracle(MariaDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {
        logger.writeCurrent("\n================= Generate new transaction list =================");
        MariaDBTxTestGenerator txTestGenerator = new MariaDBTxTestGenerator(state);
        List<Transaction> transactions = txTestGenerator.generateTransactions();
        for (Transaction tx : transactions) {
            logger.writeCurrent(tx.toString());
        }
        List<List<TxStatement>> schedules = txTestGenerator.genSchedules(transactions);

        try {
            for (List<TxStatement> schedule : schedules) {
                logger.writeCurrent("Input schedule: " + schedule.stream().map(TxStatement::getStmtId).
                        collect(Collectors.joining(", ", "[", "]")));
                MariaDBIsolation.MariaDBIsolationLevel isoLevel = Randomly.fromOptions(MariaDBIsolation.MariaDBIsolationLevel.values());
                logger.writeCurrent("Isolation level: " + isoLevel);
                MariaDBTxTestExecutor testExecutor = new MariaDBTxTestExecutor(state, transactions, schedule, isoLevel);

                TxTestExecutionResult testResult = testExecutor.execute();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleSchedule = genOracleSchedule(testResult);
                MariaDBTxTestExecutor oracleExecutor = new MariaDBTxTestExecutor(state, transactions, oracleSchedule, isoLevel);
                TxTestExecutionResult oracleResult = oracleExecutor.execute();
                reproduceDatabase(state.getState().getStatements());

                List<TxStatement> oracleWithoutCommitAndRollbackSchedule = genOracleWithoutCommitAndRollbackSchedule(testResult);
                MariaDBTxTestExecutor oracleWithoutCommitAndRollbackExecutor = new MariaDBTxTestExecutor(state, transactions,
                        oracleWithoutCommitAndRollbackSchedule, isoLevel);
                TxTestExecutionResult oracleWithoutCommitAndRollbackResult = oracleWithoutCommitAndRollbackExecutor.execute();
                reproduceDatabase(state.getState().getStatements());

                String compareResultInfo;
                if (isoLevel == MariaDBIsolation.MariaDBIsolationLevel.SERIALIZABLE) {
                    compareResultInfo = compareAllResults(testResult, oracleResult);
                } else {
                    compareResultInfo = compareWriteTxResults(testResult, oracleResult);
                }
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

                if (isoLevel == MariaDBIsolation.MariaDBIsolationLevel.SERIALIZABLE) {
                    compareResultInfo = compareAllResults(testResult, oracleWithoutCommitAndRollbackResult);
                } else {
                    compareResultInfo = compareWriteTxResults(testResult, oracleWithoutCommitAndRollbackResult);
                }
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
                    // If tx reports deadlock, tx should be rollback.
                    TxStatement rollbackStmt = new MariaDBTxStatement(stmtTx, new TxSQLQueryAdapter("ROLLBACK"));
                    oracleSchedule.add(rollbackStmt);
                } else if (stmtResult.getStatement().getType() == MariaDBTxStatement.MariaDBStatementType.COMMIT
                        || stmtResult.getStatement().getType() == MariaDBTxStatement.MariaDBStatementType.ROLLBACK) {
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
            if (!stmtResult.isBlocked() && !stmtResult.reportDeadlock() && stmtResult.getStatement().getType() == MariaDBTxStatement.MariaDBStatementType.COMMIT) {
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
