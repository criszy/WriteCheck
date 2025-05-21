package sqlancer.tidb.oracle.transaction;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.Query;
import sqlancer.common.transaction.*;
import sqlancer.tidb.TiDBErrors;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.transaction.TiDBIsolation.TiDBIsolationLevel;
import sqlancer.tidb.transaction.TiDBTxStatement;
import sqlancer.tidb.transaction.TiDBTxTestExecutor;

public class TiDBOptWriteCheckReproduceOracle extends TiDBWriteCheckReproduceOracle {

    public TiDBOptWriteCheckReproduceOracle(TiDBGlobalState state) {
        super(state);
    }

    @Override
    public void check() throws SQLException {

        Scanner scanner;
        try {
            File caseFile = new File(options.getCaseFile());
            scanner = new Scanner(caseFile);
            logger.writeCurrent("Read database and transaction from file: " + options.getCaseFile());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Read case from file failed: ", e);
        }
        List<Query<?>> dbInitQueries = prepareTableFromScanner(scanner);
        List<Transaction> transactions = new ArrayList<>();
        for (int i = 0; i < state.getOptions().getNrTransactions(); i++) {
            transactions.add(readTransactionFromScanner(scanner));
        }
        String scheduleStr = readOrderFromScanner(scanner);
        List<TxStatement> schedule = checkOrder(scheduleStr, transactions);
        boolean detectBug = false;
        try {
            TiDBIsolationLevel level = TiDBIsolationLevel.REPEATABLE_READ;
            TiDBTxTestExecutor testExecutor = new TiDBTxTestExecutor(state, transactions, schedule, level);
            TxTestExecutionResult testResult = testExecutor.execute();
            reproduceDatabase(dbInitQueries);

            List<TxStatement> oracleSchedule = genOracleSchedule(testResult);
            TiDBTxTestExecutor oracleExecutor = new TiDBTxTestExecutor(state, transactions, oracleSchedule, level);
            TxTestExecutionResult oracleResult = oracleExecutor.execute();
            reproduceDatabase(dbInitQueries);

            List<TxStatement> oracleWithoutCommitAndRollbackSchedule = genOracleWithoutCommitAndRollbackSchedule(testResult);
            TiDBTxTestExecutor oracleWithoutCommitAndRollbackExecutor = new TiDBTxTestExecutor(state, transactions,
                    oracleWithoutCommitAndRollbackSchedule, level);
            TxTestExecutionResult oracleWithoutCommitAndRollbackResult = oracleWithoutCommitAndRollbackExecutor.execute();
            reproduceDatabase(dbInitQueries);

            String compareResultInfo = compareWriteTxResults(testResult, oracleResult);
            if (compareResultInfo.equals("")) {
                state.getLogger().writeCurrent("============Is Same============");
                state.getLogger().writeCurrent("==Oracle With Commit And Rollback==");
                state.getLogger().writeCurrent("Execution Result:");
                state.getLogger().writeCurrent(testResult.toString());
                state.getLogger().writeCurrent("Oracle Result:");
                state.getLogger().writeCurrent(oracleResult.toString());
            } else {
                state.getState().getLocalState().log("============Bug Report============");
                state.getState().getLocalState().log("==Oracle With Commit And Rollback==");
                for (Transaction tx : transactions) {
                    state.getState().getLocalState().log(tx.toString());
                }
                state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(o -> o.getStmtId()).
                        collect(Collectors.joining(", ", "[", "]")));
                state.getState().getLocalState().log(compareResultInfo);
                state.getState().getLocalState().log("Execution Result:");
                state.getState().getLocalState().log(testResult.toString());
                state.getState().getLocalState().log("Oracle Result:");
                state.getState().getLocalState().log(oracleResult.toString());
                detectBug = true;
            }

            compareResultInfo = compareWriteTxResults(testResult, oracleWithoutCommitAndRollbackResult);
            if (compareResultInfo.equals("")) {
                state.getLogger().writeCurrent("============Is Same============");
                state.getLogger().writeCurrent("==Oracle Without Commit And Rollback==");
                state.getLogger().writeCurrent("Execution Result:");
                state.getLogger().writeCurrent(testResult.toString());
                state.getLogger().writeCurrent("Oracle Result:");
                state.getLogger().writeCurrent(oracleWithoutCommitAndRollbackResult.toString());
            } else {
                state.getState().getLocalState().log("============Bug Report============");
                state.getState().getLocalState().log("==Oracle Without Commit And Rollback==");
                for (Transaction tx : transactions) {
                    state.getState().getLocalState().log(tx.toString());
                }
                state.getState().getLocalState().log("Input schedule: " + schedule.stream().map(o -> o.getStmtId()).
                        collect(Collectors.joining(", ", "[", "]")));
                state.getState().getLocalState().log(compareResultInfo);
                state.getState().getLocalState().log("Execution Result:");
                state.getState().getLocalState().log(testResult.toString());
                state.getState().getLocalState().log("Oracle Result:");
                state.getState().getLocalState().log(oracleWithoutCommitAndRollbackResult.toString());
                detectBug = true;
            }
            if (detectBug) {
                throw new AssertionError("Transaction execution mismatches its oracle");
            }
        } finally {
            for (Transaction tx : transactions) {
                tx.closeConnection();
            }
        }
        System.exit(0);
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

    public List<TxStatement> genOracleWithoutCommitAndRollbackSchedule(
            TxTestExecutionResult testResult) {
        List<TxStatement> oracleSchedule = new ArrayList<>();
        for (TxStatementExecutionResult stmtResult : testResult.getStatementExecutionResults()) {
            Transaction stmtTx = stmtResult.getStatement().getTransaction();
            if (stmtResult.reportDeadlock()) {
                continue;
            } else if (stmtResult.getStatement().getType() == TiDBTxStatement.TiDBStatementType.COMMIT) {
                // We remove BEGIN and COMMIT statements for COMMIT transactions
                List<TxStatement> txStatements = new ArrayList<>(stmtTx.getStatements());
                txStatements.remove(0);
                txStatements.remove(txStatements.size() - 1);
                oracleSchedule.addAll(txStatements);
            }
        }
        return oracleSchedule;
    }

    private Transaction readTransactionFromScanner(Scanner input) throws SQLException {
        Transaction transaction = new Transaction(state.createConnection());
        List<TxStatement> statementList = new ArrayList<>();
        String txId = input.nextLine();
        transaction.setId(Integer.parseInt(txId));
        String sql;
        do {
            if (!input.hasNext()) break;
            sql = input.nextLine();
            if (sql.equals("") || sql.equals("END")) break;
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            TiDBErrors.addInsertErrors(errors);
            TiDBErrors.addExpressionHavingErrors(errors);
            errors.add("Deadlock found");
            errors.add("try again later");
            TxSQLQueryAdapter txStatement = new TxSQLQueryAdapter(sql, errors);
            TxStatement cell = new TiDBTxStatement(transaction, txStatement);
            statementList.add(cell);
        } while (true);
        transaction.setStatements(statementList);
        return transaction;
    }
}
