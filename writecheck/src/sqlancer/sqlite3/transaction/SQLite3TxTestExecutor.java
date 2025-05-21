package sqlancer.sqlite3.transaction;

import sqlancer.common.query.SQLancerResultSet;
import sqlancer.common.transaction.*;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.gen.transaction.SQLite3IsolationLevelGenerator;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class SQLite3TxTestExecutor extends TxTestExecutor<SQLite3GlobalState> {

    public SQLite3TxTestExecutor(SQLite3GlobalState globalState, List<Transaction> transactions,
                                 List<TxStatement> schedule, SQLite3Isolation.SQLite3IsolationLevel isoLevel) {
        super(globalState, transactions, schedule, isoLevel);
    }

    @Override
    public TxTestExecutionResult execute() throws SQLException {
        ExecutorService executor = Executors.newFixedThreadPool(transactions.size());
        Map<Transaction, TxStatement> blockedTxs = new HashMap<>();
        List<Integer> committedTxIds = new ArrayList<>();

        List<TxStatement> submittedStmts = new ArrayList<>();
        List<TxStatementExecutionResult> stmtExecutionResults = new ArrayList<>();

        generateIsolationLevel();

        setTimeout();

        while (submittedStmts.size() != schedule.size()) {
            for (TxStatement curStmt : schedule) {
                if (submittedStmts.contains(curStmt)) {
                    continue;
                }

                Transaction curTx = curStmt.getTransaction();

                if (transactions.size() - committedTxIds.size() == blockedTxs.size() && blockedTxs.size() >= 1) {
                    for (Map.Entry<Transaction, TxStatement> blockedTx : blockedTxs.entrySet()) {
                        TxStatementExecutionResult stmtResult = new TxStatementExecutionResult(blockedTx.getValue());
                        stmtResult.setErrorInfo("Deadlock");
                        stmtExecutionResults.add(stmtResult);
                        TxSQLQueryAdapter rollbackStmt = new TxSQLQueryAdapter("ROLLBACK");
                        rollbackStmt.execute(blockedTx.getKey());
                        committedTxIds.add(blockedTx.getKey().getId());
                    }
                    blockedTxs.clear();
                }

                if (blockedTxs.containsKey(curTx)) {
                    continue;
                }

                if (committedTxIds.contains(curTx.getId())) {
                    if (!submittedStmts.contains(curStmt)) {
                        submittedStmts.add(curStmt);
                    }
                    continue;
                }

                submittedStmts.add(curStmt);
                Future<TxStatementExecutionResult> stmtFuture = executor.submit(new TxStmtExecutor(curStmt));
                TxStatementExecutionResult stmtResult = null;
                try {
                    stmtResult = stmtFuture.get(WAIT_THRESHOLD, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    // ignore
                } catch (ExecutionException e) {
                    throw new RuntimeException("Transaction statement returning result exception: ", e);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Transaction statement interrupted exception: ", e);
                }

                if (stmtResult != null) {
                    if(stmtResult.reportError()) {
                        if (stmtResult.getErrorInfo().contains("database is locked")) {
                            TxStatementExecutionResult blockedStmtResult = new TxStatementExecutionResult(curStmt);
                            blockedStmtResult.setBlocked(true);
                            blockedTxs.put(curTx, curStmt);
                            stmtExecutionResults.add(blockedStmtResult);
                            continue;
                        }
                    }
                    stmtExecutionResults.add(stmtResult);
                    if (stmtResult.getStatement().isEndTxType()) {
                        committedTxIds.add(stmtResult.getStatement().getTransaction().getId());
                    }

                    boolean hasResumedTxs = false;
                    Iterator<Transaction> txIterator = blockedTxs.keySet().iterator();
                    while (txIterator.hasNext()) {
                        Transaction blockedTx = txIterator.next();
                        TxStatement blockedStmt = blockedTxs.get(blockedTx);
                        Future<TxStatementExecutionResult> blockedStmtFuture = executor.submit(new TxStmtExecutor(blockedStmt));
                        TxStatementExecutionResult blockedStmtResult = null;
                        try {
                            blockedStmtResult = blockedStmtFuture.get(WAIT_THRESHOLD, TimeUnit.SECONDS);
                        } catch (TimeoutException e) {
                            // ignore
                        } catch (ExecutionException e) {
                            throw new RuntimeException("Transaction blocked statement returning result exception: ", e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException("Transaction blocked statement interrupted exception: ", e);
                        }

                        if (blockedStmtResult != null) {
                            if (blockedStmtResult.reportError()) {
                                if (blockedStmtResult.getErrorInfo().contains("database is locked")) {
                                    continue;
                                }
                            }
                            hasResumedTxs = true;
                            txIterator.remove();
                            stmtExecutionResults.add(blockedStmtResult);
                            if(blockedStmtResult.getStatement().isEndTxType()) {
                                committedTxIds.add(blockedStmtResult.getStatement().getTransaction().getId());
                            }
                        }
                    }
                    if (hasResumedTxs) {
                        break;
                    }
                }
            }
        }

        TxTestExecutionResult txResult = new TxTestExecutionResult();
        txResult.setIsolationLevel(isoLevel);
        txResult.setStatementExecutionResults(stmtExecutionResults);
        txResult.setDbFinalStates(getDBState());
        return txResult;
    }

    @Override
    protected void generateIsolationLevel() throws SQLException {
        SQLite3IsolationLevelGenerator isoLevelGenerator = new SQLite3IsolationLevelGenerator(isoLevel);
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter isoQuery = new TxSQLQueryAdapter(isoLevelGenerator.getQuery());
            isoQuery.execute(tx);
        }
    }

    @Override
    protected void setTimeout() throws SQLException {
        for (Transaction tx : transactions) {
            TxSQLQueryAdapter timeoutQuery = new TxSQLQueryAdapter("PRAGMA busy_timeout = 0");
            timeoutQuery.execute(tx);
        }
    }

    @Override
    protected SQLancerResultSet showWarnings(Transaction transaction) throws SQLException {
        return null;
    }

    @Override
    protected void handleAbortedTxn(Transaction transaction) throws SQLException {

    }

}
