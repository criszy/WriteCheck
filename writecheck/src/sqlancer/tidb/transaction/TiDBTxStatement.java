package sqlancer.tidb.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementType;

public class TiDBTxStatement extends TxStatement {

    public enum TiDBStatementType implements TxStatementType {
        BEGIN, BEGIN_OPTIMISTIC, COMMIT, ROLLBACK,
        SELECT, SELECT_FOR_UPDATE,
        UPDATE, DELETE, INSERT, REPLACE,
        SET,
        UNKNOWN

    }

    public TiDBTxStatement(Transaction transaction, TxSQLQueryAdapter txQueryAdapter) {
        super(transaction, txQueryAdapter);
    }

    public TiDBTxStatement(Transaction transaction, SQLQueryAdapter queryAdapter) {
        this(transaction, new TxSQLQueryAdapter(queryAdapter));
    }

    @Override
    public boolean isSelectType() {
        return this.type == TiDBStatementType.SELECT || this.type == TiDBStatementType.SELECT_FOR_UPDATE;
    }

    @Override
    public boolean isNoErrorType() {
        return false;
    }

    @Override
    public boolean isEndTxType() {
        return this.type == TiDBStatementType.COMMIT || this.type == TiDBStatementType.ROLLBACK;
    }

    @Override
    protected void setStatementType() {
        String stmt = txQueryAdapter.getQueryString().replace(";", "").toUpperCase();
        TiDBStatementType realType = TiDBStatementType.valueOf(stmt.split(" ")[0]);
        if (realType == TiDBStatementType.BEGIN) {
            if (stmt.contains("OPTIMISTIC")) {
                realType = TiDBStatementType.BEGIN_OPTIMISTIC;
            }
        }
        if (realType == TiDBStatementType.SELECT) {
            int forIdx = stmt.indexOf("FOR ");
            if (forIdx != -1) {
                String postfix = stmt.substring(forIdx);
                // not implement FOR SHARE, since TiDB does not support FOR SHARE
                if (postfix.equals("FOR UPDATE")) {
                    realType = TiDBStatementType.SELECT_FOR_UPDATE;
                } else {
                    throw new RuntimeException("Invalid postfix: " + txQueryAdapter.getQueryString());
                }
            }
        }
        this.type = realType;
    }

    @Override
    protected boolean reportDeadlock(String errorInfo) {
        if (errorInfo == null) {
            return false;
        }
        // TODO: confirm "lock=true"
        return errorInfo.contains("Deadlock") || errorInfo.contains("lock=true")
                || errorInfo.contains("try again later");
    }

    @Override
    protected boolean reportRollback(String errorInfo) {
        return false;
    }
}
