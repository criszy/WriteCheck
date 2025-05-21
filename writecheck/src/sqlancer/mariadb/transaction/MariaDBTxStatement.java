package sqlancer.mariadb.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementType;

public class MariaDBTxStatement extends TxStatement {

    public enum MariaDBStatementType implements TxStatementType {
        BEGIN, COMMIT, ROLLBACK,
        SELECT, SELECT_FOR_UPDATE, SELECT_FOR_SHARE,
        UPDATE, DELETE, INSERT, REPLACE,
        SET,
        UNKNOWN

    }

    public MariaDBTxStatement(Transaction transaction, TxSQLQueryAdapter txQueryAdapter) {
        super(transaction, txQueryAdapter);
    }

    public MariaDBTxStatement(Transaction transaction, SQLQueryAdapter queryAdapter) {
        this(transaction, new TxSQLQueryAdapter(queryAdapter));
    }

    @Override
    public boolean isSelectType() {
        return this.type == MariaDBTxStatement.MariaDBStatementType.SELECT
                || this.type == MariaDBStatementType.SELECT_FOR_SHARE
                || this.type == MariaDBTxStatement.MariaDBStatementType.SELECT_FOR_UPDATE;
    }

    @Override
    public boolean isNoErrorType() {
        return this.type == MariaDBTxStatement.MariaDBStatementType.BEGIN
                || this.type == MariaDBStatementType.COMMIT
                || this.type == MariaDBTxStatement.MariaDBStatementType.ROLLBACK;
    }

    @Override
    public boolean isEndTxType() {
        return this.type == MariaDBStatementType.COMMIT || this.type == MariaDBStatementType.ROLLBACK;
    }

    @Override
    protected void setStatementType() {
        String stmt = txQueryAdapter.getQueryString().replace(";", "").toUpperCase();
        MariaDBTxStatement.MariaDBStatementType realType = MariaDBTxStatement.MariaDBStatementType.valueOf(stmt.split(" ")[0]);
        if (realType == MariaDBTxStatement.MariaDBStatementType.SELECT) {
            int forIdx = stmt.indexOf("FOR UPDATE");
            if (forIdx == -1) {
                forIdx = stmt.indexOf("LOCK IN SHARE MODE");
            }
            if (forIdx != -1) {
                String postfix = stmt.substring(forIdx);
                if (postfix.equals("FOR UPDATE")) {
                    realType = MariaDBTxStatement.MariaDBStatementType.SELECT_FOR_UPDATE;
                } else if (postfix.equals("LOCK IN SHARE MODE")) {
                    realType = MariaDBTxStatement.MariaDBStatementType.SELECT_FOR_SHARE;
                }else {
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
        return errorInfo.contains("Deadlock") || errorInfo.contains("lock=true");
    }

    @Override
    protected boolean reportRollback(String errorInfo) {
        return false;
    }
}
