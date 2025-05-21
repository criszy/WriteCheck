package sqlancer.sqlite3.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxSQLQueryAdapter;
import sqlancer.common.transaction.TxStatement;
import sqlancer.common.transaction.TxStatementType;

public class SQLite3TxStatement extends TxStatement {

    public enum SQLite3StatementType implements TxStatementType {
        BEGIN, COMMIT, ROLLBACK,
        SELECT,
        UPDATE, DELETE, INSERT, REPLACE,
        PRAGMA,
        UNKNOWN

    }

    public SQLite3TxStatement(Transaction transaction, TxSQLQueryAdapter txQueryAdapter) {
        super(transaction, txQueryAdapter);
    }

    public SQLite3TxStatement(Transaction transaction, SQLQueryAdapter queryAdapter) {
        this(transaction, new TxSQLQueryAdapter(queryAdapter));
    }

    @Override
    public boolean isSelectType() {
        return this.type == SQLite3TxStatement.SQLite3StatementType.SELECT;
    }

    @Override
    public boolean isNoErrorType() {
        return false;
    }

    @Override
    public boolean isEndTxType() {
        return this.type == SQLite3StatementType.COMMIT || this.type == SQLite3StatementType.ROLLBACK;
    }

    @Override
    protected void setStatementType() {
        String stmt = txQueryAdapter.getQueryString().replace(";", "").toUpperCase();
        SQLite3TxStatement.SQLite3StatementType realType = SQLite3TxStatement.SQLite3StatementType.valueOf(stmt.split(" ")[0]);
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
