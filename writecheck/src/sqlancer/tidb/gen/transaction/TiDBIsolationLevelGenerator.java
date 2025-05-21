package sqlancer.tidb.gen.transaction;

import java.sql.SQLException;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.IsolationLevel;

public class TiDBIsolationLevelGenerator {

    private final IsolationLevel isolationLevel;

    public TiDBIsolationLevelGenerator(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public SQLQueryAdapter getQuery() throws SQLException {
        String sql = "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.getName();
        return new SQLQueryAdapter(sql);
    }
}
