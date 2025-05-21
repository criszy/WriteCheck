package sqlancer.sqlite3.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.IsolationLevel;

import java.sql.SQLException;

public class SQLite3IsolationLevelGenerator {

    private final IsolationLevel isolationLevel;

    public SQLite3IsolationLevelGenerator(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public SQLQueryAdapter getQuery() throws SQLException {
        String sql;
        if (isolationLevel.getName().equals("READ UNCOMMITTED"))
            sql = "PRAGMA read_uncommitted = true";
        else
            sql = "PRAGMA read_uncommitted = false";
        return new SQLQueryAdapter(sql);

    }
}
