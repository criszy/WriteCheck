package sqlancer.postgres.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.IsolationLevel;

import java.sql.SQLException;

public class PostgresIsolationLevelGenerator {

    private final IsolationLevel isolationLevel;

    public PostgresIsolationLevelGenerator(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public SQLQueryAdapter getQuery() throws SQLException {
        return null;
    }
}
