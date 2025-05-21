package sqlancer.mariadb.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.transaction.IsolationLevel;

import java.sql.SQLException;

public class MariaDBIsolationLevelGenerator {

    private final IsolationLevel isolationLevel;

    public MariaDBIsolationLevelGenerator(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public SQLQueryAdapter getQuery() throws SQLException {
        String sql = "SET SESSION TRANSACTION ISOLATION LEVEL " + isolationLevel.getName();
        return new SQLQueryAdapter(sql);
    }

}
