package sqlancer.postgres.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

import java.sql.SQLException;

public class PostgresRollbackGenerator {

    public static SQLQueryAdapter getQuery(PostgresGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("ROLLBACK");
    }
}
