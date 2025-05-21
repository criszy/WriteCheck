package sqlancer.mysql.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

import java.sql.SQLException;

public class MySQLRollbackGenerator {

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("ROLLBACK");
    }
}
