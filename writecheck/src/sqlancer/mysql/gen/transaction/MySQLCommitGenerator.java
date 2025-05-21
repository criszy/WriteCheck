package sqlancer.mysql.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

import java.sql.SQLException;

public class MySQLCommitGenerator {

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("COMMIT");
    }
}
