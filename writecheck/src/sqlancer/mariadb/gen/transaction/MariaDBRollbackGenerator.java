package sqlancer.mariadb.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider;

import java.sql.SQLException;

public class MariaDBRollbackGenerator {

    public static SQLQueryAdapter getQuery(MariaDBProvider.MariaDBGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("ROLLBACK");
    }

}
