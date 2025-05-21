package sqlancer.mariadb.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider;

import java.sql.SQLException;

public class MariaDBSelectForUpdateGenerator {

    public static SQLQueryAdapter getQuery(MariaDBProvider.MariaDBGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = MariaDBSelectGenerator.getQuery(globalState);
        String selectForUpdateStatement = selectStatement.getQueryString().replace(";", "") + " FOR UPDATE;";
        return new SQLQueryAdapter(selectForUpdateStatement, selectStatement.getExpectedErrors());
    }

}
