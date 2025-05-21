package sqlancer.mariadb.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider;

import java.sql.SQLException;

public class MariaDBSelectForShareGenerator {

    public static SQLQueryAdapter getQuery(MariaDBProvider.MariaDBGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = MariaDBSelectGenerator.getQuery(globalState);
        String selectForShareStatement = selectStatement.getQueryString().replace(";", "") + " LOCK IN SHARE MODE;";
        return new SQLQueryAdapter(selectForShareStatement, selectStatement.getExpectedErrors());
    }

}
