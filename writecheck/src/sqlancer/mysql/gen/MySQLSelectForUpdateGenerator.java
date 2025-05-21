package sqlancer.mysql.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

import java.sql.SQLException;

public class MySQLSelectForUpdateGenerator {

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = MySQLSelectGenerator.getQuery(globalState);
        String selectForUpdateStatement = selectStatement.getQueryString().replace(";", "") + " FOR UPDATE;";
        return new SQLQueryAdapter(selectForUpdateStatement, selectStatement.getExpectedErrors());
    }
}
