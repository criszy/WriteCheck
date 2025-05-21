package sqlancer.mysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLGlobalState;

import java.sql.SQLException;

public class MySQLSelectForShareGenerator {

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = MySQLSelectGenerator.getQuery(globalState);
        String keywords = " FOR SHARE;";
        if (Randomly.getBoolean()) {
            keywords = " LOCK IN SHARE MODE;";
        }
        String selectForUpdateStatement = selectStatement.getQueryString().replace(";", "") + keywords;
        return new SQLQueryAdapter(selectForUpdateStatement, selectStatement.getExpectedErrors());
    }
}
