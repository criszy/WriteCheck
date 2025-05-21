package sqlancer.postgres.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

import java.sql.SQLException;

public class PostgresSelectForUpdateGenerator {

    public static SQLQueryAdapter getQuery(PostgresGlobalState globalState) throws SQLException {
        SQLQueryAdapter selectStatement = PostgresSelectGenerator.getQuery(globalState);
        String selectForUpdateStatement = selectStatement.getQueryString().replace(";", "") + " FOR UPDATE;";
        return new SQLQueryAdapter(selectForUpdateStatement, selectStatement.getExpectedErrors());
    }
}
