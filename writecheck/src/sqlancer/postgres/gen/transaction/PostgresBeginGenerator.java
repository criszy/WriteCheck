package sqlancer.postgres.gen.transaction;

import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;

public class PostgresBeginGenerator {

    public static SQLQueryAdapter getQuery(PostgresGlobalState state) {
        ExpectedErrors errors = new ExpectedErrors();
        errors.add("SET TRANSACTION ISOLATION LEVEL must be called before any query");
        return new SQLQueryAdapter("BEGIN", errors);
    }
}
