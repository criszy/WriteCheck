package sqlancer.tidb.gen.transaction;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.tidb.TiDBProvider;

import java.sql.SQLException;

public class TiDBBeginOptimisticGenerator {

    public static SQLQueryAdapter getQuery(TiDBProvider.TiDBGlobalState globalState) throws SQLException {
        return new SQLQueryAdapter("BEGIN OPTIMISTIC");
    }
}
