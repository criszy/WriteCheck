package sqlancer.mariadb.gen;

import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema;

public final class MariaDBTruncateGenerator {

    private MariaDBTruncateGenerator() {
    }

    public static SQLQueryAdapter truncate(MariaDBGlobalState globalState) {
        MariaDBSchema s = globalState.getSchema();
        StringBuilder sb = new StringBuilder("TRUNCATE ");
        sb.append(s.getRandomTable().getName());
        sb.append(" ");
        MariaDBCommon.addWaitClause(sb);
        return new SQLQueryAdapter(sb.toString());
    }

}
