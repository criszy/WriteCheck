package sqlancer.mariadb.gen;

import java.sql.SQLException;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.ast.MariaDBVisitor;

public class MariaDBDeleteGenerator {

    private MariaDBDeleteGenerator() {
    }

    public static SQLQueryAdapter getQuery(MariaDBGlobalState globalState) throws SQLException {
        ExpectedErrors errors = new ExpectedErrors();
        MariaDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState.getRandomly()).setColumns(table.getColumns());
        StringBuilder sb = new StringBuilder("DELETE ");
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("LOW_PRIORITY ");
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("QUICK ");
        }
        if (Randomly.getBooleanWithSmallProbability()) {
            sb.append("IGNORE ");
        }
        sb.append("FROM ");
        sb.append(table.getName());
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(MariaDBVisitor.asString(gen.generateExpression()));
            errors.add("Truncated incorrect");
            errors.add("Data truncation");
            errors.add("Truncated incorrect FLOAT value");
        }
        if (Randomly.getBoolean()) {
            sb.append(" ORDER BY ");
            MariaDBErrors.addExpressionErrors(errors);
            errors.add("Unknown column");
            sb.append(gen.generateOrderBys().stream().map(o -> MariaDBVisitor.asString(o))
                    .collect(Collectors.joining(", ")));
        }
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(Randomly.getNotCachedInteger(0, Integer.MAX_VALUE));
        }
        errors.add("Bad Number");
        errors.add("Truncated incorrect");
        errors.add("is not valid for CHARACTER SET");
        errors.add("Division by 0");
        errors.add("error parsing regexp");
        return new SQLQueryAdapter(sb.toString(), errors);

    }

}
