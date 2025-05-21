package sqlancer.mysql.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLVisitor;

import java.util.List;

public class MySQLUpdateGenerator {

    private MySQLUpdateGenerator() {

    }

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) {
        MySQLTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(table.getColumns());
        ExpectedErrors errors = new ExpectedErrors();
        StringBuilder sb = new StringBuilder("UPDATE ");
        if (Randomly.getBoolean()) {
            sb.append("LOW_PRIORITY ");
        }
        if (Randomly.getBoolean()) {
            sb.append("IGNORE ");
        }
        sb.append(table.getName());
        sb.append(" SET ");
        List<MySQLColumn> columns = table.getRandomNonEmptyColumnSubset();
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append("=");
            if (Randomly.getBoolean()) {
                sb.append(MySQLVisitor.asString(gen.generateConstant()));
            } else {
                sb.append(MySQLVisitor.asString(gen.generateExpression()));
            }
        }
        if (Randomly.getBoolean()) {
            sb.append(" WHERE ");
            sb.append(MySQLVisitor.asString(gen.generateExpression()));
            MySQLErrors.addExpressionErrors(errors);
        }

        // MariaDB insertErrors
        errors.add("Out of range");
        errors.add("Duplicate entry"); // violates UNIQUE constraint
        errors.add("cannot be null"); // violates NOT NULL constraint
        errors.add("Incorrect integer value"); // e.g., insert TEXT into an int value
        errors.add("Data truncated for column"); // int + plus string into int
        errors.add("doesn't have a default value"); // no default value
        errors.add("The value specified for generated column"); // trying to insert into a generated column
        errors.add("Incorrect double value");
        errors.add("Incorrect string value");
        errors.add("Incorrect decimal value");
        return new SQLQueryAdapter(sb.toString(), errors);
    }
}
