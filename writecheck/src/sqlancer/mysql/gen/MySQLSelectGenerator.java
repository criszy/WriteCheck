package sqlancer.mysql.gen;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mysql.MySQLErrors;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLColumn;
import sqlancer.mysql.MySQLSchema.MySQLTable;
import sqlancer.mysql.MySQLSchema.MySQLTables;
import sqlancer.mysql.MySQLVisitor;
import sqlancer.mysql.ast.MySQLColumnReference;
import sqlancer.mysql.ast.MySQLExpression;
import sqlancer.mysql.ast.MySQLSelect;
import sqlancer.mysql.ast.MySQLTableReference;

public class MySQLSelectGenerator {

    public static SQLQueryAdapter getQuery(MySQLGlobalState globalState) throws SQLException {
        MySQLSelect selectStatement = new MySQLSelect();
        MySQLTables randomFromTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<MySQLTable> tables = randomFromTables.getTables();
        List<MySQLColumn> columns = randomFromTables.getColumns();
        MySQLExpressionGenerator gen = new MySQLExpressionGenerator(globalState).setColumns(columns);
        selectStatement.setSelectType(Randomly.fromOptions(MySQLSelect.SelectType.values()));
        List<MySQLExpression> fetchColumns = Arrays.asList(MySQLColumnReference.create(
                randomFromTables.getColumns().get(0), null));
        selectStatement.setFetchColumns(fetchColumns);
        List<MySQLExpression> tableList = tables.stream().map(t -> new MySQLTableReference(t))
                .collect(Collectors.toList());
        selectStatement.setFromList(tableList);
        selectStatement.setWhereClause(null);
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generateExpression());
        }
        ExpectedErrors errors = new ExpectedErrors();
        MySQLErrors.addExpressionErrors(errors);
        return new SQLQueryAdapter(MySQLVisitor.asString(selectStatement), errors);
    }
}
