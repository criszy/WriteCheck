package sqlancer.sqlite3.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.schema.SQLite3Schema;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class SQLite3SelectGenerator {

    public static SQLQueryAdapter getQuery(SQLite3GlobalState globalState) throws SQLException {
        SQLite3Select selectStatement = new SQLite3Select();
        SQLite3Schema.SQLite3Tables selectTables = globalState.getSchema().getRandomTableNonEmptyTables();
        // TODO: choose subset of table columns
        List<SQLite3Expression> fetchColumns = Arrays.asList(new SQLite3Expression.SQLite3ColumnName(selectTables.getColumns().get(0), null));
        selectStatement.setFetchColumns(fetchColumns);
        SQLite3ExpressionGenerator gen = new SQLite3ExpressionGenerator(globalState).setColumns(selectTables.getColumns());
        List<SQLite3Schema.SQLite3Table> tables = selectTables.getTables();
//        if (Randomly.getBoolean()) {
//            SQLite3HintGenerator.generateHints(selectStatement, tables);
//        }
        List<SQLite3Expression> tableList = tables.stream().map(SQLite3Expression.SQLite3TableReference::new).collect(Collectors.toList());
//        List<SQLite3Expression> joins = SQLite3Join.getJoins(tableList, globalState);
        // TODO: randomly generate join operation
//        selectStatement.setJoinList(joins);
        selectStatement.setFromList(tableList);
        selectStatement.setWhereClause(null);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generatePredicate());
        }
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            selectStatement.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
//        }
        String queryString = SQLite3Visitor.asString(selectStatement);

        ExpectedErrors errors = new ExpectedErrors();
        SQLite3Errors.addExpectedExpressionErrors(errors);
//        SQLite3Errors.addExpressionHavingErrors(errors);
        return new SQLQueryAdapter(queryString, errors);
    }

}
