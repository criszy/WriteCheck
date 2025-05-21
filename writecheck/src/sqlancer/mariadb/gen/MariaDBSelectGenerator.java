package sqlancer.mariadb.gen;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBErrors;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.MariaDBSchema.MariaDBTable;
import sqlancer.mariadb.MariaDBSchema.MariaDBTables;
import sqlancer.mariadb.ast.MariaDBColumnName;
import sqlancer.mariadb.ast.MariaDBExpression;
import sqlancer.mariadb.ast.MariaDBSelectStatement;
import sqlancer.mariadb.ast.MariaDBVisitor;

public class MariaDBSelectGenerator {

    public static SQLQueryAdapter getQuery(MariaDBGlobalState globalState) throws SQLException {
        MariaDBSelectStatement selectStatement = new MariaDBSelectStatement();
        MariaDBTables selectTables = globalState.getSchema().getRandomTableNonEmptyTables();
        // TODO: choose subset of table columns
        List<MariaDBExpression> fetchColumns = Arrays.asList(new MariaDBColumnName(selectTables.getColumns().get(0)));
        selectStatement.setFetchColumns(fetchColumns);
        MariaDBExpressionGenerator gen = new MariaDBExpressionGenerator(globalState.getRandomly()).setColumns(selectTables.getColumns());
        List<MariaDBTable> tables = selectTables.getTables();
//        if (Randomly.getBoolean()) {
//            MariaDBHintGenerator.generateHints(selectStatement, tables);
//        }
//        List<MariaDBExpression> joins = MariaDBJoin.getJoins(tableList, globalState);
//        // TODO: randomly generate join operation
//        selectStatement.setJoinList(joins);
        selectStatement.setFromTables(tables);
        selectStatement.setWhereClause(null);
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            selectStatement.setOrderByExpressions(gen.generateOrderBys());
//        }
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generatePredicate());
        }
//        if (Randomly.getBooleanWithRatherLowProbability()) {
//            selectStatement.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
//        }
        String queryString = MariaDBVisitor.asString(selectStatement);

        ExpectedErrors errors = new ExpectedErrors();
        MariaDBErrors.addExpressionErrors(errors);
//        MariaDBErrors.addExpressionHavingErrors(errors);
        return new SQLQueryAdapter(queryString, errors);
    }

}
