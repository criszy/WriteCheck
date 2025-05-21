package sqlancer.cockroachdb.gen;

import sqlancer.Randomly;
import sqlancer.cockroachdb.CockroachDBErrors;
import sqlancer.cockroachdb.CockroachDBProvider;
import sqlancer.cockroachdb.CockroachDBSchema;
import sqlancer.cockroachdb.CockroachDBVisitor;
import sqlancer.cockroachdb.ast.CockroachDBColumnReference;
import sqlancer.cockroachdb.ast.CockroachDBExpression;
import sqlancer.cockroachdb.ast.CockroachDBSelect;
import sqlancer.cockroachdb.ast.CockroachDBTableReference;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CockroachDBSelectGenerator {

    public static SQLQueryAdapter getQuery(CockroachDBProvider.CockroachDBGlobalState globalState) throws SQLException {
        CockroachDBSelect selectStatement = new CockroachDBSelect();
        CockroachDBSchema.CockroachDBTables selectTables = globalState.getSchema().getRandomTableNonEmptyTables();
        // TODO: choose subset of table columns
        List<CockroachDBExpression> fetchColumns = Arrays.asList(new CockroachDBColumnReference(selectTables.getColumns().get(0)));
        selectStatement.setFetchColumns(fetchColumns);
        CockroachDBExpressionGenerator gen = new CockroachDBExpressionGenerator(globalState).setColumns(selectTables.getColumns());
        List<CockroachDBSchema.CockroachDBTable> tables = selectTables.getTables();
//        if (Randomly.getBoolean()) {
//            CockroachDBHintGenerator.generateHints(selectStatement, tables);
//        }
        List<CockroachDBExpression> tableList = tables.stream().map(CockroachDBTableReference::new).collect(Collectors.toList());
//        List<CockroachDBExpression> joins = CockroachDBJoin.getJoins(tableList, globalState);
//        // TODO: randomly generate join operation
//        selectStatement.setJoinList(joins);
        selectStatement.setFromList(tableList);
        selectStatement.setWhereClause(null);
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setOrderByExpressions(gen.generateOrderBys());
        }
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(gen.generatePredicate());
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setGroupByExpressions(gen.generateExpressions(Randomly.smallNumber() + 1));
        }
        String queryString = CockroachDBVisitor.asString(selectStatement);

        ExpectedErrors errors = new ExpectedErrors();
        CockroachDBErrors.addExpressionErrors(errors);
        CockroachDBErrors.addTransactionErrors(errors);
        errors.add("duplicate key value");
        return new SQLQueryAdapter(queryString, errors);
    }

}
