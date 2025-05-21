package sqlancer.postgres.gen;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.postgres.PostgresGlobalState;
import sqlancer.postgres.PostgresSchema.PostgresColumn;
import sqlancer.postgres.PostgresSchema.PostgresDataType;
import sqlancer.postgres.PostgresSchema.PostgresTable;
import sqlancer.postgres.PostgresSchema.PostgresTables;
import sqlancer.postgres.PostgresVisitor;
import sqlancer.postgres.ast.PostgresColumnValue;
import sqlancer.postgres.ast.PostgresConstant;
import sqlancer.postgres.ast.PostgresExpression;
import sqlancer.postgres.ast.PostgresSelect;
import sqlancer.postgres.oracle.PostgresNoRECOracle;

public class PostgresSelectGenerator {

    public static SQLQueryAdapter getQuery(PostgresGlobalState globalState) throws SQLException {
        PostgresSelect selectStatement = new PostgresSelect();
        PostgresTables randomTables = globalState.getSchema().getRandomTableNonEmptyTables();
        List<PostgresColumn> columns = randomTables.getColumns();
        PostgresExpressionGenerator gen = new PostgresExpressionGenerator(globalState).setColumns(columns);
        PostgresExpression whereCondition = gen.generateExpression(PostgresDataType.BOOLEAN);
        List<PostgresTable> tables = randomTables.getTables();
        selectStatement.setFromList(tables.stream().map(t -> new PostgresSelect.PostgresFromTable(t, false))
                .collect(Collectors.toList()));
        selectStatement.setFetchColumns(generateFetchColumns(columns));
        selectStatement.setWhereClause(null);
        if (Randomly.getBoolean()) {
            selectStatement.setWhereClause(whereCondition);
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setJoinClauses(PostgresNoRECOracle.getJoinStatements(globalState, columns, tables));
        }
        PostgresExpression limitClause = generateLimit();
        if (Randomly.getBooleanWithRatherLowProbability()) {
            selectStatement.setLimitClause(limitClause);
        }
        if (limitClause != null) {
            PostgresExpression offsetClause = generateOffset();
            selectStatement.setOffsetClause(offsetClause);
        }
        if (Randomly.getBooleanWithRatherLowProbability()) {
            List<PostgresExpression> orderBy = new PostgresExpressionGenerator(globalState).setColumns(columns)
                    .generateOrderBy();
            selectStatement.setOrderByExpressions(orderBy);
        }
        ExpectedErrors errors = new ExpectedErrors();
        PostgresCommon.addCommonExpressionErrors(errors);
        PostgresCommon.addCommonFetchErrors(errors);
        errors.add("permission denied for");
        errors.add("does not exist");
        errors.add("FOR UPDATE cannot");
        errors.add("specified more than once");
        errors.add("could not serialize");
        return new SQLQueryAdapter(PostgresVisitor.asString(selectStatement), errors);
    }

    private static List<PostgresExpression> generateFetchColumns(List<PostgresColumn> columns) {
        if (Randomly.getBooleanWithRatherLowProbability()) {
            return Arrays.asList(new PostgresColumnValue(PostgresColumn.createDummy("*"), null));
        }
        List<PostgresExpression> fetchColumns = new ArrayList<>();
        List<PostgresColumn> targetColumns = Randomly.nonEmptySubset(columns);
        for (PostgresColumn c : targetColumns) {
            fetchColumns.add(new PostgresColumnValue(c, null));
        }
        return fetchColumns;
    }

    private static PostgresConstant generateLimit() {
        if (Randomly.getBoolean()) {
            return PostgresConstant.createIntConstant(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }

    private static PostgresExpression generateOffset() {
        if (Randomly.getBoolean()) {
            return PostgresConstant.createIntConstant(0);
        } else {
            return null;
        }
    }
}
