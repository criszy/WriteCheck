package sqlancer.tidb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.gen.TiDBAlterTableGenerator;
import sqlancer.tidb.gen.TiDBAnalyzeTableGenerator;
import sqlancer.tidb.gen.TiDBDeleteGenerator;
import sqlancer.tidb.gen.TiDBIndexGenerator;
import sqlancer.tidb.gen.TiDBInsertGenerator;
import sqlancer.tidb.gen.TiDBRandomQuerySynthesizer;
import sqlancer.tidb.gen.TiDBTableGenerator;
import sqlancer.tidb.gen.TiDBSetGenerator;
import sqlancer.tidb.gen.TiDBUpdateGenerator;
import sqlancer.tidb.gen.TiDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class TiDBProvider extends SQLProviderAdapter<TiDBGlobalState, TiDBOptions> {

    public TiDBProvider() {
        super(TiDBGlobalState.class, TiDBOptions.class);
    }

    public enum Action implements AbstractAction<TiDBGlobalState> {
        INSERT(TiDBInsertGenerator::getQuery), //
        ANALYZE_TABLE(TiDBAnalyzeTableGenerator::getQuery), //
        TRUNCATE((g) -> new SQLQueryAdapter("TRUNCATE " + g.getSchema().getRandomTable(t -> !t.isView()).getName())), //
        CREATE_INDEX(TiDBIndexGenerator::getQuery), //
        DELETE(TiDBDeleteGenerator::getQuery), //
        SET(TiDBSetGenerator::getQuery), // TODO: eliminate error report
        UPDATE(TiDBUpdateGenerator::getQuery), //
        ADMIN_CHECKSUM_TABLE(
                (g) -> new SQLQueryAdapter("ADMIN CHECKSUM TABLE " + g.getSchema().getRandomTable().getName())), // TODO: eliminate error report
        VIEW_GENERATOR(TiDBViewGenerator::getQuery), //
        ALTER_TABLE(TiDBAlterTableGenerator::getQuery), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            TiDBErrors.addExpressionErrors(errors);
            TiDBErrors.addExpressionHavingErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + TiDBRandomQuerySynthesizer.generate(g, Randomly.smallNumber() + 1).getQueryString(),
                    errors);
        });

        private final SQLQueryProvider<TiDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TiDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    public static class TiDBGlobalState extends SQLGlobalState<TiDBOptions, TiDBSchema> {

        @Override
        protected TiDBSchema readSchema() throws SQLException {
            return TiDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

        public SQLConnection createConnection() throws SQLException {
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            if (host == null) {
                host = TiDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = TiDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();
            String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
            Connection con = DriverManager.getConnection(url, getOptions().getUserName(),
                    getOptions().getPassword());
            return new SQLConnection(con);
        }

        public boolean useTxReproduce() {
            return getDbmsSpecificOptions().oracle.stream().anyMatch(o -> o == TiDBOptions.TiDBOracleFactory.WRITE_CHECK_REPRODUCE
                    || o == TiDBOptions.TiDBOracleFactory.OPT_WRITE_CHECK_REPRODUCE);
        }

        public boolean useWriteCheck() {
            return getDbmsSpecificOptions().oracle.stream().anyMatch(o -> o == TiDBOptions.TiDBOracleFactory.WRITE_CHECK
                    || o == TiDBOptions.TiDBOracleFactory.OPT_WRITE_CHECK);
        }

    }

    private static int mapActions(TiDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case ANALYZE_TABLE:
        case CREATE_INDEX:
            return r.getInteger(0, 2);
        case INSERT:
        case EXPLAIN:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case TRUNCATE:
        case DELETE:
        case ADMIN_CHECKSUM_TABLE:   //  TODO: eliminate error report
            return r.getInteger(0, 2);
        case SET:   //  TODO: eliminate error report
        case UPDATE:
            return r.getInteger(0, 5);
        case VIEW_GENERATOR:
            // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/8
            return r.getInteger(0, 2);
        case ALTER_TABLE:
            return r.getInteger(0, 10); // https://github.com/tidb-challenge-program/bug-hunting-issue/issues/10
        default:
            throw new AssertionError(a);
        }

    }

    private enum DatabaseInitStmt implements AbstractAction<TiDBGlobalState> {
        INSERT(Action.INSERT),
        CREATE_INDEX(Action.CREATE_INDEX);

        private final Action sqlQueryProvider;

        DatabaseInitStmt(Action sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(TiDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapDatabaseInitStmt(TiDBGlobalState globalState, DatabaseInitStmt a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
            case CREATE_INDEX:
                return r.getInteger(0, 3);
            case INSERT:
                return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
            default:
                throw new AssertionError(a);
        }

    }

    @Override
    public void generateDatabase(TiDBGlobalState globalState) throws Exception {
        if (globalState.useTxReproduce()) {
            return;
        }

        if (globalState.useWriteCheck()) {
            int tableNum = Randomly.fromOptions(1, 5);
            for (int i = 0; i < tableNum; ) {
                SQLQueryAdapter createTable = new TiDBTableGenerator().getQuery(globalState);
                boolean success = globalState.executeStatement(createTable);
                if (success) {
                    globalState.updateSchema();
                    StatementExecutor<TiDBGlobalState, DatabaseInitStmt> se = new StatementExecutor<>(globalState, DatabaseInitStmt.values(),
                            TiDBProvider::mapDatabaseInitStmt, (q) -> {
                        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
                    try {
                        se.executeStatements();
                    } catch (SQLException e) {
                        if (e.getMessage().contains(
                                "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                            throw new IgnoreMeException(); // TODO: drop view instead
                        } else {
                            throw new AssertionError(e);
                        }
                    }
                    i++;
                }
            }

        } else {
            int tableNum = Randomly.fromOptions(1, 2);
            for (int i = 0; i < tableNum; i++) {
                boolean success;
                do {
                    SQLQueryAdapter qt = new TiDBTableGenerator().getQuery(globalState);
                    success = globalState.executeStatement(qt);
                } while (!success);
            }

            StatementExecutor<TiDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                    TiDBProvider::mapActions, (q) -> {
                if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                    throw new IgnoreMeException();
                }
            });
            try {
                se.executeStatements();
            } catch (SQLException e) {
                if (e.getMessage().contains(
                        "references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them")) {
                    throw new IgnoreMeException(); // TODO: drop view instead
                } else {
                    throw new AssertionError(e);
                }
            }
        }
    }

    @Override
    public SQLConnection createDatabase(TiDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = TiDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = TiDBOptions.DEFAULT_PORT;
        }

        String databaseName = globalState.getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/", host, port);
        Connection con = DriverManager.getConnection(url, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        globalState.getState().logStatement("USE test");
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + databaseName);
        String createDatabaseCommand = "CREATE DATABASE " + databaseName;
        globalState.getState().logStatement(createDatabaseCommand);
        globalState.getState().logStatement("USE " + databaseName);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = con.createStatement()) {
            s.execute(createDatabaseCommand);
        }
        con.close();
        con = DriverManager.getConnection(url + databaseName, globalState.getOptions().getUserName(),
                globalState.getOptions().getPassword());
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "tidb";
    }
}
