package sqlancer.mariadb;

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
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;
import sqlancer.mariadb.gen.MariaDBIndexGenerator;
import sqlancer.mariadb.gen.MariaDBInsertGenerator;
import sqlancer.mariadb.gen.MariaDBSetGenerator;
import sqlancer.mariadb.gen.MariaDBTableAdminCommandGenerator;
import sqlancer.mariadb.gen.MariaDBTableGenerator;
import sqlancer.mariadb.gen.MariaDBTruncateGenerator;
import sqlancer.mariadb.gen.MariaDBUpdateGenerator;

@AutoService(DatabaseProvider.class)
public class MariaDBProvider extends SQLProviderAdapter<MariaDBGlobalState, MariaDBOptions> {

    public static final int MAX_EXPRESSION_DEPTH = 3;

    public MariaDBProvider() {
        super(MariaDBGlobalState.class, MariaDBOptions.class);
    }

    enum Action implements AbstractAction<MariaDBGlobalState> {
        ANALYZE_TABLE(MariaDBTableAdminCommandGenerator::analyzeTable), //
        CHECKSUM(MariaDBTableAdminCommandGenerator::checksumTable), //
        CHECK_TABLE(MariaDBTableAdminCommandGenerator::checkTable), //
        CREATE_INDEX(MariaDBIndexGenerator::generate), //
        INSERT(MariaDBInsertGenerator:: getQuery), //
        OPTIMIZE(MariaDBTableAdminCommandGenerator::optimizeTable), //
        REPAIR_TABLE(MariaDBTableAdminCommandGenerator::repairTable), //
        SET(MariaDBSetGenerator::set), //
        TRUNCATE(MariaDBTruncateGenerator::truncate), //
        UPDATE(MariaDBUpdateGenerator::getQuery); //

        private final SQLQueryProvider<MariaDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<MariaDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(MariaDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(MariaDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
            case CHECKSUM:
            case CHECK_TABLE:
            case TRUNCATE:
            case REPAIR_TABLE:
            case OPTIMIZE:
            case ANALYZE_TABLE:
            case UPDATE:
            case CREATE_INDEX:
                nrPerformed = r.getInteger(0, 2);
                break;
            case SET:
                nrPerformed = 20;
                break;
            case INSERT:
                nrPerformed = r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    public enum DatabaseInitStmt implements AbstractAction<MariaDBGlobalState> {
        INSERT(Action.INSERT),
        CREATE_INDEX(Action.CREATE_INDEX);

        private final Action sqlQueryProvider;

        DatabaseInitStmt(Action sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        public SQLQueryAdapter getQuery(MariaDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapDatabaseInitStmt(MariaDBGlobalState globalState, DatabaseInitStmt a) {
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
    public void generateDatabase(MariaDBGlobalState globalState) throws Exception {
        if (globalState.useTxReproduce()) {
            return;
        }

        if (globalState.useWriteCheck()) {
            int tableNum = Randomly.fromOptions(1, 5);
            for (int i = 0; i < tableNum; ) {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = MariaDBTableGenerator.generate(tableName, globalState.getRandomly(),
                        globalState.getSchema());
                boolean success = globalState.executeStatement(createTable);
                if (success) {
                    globalState.updateSchema();
                    StatementExecutor<MariaDBGlobalState, DatabaseInitStmt> se = new StatementExecutor<>(globalState, DatabaseInitStmt.values(),
                            MariaDBProvider::mapDatabaseInitStmt, (q) -> {
                        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                            throw new IgnoreMeException();
                        }
                    });
                    se.executeStatements();
                    i++;
                }
            }

        } else {
            while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
                String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter createTable = MariaDBTableGenerator.generate(tableName, globalState.getRandomly(),
                        globalState.getSchema());
                globalState.executeStatement(createTable);
            }

            StatementExecutor<MariaDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                    MariaDBProvider::mapActions, (q) -> {
                if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                    throw new IgnoreMeException();
                }
            });
            se.executeStatements();
        }

    }

    public static class MariaDBGlobalState extends SQLGlobalState<MariaDBOptions, MariaDBSchema> {

        @Override
        protected MariaDBSchema readSchema() throws SQLException {
            return MariaDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

        public SQLConnection createConnection() throws SQLException {
            String host = getOptions().getHost();
            int port = getOptions().getPort();
            if (host == null) {
                host = MariaDBOptions.DEFAULT_HOST;
            }
            if (port == MainOptions.NO_SET_PORT) {
                port = MariaDBOptions.DEFAULT_PORT;
            }
            String databaseName = getDatabaseName();
            String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
            Connection con = DriverManager.getConnection(url, getOptions().getUserName(),
                    getOptions().getPassword());
            return new SQLConnection(con);
        }

        public boolean useTxReproduce() {
            return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MariaDBOptions.MariaDBOracleFactory.WRITE_CHECK_REPRODUCE);
        }

        public boolean useWriteCheck() {
            return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MariaDBOptions.MariaDBOracleFactory.WRITE_CHECK);
        }

    }

    @Override
    public SQLConnection createDatabase(MariaDBGlobalState globalState) throws SQLException {
        globalState.getState().logStatement("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        globalState.getState().logStatement("CREATE DATABASE " + globalState.getDatabaseName());
        globalState.getState().logStatement("USE " + globalState.getDatabaseName());
        String username = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) {
            host = MariaDBOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MariaDBOptions.DEFAULT_PORT;
        }
        String url = String.format("jdbc:mariadb://%s:%d", host, port);
        Connection con = DriverManager.getConnection(url, username, password);
        try (Statement s = con.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("CREATE DATABASE " + globalState.getDatabaseName());
        }
        try (Statement s = con.createStatement()) {
            s.execute("USE " + globalState.getDatabaseName());
        }
        return new SQLConnection(con);
    }

    @Override
    public String getDBMSName() {
        return "mariadb";
    }

}
