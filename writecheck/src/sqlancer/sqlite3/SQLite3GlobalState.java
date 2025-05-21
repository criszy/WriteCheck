package sqlancer.sqlite3;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.sqlite3.schema.SQLite3Schema;

public class SQLite3GlobalState extends SQLGlobalState<SQLite3Options, SQLite3Schema> {

    @Override
    protected SQLite3Schema readSchema() throws SQLException {
        return SQLite3Schema.fromConnection(this);
    }

    public SQLConnection createConnection() throws SQLException {
        String databaseName = getDatabaseName();
        String databaseAbsolutePath = System.getProperty("user.dir") + File.separator + "." + File.separator
                + "databases" + File.separator + databaseName + ".db";
        String url = String.format("jdbc:sqlite:%s", databaseAbsolutePath);
        Connection con = DriverManager.getConnection(url);
        return new SQLConnection(con);
    }

    public boolean useTxReproduce() {
        return getDbmsSpecificOptions().getTestOracleFactory().stream().anyMatch(o -> o == SQLite3Options.SQLite3OracleFactory.WRITE_CHECK_REPRODUCE);
    }

    public boolean useWriteCheck() {
        return getDbmsSpecificOptions().getTestOracleFactory().stream().anyMatch(o -> o == SQLite3Options.SQLite3OracleFactory.WRITE_CHECK);
    }

}
