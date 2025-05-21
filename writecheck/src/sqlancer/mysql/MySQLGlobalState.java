
package sqlancer.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;

public class MySQLGlobalState extends SQLGlobalState<MySQLOptions, MySQLSchema> {

    @Override
    protected MySQLSchema readSchema() throws SQLException {
        return MySQLSchema.fromConnection(getConnection(), getDatabaseName());
    }

    public boolean usesPQS() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.PQS);
    }

    public SQLConnection createConnection() throws SQLException {
        String host = getOptions().getHost();
        int port = getOptions().getPort();
        if (host == null) {
            host = MySQLOptions.DEFAULT_HOST;
        }
        if (port == MainOptions.NO_SET_PORT) {
            port = MySQLOptions.DEFAULT_PORT;
        }
        String databaseName = getDatabaseName();
        String url = String.format("jdbc:mysql://%s:%d/%s", host, port, databaseName);
        Connection con = DriverManager.getConnection(url, getOptions().getUserName(),
                getOptions().getPassword());
        return new SQLConnection(con);
    }

    public boolean useTxReproduce() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.WRITE_CHECK_REPRODUCE);
    }

    public boolean useWriteCheck() {
        return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == MySQLOracleFactory.WRITE_CHECK);
    }
}