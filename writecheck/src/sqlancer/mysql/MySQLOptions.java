package sqlancer.mysql;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.TestOracle;
import sqlancer.mysql.MySQLOptions.MySQLOracleFactory;
import sqlancer.mysql.oracle.MySQLNoRECOracle;
import sqlancer.mysql.oracle.MySQLPivotedQuerySynthesisOracle;
import sqlancer.mysql.oracle.MySQLTLPWhereOracle;
import sqlancer.mysql.oracle.transaction.MySQLWriteCheckReproduceOracle;
import sqlancer.mysql.oracle.transaction.MySQLWriteCheckOracle;

@Parameters(separators = "=", commandDescription = "MySQL (default port: " + MySQLOptions.DEFAULT_PORT
        + ", default host: " + MySQLOptions.DEFAULT_HOST + ")")
public class MySQLOptions implements DBMSSpecificOptions<MySQLOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 3306;

    @Parameter(names = "--oracle")
    public List<MySQLOracleFactory> oracles = Arrays.asList(MySQLOracleFactory.TLP_WHERE);

    public enum MySQLOracleFactory implements OracleFactory<MySQLGlobalState> {

        TLP_WHERE {
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLTLPWhereOracle(globalState);
            }

        },
        PQS {
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        NOREC{
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLNoRECOracle(globalState);
            }
        },
        WRITE_CHECK {
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLWriteCheckOracle(globalState);
            }
        },
        WRITE_CHECK_REPRODUCE {
            @Override
            public TestOracle<MySQLGlobalState> create(MySQLGlobalState globalState) throws SQLException {
                return new MySQLWriteCheckReproduceOracle(globalState);
            }
        }
    }

    @Override
    public List<MySQLOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
