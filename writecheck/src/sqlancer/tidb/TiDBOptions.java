package sqlancer.tidb;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import sqlancer.DBMSSpecificOptions;
import sqlancer.OracleFactory;
import sqlancer.common.oracle.CompositeTestOracle;
import sqlancer.common.oracle.TestOracle;
import sqlancer.tidb.TiDBOptions.TiDBOracleFactory;
import sqlancer.tidb.TiDBProvider.TiDBGlobalState;
import sqlancer.tidb.oracle.TiDBNoRECOracle;
import sqlancer.tidb.oracle.TiDBTLPHavingOracle;
import sqlancer.tidb.oracle.TiDBTLPWhereOracle;
import sqlancer.tidb.oracle.transaction.TiDBOptWriteCheckOracle;
import sqlancer.tidb.oracle.transaction.TiDBOptWriteCheckReproduceOracle;
import sqlancer.tidb.oracle.transaction.TiDBWriteCheckOracle;
import sqlancer.tidb.oracle.transaction.TiDBWriteCheckReproduceOracle;

@Parameters(separators = "=", commandDescription = "TiDB (default port: " + TiDBOptions.DEFAULT_PORT
        + ", default host: " + TiDBOptions.DEFAULT_HOST + ")")
public class TiDBOptions implements DBMSSpecificOptions<TiDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 4000;

    @Parameter(names = "--oracle")
    public List<TiDBOracleFactory> oracle = Arrays.asList(TiDBOracleFactory.QUERY_PARTITIONING);

    public enum TiDBOracleFactory implements OracleFactory<TiDBGlobalState> {
        HAVING {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPHavingOracle(globalState);
            }
        },
        WHERE {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBTLPWhereOracle(globalState);
            }
        },
        NOREC {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBNoRECOracle(globalState);
            }
        },
        QUERY_PARTITIONING {
            @Override
            public TestOracle<TiDBGlobalState> create(TiDBGlobalState globalState) throws SQLException {
                List<TestOracle<TiDBGlobalState>> oracles = new ArrayList<>();
                oracles.add(new TiDBTLPWhereOracle(globalState));
                oracles.add(new TiDBTLPHavingOracle(globalState));
                return new CompositeTestOracle<TiDBGlobalState>(oracles, globalState);
            }
        },
        WRITE_CHECK {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBWriteCheckOracle(globalState);
            }
        },
        OPT_WRITE_CHECK {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBOptWriteCheckOracle(globalState);
            }
        },
        WRITE_CHECK_REPRODUCE {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBWriteCheckReproduceOracle(globalState);
            }
        },
        OPT_WRITE_CHECK_REPRODUCE {
            @Override
            public TestOracle create(TiDBGlobalState globalState) throws SQLException {
                return new TiDBOptWriteCheckReproduceOracle(globalState);
            }
        };

    }

    @Override
    public List<TiDBOracleFactory> getTestOracleFactory() {
        return oracle;
    }
}
