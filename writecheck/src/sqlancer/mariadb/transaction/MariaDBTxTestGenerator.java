package sqlancer.mariadb.transaction;

import sqlancer.common.transaction.Transaction;
import sqlancer.common.transaction.TxTestGenerator;
import sqlancer.mariadb.MariaDBProvider;
import sqlancer.mariadb.gen.transaction.MariaDBTransactionProvider;

import java.sql.SQLException;

public class MariaDBTxTestGenerator extends TxTestGenerator<MariaDBProvider.MariaDBGlobalState> {

    public MariaDBTxTestGenerator(MariaDBProvider.MariaDBGlobalState globalState) {
        super(globalState);
    }

    @Override
    protected Transaction generateTransaction() throws SQLException {
        return MariaDBTransactionProvider.generateTransaction(globalState);
    }

}
