package sqlancer.mariadb.ast;

import sqlancer.mariadb.MariaDBSchema;

public class MariaDBTableReference implements MariaDBExpression {
    private final MariaDBSchema.MariaDBTable table;

    public MariaDBTableReference(MariaDBSchema.MariaDBTable table) {
        this.table = table;
    }

    public MariaDBSchema.MariaDBTable getTable() {
        return table;
    }

}
