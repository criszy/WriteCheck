package sqlancer.mariadb.transaction;

import sqlancer.common.transaction.IsolationLevel;

public class MariaDBIsolation {
    public enum MariaDBIsolationLevel implements IsolationLevel {

        READ_UNCOMMITTED("READ UNCOMMITTED"),
        READ_COMMITTED("READ COMMITTED"),
        REPEATABLE_READ("REPEATABLE READ"),
        SERIALIZABLE("SERIALIZABLE");

        private final String name;

        MariaDBIsolationLevel(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
