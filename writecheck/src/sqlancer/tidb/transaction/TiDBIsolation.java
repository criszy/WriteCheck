package sqlancer.tidb.transaction;

import sqlancer.common.transaction.IsolationLevel;

public class TiDBIsolation {
    public enum TiDBIsolationLevel implements IsolationLevel {

        READ_COMMITTED("READ COMMITTED"),
        REPEATABLE_READ("REPEATABLE READ");

        private final String name;

        TiDBIsolationLevel(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }
}
