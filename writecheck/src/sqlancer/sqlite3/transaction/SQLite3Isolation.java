package sqlancer.sqlite3.transaction;

import sqlancer.common.transaction.IsolationLevel;

public class SQLite3Isolation {

    public enum SQLite3IsolationLevel implements IsolationLevel {

        SERIALIZABLE("SERIALIZABLE");

        private final String name;

        SQLite3IsolationLevel(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }
    }

}
