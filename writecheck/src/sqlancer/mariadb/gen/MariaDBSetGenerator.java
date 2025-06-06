package sqlancer.mariadb.gen;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.mariadb.MariaDBProvider.MariaDBGlobalState;

public class MariaDBSetGenerator {

    private final Randomly r;
    private final StringBuilder sb = new StringBuilder();

    // currently, global options are only generated when a single thread is executed
    private boolean isSingleThreaded;

    public MariaDBSetGenerator(Randomly r, MainOptions options) {
        this.r = r;
        this.isSingleThreaded = options.getNumberConcurrentThreads() == 1;
    }

    public static SQLQueryAdapter set(MariaDBGlobalState globalState) {
        return new MariaDBSetGenerator(globalState.getRandomly(), globalState.getOptions()).get();
    }

    private enum Scope {
        GLOBAL, SESSION
    }

    private enum Action {

        AUTOCOMMIT("autocommit", (r) -> 1, Scope.GLOBAL, Scope.SESSION), //
        BIG_TABLES("big_tables", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL, Scope.SESSION), //
        COMPLETION_TYPE("completion_type", (r) -> Randomly.fromOptions("'NO_CHAIN'", "'CHAIN'", "'RELEASE'", 0, 1, 2),
                Scope.GLOBAL), //
        // BULK_INSERT_CACHE_SIZE("bulk_insert_buffer_size", (r) -> r.getLong(0, Long.MAX_VALUE), Scope.GLOBAL,
        // Scope.SESSION),
        CONCURRENT_INSERT("concurrent_insert", (r) -> Randomly.fromOptions("NEVER", "AUTO", "ALWAYS", 0, 1, 2),
                Scope.GLOBAL),
        //Change variable "cte_max_recursion_depth" to "max_recursive_iterations" since the latter is used in mariadb.
//        CTE_MAX_RECURSION_DEPTH("cte_max_recursion_depth", (r) -> r.getLong(0, 4294967295L), Scope.GLOBAL),
        MAX_RECURSIVE_ITERATIONS("max_recursive_iterations", (r) -> r.getLong(0, 4294967295L), Scope.GLOBAL),
        DELAY_KEY_WRITE("delay_key_write", (r) -> Randomly.fromOptions("ON", "OFF", "ALL"), Scope.GLOBAL),
        EQ_RANGE_INDEX_DIVE_LIMIT("eq_range_index_dive_limit", (r) -> r.getLong(0, 4294967295L), Scope.GLOBAL),
        FLUSH("flush", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL),
        FOREIGN_KEY_CHECKS("foreign_key_checks", (r) -> Randomly.fromOptions(1, 0), Scope.GLOBAL, Scope.SESSION),
        HOST_CACHE_SIZE("host_cache_size", (r) -> r.getLong(0, 65536), Scope.GLOBAL),
        JOIN_BUFFER_SIZE("join_buffer_size", (r) -> r.getLong(128, Long.MAX_VALUE), Scope.GLOBAL, Scope.SESSION),
        /* disabled as a workaround for https://bugs.mysql.com/bug.php?id=95987 */
        // KEY_BUFFER_SIZE("key_buffer_size", (r) -> r.getLong(8, Long.MAX_VALUE), Scope.GLOBAL),
        // KEY_CACHE_AGE_THRESHOLD("key_cache_age_threshold", (r) -> r.getLong(100, Long.MAX_VALUE), Scope.GLOBAL),
        // KEY_CACHE_BLOCK_SIZE("key_cache_block_size", (r) -> r.getLong(512, 16384), Scope.GLOBAL),
        // KEY_CACHE_DIVISION_LIMIT("key_cache_division_limit", (r) -> r.getLong(1, 100), Scope.GLOBAL),
        // MAX_HEAP_TABLE_SIZE("max_heap_table_size", (r) -> r.getLong(16384, Long.MAX_VALUE), Scope.GLOBAL,
        // Scope.SESSION),
        MAX_LENGTH_FOR_SORT_DATA("max_length_for_sort_data", (r) -> r.getLong(4, 8388608), Scope.GLOBAL, Scope.SESSION),
        MAX_SEEKS_FOR_KEY("max_seeks_for_key", (r) -> r.getLong(1, Long.MAX_VALUE), Scope.GLOBAL, Scope.SESSION),
        MAX_SORT_LENGTH("max_sort_length", (r) -> r.getLong(4, 8388608), Scope.GLOBAL, Scope.SESSION),
        MAX_SP_RECURSION_DEPTH("max_sp_recursion_depth", (r) -> r.getLong(0, 255), Scope.GLOBAL, Scope.SESSION),
        MYISAM_DATA_POINTER_SIZE("myisam_data_pointer_size", (r) -> r.getLong(2, 7), Scope.GLOBAL),
        MYISAM_MAX_SORT_FILE_SIZE("myisam_max_sort_file_size", (r) -> r.getLong(0, 9223372036854775807L), Scope.GLOBAL),
        MYISAM_REPAIR_THREADS("myisam_repair_threads", (r) -> r.getLong(1, Long.MAX_VALUE), Scope.GLOBAL,
                Scope.SESSION),
        // MYISAM_SORT_BUFFER_SIZE("myisam_sort_buffer_size", (r) -> r.getLong(4096, Long.MAX_VALUE), Scope.GLOBAL,
        // Scope.SESSION),
        MYISAM_STATS_METHOD("myisam_stats_method",
                (r) -> Randomly.fromOptions("nulls_equal", "nulls_unequal", "nulls_ignored"), Scope.GLOBAL,
                Scope.SESSION),
        MYISAM_USE_MMAP("myisam_use_mmap", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL),
        OPTIMIZER_PRUNE_LEVEL("optimizer_prune_level", (r) -> Randomly.fromOptions(0, 1), Scope.GLOBAL, Scope.SESSION),
        OPTIMIZER_SEARCH_DEPTH("optimizer_search_depth", (r) -> r.getLong(0, 62), Scope.GLOBAL, Scope.SESSION),
        OPTIMIZER_SWITCH("optimizer_switch", (r) -> getOptimizerSwitchConfiguration(r), Scope.GLOBAL, Scope.SESSION),
        // PRELOAD_BUFFER_SIZE("preload_buffer_size", (r) -> r.getLong(1024, 1073741824), Scope.GLOBAL, Scope.SESSION),
        // QUERY_ALLOC_BLOCK_SIZE("query_alloc_block_size", (r) -> r.getLong(1024, 4294967295L), Scope.GLOBAL,
        // Scope.SESSION),

        // causes out of memory errors
        // QUERY_PREALLOC_SIZE("query_prealloc_size", (r) -> r.getLong(8192,
        // Long.MAX_VALUE), Scope.GLOBAL, Scope.SESSION),
        // RANGE_ALLOC_BLOCK_SIZE("range_alloc_block_size", (r) -> r.getLong(4096, Long.MAX_VALUE), Scope.GLOBAL,
        // Scope.SESSION),
        /*
         * Removed Scope.GLOBAL as a workaround for https://bugs.mysql.com/bug.php?id=95985
         */
        // READ_BUFFER_SIZE("read_buffer_size", (r) -> r.getLong(8200, 2147479552), Scope.GLOBAL, Scope.SESSION),
        // READ_RND_BUFFER_SIZE("read_rnd_buffer_size", (r) -> r.getLong(1, 2147483647), Scope.GLOBAL, Scope.SESSION),
        
        //remove "schema_definition_cache" since not supported.
//        SCHEMA_DEFINITION_CACHE("schema_definition_cache", (r) -> r.getLong(256, 524288), Scope.GLOBAL),
        /*
         * sort_buffer_size is commented out as a workaround for https://bugs.mysql.com/bug.php?id=95969
         */
        // SORT_BUFFER_SIZE("sort_buffer_size", (r) -> r.getLong(32768, Long.MAX_VALUE)),
        SQL_AUTO_IS_NULL("sql_auto_is_null", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL, Scope.SESSION),
        SQL_BUFFER_RESULT("sql_buffer_result", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL, Scope.SESSION),
        SQL_LOG_OFF("sql_log_off", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL, Scope.SESSION),
        SQL_QUOTE_SHOW_CREATE("sql_quote_show_create", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL,
                Scope.SESSION),
        // SQL_REQUIRE_PRIMARY_KEY("sql_require_primary_key", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL),
        // TMP_TABLE_SIZE("tmp_table_size", (r) -> r.getLong(1024, Long.MAX_VALUE), Scope.GLOBAL, Scope.SESSION),
        UNIQUE_CHECKS("unique_checks", (r) -> Randomly.fromOptions("OFF", "ON"), Scope.GLOBAL, Scope.SESSION),
        // TODO: https://dev.mysql.com/doc/refman/8.0/en/switchable-optimizations.html

        // MariaDB-specific
        JOIN_CACHE_LEVEL("join_cache_level", (r) -> r.getInteger(1, 8), Scope.GLOBAL, Scope.SESSION);

        private String name;
        private Function<Randomly, Object> prod;
        private final Scope[] scopes;

        Action(String name, Function<Randomly, Object> prod, Scope... scopes) {
            if (scopes.length == 0) {
                throw new AssertionError(name);
            }
            this.name = name;
            this.prod = prod;
            this.scopes = scopes.clone();
        }

        private static String getOptimizerSwitchConfiguration(Randomly r) {
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            String[] options = { /*
                                  * ("batched_key_access", /*"block_nested_loop", "condition_fanout_filter",
                                  */
                    "condition_pushdown_for_derived", // MariaDB
                    "derived_merge", //
                    "derived_with_keys", // MariaDB
                    "engine_condition_pushdown", //
                    "exists_to_in", // MariaDB
                    "extended_keys", // MariaDB
                    "firstmatch", // MariaDB
                    "index_condition_pushdown", //
                    /* "use_index_extensions", */
                    "index_merge", //
                    "index_merge_intersection", //
                    "index_merge_sort_intersection", //
                    "index_merge_sort_union", //
                    "index_merge_union", "in_to_exists", // MariaDB
                    /* "use_invisible_indexes", */ "mrr", "mrr_cost_based", /* "skip_scan", */ "semijoin", /*
                                                                                                            * "duplicateweedout",
                                                                                                            */
                    "firstmatch", "loosescan", "materialization", /* "subquery_materialization_cost_based" */ };
            List<String> optionSubset = Arrays.asList(Randomly.fromOptions(options));
            sb.append(optionSubset.stream().map(s -> s + "=" + Randomly.fromOptions("on", "off"))
                    .collect(Collectors.joining(",")));
            sb.append("'");
            return sb.toString();
        }

        public boolean canBeUsedInScope(Scope session) {
            for (Scope scope : scopes) {
                if (scope == session) {
                    return true;
                }
            }
            return false;
        }

        public Scope[] getScopes() {
            return scopes.clone();
        }
    }

    private SQLQueryAdapter get() {
        sb.append("SET ");
        Action a;
        if (isSingleThreaded) {
            a = Randomly.fromOptions(Action.values());
            Scope[] scopes = a.getScopes();
            Scope randomScope = Randomly.fromOptions(scopes);
            switch (randomScope) {
            case GLOBAL:
                sb.append("GLOBAL");
                break;
            case SESSION:
                sb.append("SESSION");
                break;
            default:
                throw new AssertionError(randomScope);
            }

        } else {
            do {
                a = Randomly.fromOptions(Action.values());
            } while (!a.canBeUsedInScope(Scope.SESSION));
            sb.append("SESSION");
        }
        sb.append(" ");
        sb.append(a.name);
        sb.append(" = ");
        sb.append(a.prod.apply(r));
        return new SQLQueryAdapter(sb.toString(), ExpectedErrors
                .from("At least one of the 'in_to_exists' or 'materialization' optimizer_switch flags must be 'on'"));
    }

}
