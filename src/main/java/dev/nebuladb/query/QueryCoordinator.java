package dev.nebuladb.query;

import dev.nebuladb.kv.KvStore;
import dev.nebuladb.sql.Statement;

/**
 * Turns parsed SQL into key-value operations. Rows are stored as
 * "table:primaryKey" -> value, and the table catalog as "__catalog:table", so the
 * schema is replicated and survives restarts just like the data.
 */
public final class QueryCoordinator {
    private final KvStore store;

    public QueryCoordinator(KvStore store) {
        this.store = store;
    }

    public String execute(Statement st) throws Exception {
        if (st instanceof Statement.Create c) {
            if (store.get(catalogKey(c.table())).isPresent()) throw new IllegalStateException("table exists");
            store.put(catalogKey(c.table()), "1");
            return "table created";
        }
        if (st instanceof Statement.Insert i) {
            requireTable(i.table());
            store.put(rowKey(i.table(), i.key()), i.value());
            return "inserted";
        }
        if (st instanceof Statement.Update u) {
            requireTable(u.table());
            if (store.get(rowKey(u.table(), u.key())).isEmpty()) return "0 rows updated";
            store.put(rowKey(u.table(), u.key()), u.value());
            return "updated";
        }
        if (st instanceof Statement.Delete d) {
            requireTable(d.table());
            store.delete(rowKey(d.table(), d.key()));
            return "deleted";
        }
        if (st instanceof Statement.Select s) {
            requireTable(s.table());
            return store.get(rowKey(s.table(), s.key())).map(v -> s.key() + "," + v).orElse("(0 rows)");
        }
        throw new IllegalArgumentException("unsupported statement");
    }

    private void requireTable(String t) throws Exception {
        if (store.get(catalogKey(t)).isEmpty()) throw new IllegalStateException("unknown table " + t);
    }

    private static String catalogKey(String table) {
        return "__catalog:" + table.toLowerCase();
    }

    private static String rowKey(String table, String key) {
        return table.toLowerCase() + ":" + key;
    }
}
