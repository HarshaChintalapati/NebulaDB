package dev.nebuladb.kv;

import java.io.IOException;
import java.util.Optional;

/** What the SQL layer needs: a key-value store (local shards or the replicated cluster). */
public interface KvStore {
    void put(String key, String value) throws IOException;

    Optional<String> get(String key) throws IOException;

    void delete(String key) throws IOException;
}
