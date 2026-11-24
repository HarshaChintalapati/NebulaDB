package dev.nebuladb.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

public interface StorageEngine extends Closeable {
    void put(String key, String value) throws IOException;
    Optional<String> get(String key) throws IOException;
    void delete(String key) throws IOException;
    void flush() throws IOException;
}
