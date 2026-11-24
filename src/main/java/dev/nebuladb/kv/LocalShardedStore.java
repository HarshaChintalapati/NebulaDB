package dev.nebuladb.kv;

import dev.nebuladb.sharding.ShardRouter;
import dev.nebuladb.storage.LsmStorageEngine;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

/** Single-process mode: N local LSM engines, no replication. Useful for trying the SQL layer. */
public final class LocalShardedStore implements KvStore, Closeable {
    private final ShardRouter router;
    private final List<LsmStorageEngine> shards = new ArrayList<>();

    public LocalShardedStore(Path dir, int shardCount) throws IOException {
        router = new ShardRouter(shardCount);
        for (int i = 0; i < shardCount; i++) shards.add(new LsmStorageEngine(dir.resolve("shard-" + i), 128));
    }

    private LsmStorageEngine shard(String key) {
        return shards.get(router.shardFor(key));
    }

    @Override
    public void put(String key, String value) throws IOException {
        shard(key).put(key, value);
    }

    @Override
    public Optional<String> get(String key) throws IOException {
        return shard(key).get(key);
    }

    @Override
    public void delete(String key) throws IOException {
        shard(key).delete(key);
    }

    @Override
    public void close() {
        shards.forEach(LsmStorageEngine::close);
    }
}
