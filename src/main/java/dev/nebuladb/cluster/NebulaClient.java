package dev.nebuladb.cluster;

import dev.nebuladb.kv.Command;
import dev.nebuladb.kv.KvStore;
import dev.nebuladb.net.Codec;
import dev.nebuladb.net.NodeTransport;
import dev.nebuladb.net.Transports;
import dev.nebuladb.sharding.ShardRouter;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Cluster client: routes each key to its shard, finds that shard's leader, and
 * retries through elections and node failures.
 *
 * A call returns only after the leader has committed (majority-replicated) and
 * applied the command. That return is what "acknowledged write" means in the
 * failure tests. If a call throws, the write may or may not have happened.
 * Reads go through the Raft log too, so they are linearizable.
 */
public final class NebulaClient implements KvStore, AutoCloseable {
    private final ClusterConfig config;
    private final ShardRouter router;
    private final NodeTransport transport;
    private final Map<Integer, String> leaderCache = new ConcurrentHashMap<>();
    private final long deadlineMs;

    public NebulaClient(ClusterConfig config, Transports.Kind transportKind) {
        this(config, transportKind, 15_000);
    }

    public NebulaClient(ClusterConfig config, Transports.Kind transportKind, long deadlineMs) {
        this.config = config;
        this.router = new ShardRouter(config.shards());
        this.transport = Transports.client(transportKind, config.nodes(), Duration.ofSeconds(6));
        this.deadlineMs = deadlineMs;
    }

    @Override
    public void put(String key, String value) throws IOException {
        execute(key, Command.put(key, value));
    }

    @Override
    public Optional<String> get(String key) throws IOException {
        return Optional.ofNullable(execute(key, Command.get(key)));
    }

    @Override
    public void delete(String key) throws IOException {
        execute(key, Command.delete(key));
    }

    public int shardFor(String key) {
        return router.shardFor(key);
    }

    private String execute(String key, Command command) throws IOException {
        int shard = router.shardFor(key);
        List<String> replicas = config.replicas(shard);
        Codec.KvRequest request = new Codec.KvRequest(shard, command.encode());
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(deadlineMs);
        String target = leaderCache.getOrDefault(shard, replicas.get(0));
        int attempt = 0;
        int hintsFollowed = 0;
        String lastError = "no attempt";
        while (System.nanoTime() < deadline) {
            try {
                Codec.KvResponse resp = transport.kv(target, request).get(7, TimeUnit.SECONDS);
                switch (resp.status()) {
                    case OK -> {
                        leaderCache.put(shard, target);
                        return resp.value();
                    }
                    case NOT_LEADER -> {
                        lastError = target + " is not leader";
                        if (resp.info() != null && replicas.contains(resp.info())
                                && !resp.info().equals(target) && hintsFollowed++ < 3) {
                            target = resp.info();
                            continue; // follow the hint immediately
                        }
                    }
                    case ERROR -> lastError = target + ": " + resp.info();
                }
            } catch (Exception e) {
                lastError = target + ": " + e;
            }
            leaderCache.remove(shard);
            attempt++;
            target = replicas.get(attempt % replicas.size());
            if (attempt % replicas.size() == 0) {
                hintsFollowed = 0;
                sleep(50); // went round once; an election is probably running
            }
        }
        throw new IOException("shard " + shard + " unavailable: " + lastError);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        transport.close();
    }
}
