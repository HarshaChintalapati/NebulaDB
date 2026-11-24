package dev.nebuladb.cluster;

import dev.nebuladb.raft.RaftNode;
import dev.nebuladb.raft.RaftTiming;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.*;

/**
 * Runs a whole cluster inside one JVM, each node on its own localhost port with
 * its own data directory. Nodes still talk to each other over real HTTP. Used by
 * the tests, the chaos runner and the local benchmark.
 */
public final class LocalCluster implements AutoCloseable {
    private final ClusterConfig config;
    private final Path baseDir;
    private final RaftTiming timing;
    private final boolean fsync;
    private final Map<String, NebulaNode> running = new LinkedHashMap<>();

    public LocalCluster(int nodeCount, int shards, int rf, Path baseDir, RaftTiming timing, boolean fsync) throws IOException {
        LinkedHashMap<String, String> nodes = new LinkedHashMap<>();
        for (int i = 1; i <= nodeCount; i++) nodes.put("n" + i, "127.0.0.1:" + freePort());
        this.config = new ClusterConfig(nodes, shards, rf);
        this.baseDir = baseDir;
        this.timing = timing;
        this.fsync = fsync;
    }

    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    public ClusterConfig config() {
        return config;
    }

    public void startAll() throws IOException {
        for (String id : config.nodeIds()) start(id);
    }

    public synchronized void start(String id) throws IOException {
        if (running.containsKey(id)) return;
        NebulaNode node = new NebulaNode(id, config, baseDir.resolve(id), timing, fsync);
        node.start();
        running.put(id, node);
    }

    /** Kill -9 style: the node stops without flushing anything. */
    public synchronized void crash(String id) {
        NebulaNode n = running.remove(id);
        if (n != null) n.crash();
    }

    public synchronized Optional<NebulaNode> node(String id) {
        return Optional.ofNullable(running.get(id));
    }

    /** Current leader of a shard among running nodes (highest term wins if two think so briefly). */
    public synchronized Optional<String> leaderOf(int shard) {
        String best = null;
        long bestTerm = -1;
        for (NebulaNode n : running.values()) {
            Optional<RaftNode> g = n.group(shard);
            if (g.isPresent() && g.get().isLeader()) {
                long term = g.get().status().term();
                if (term > bestTerm) {
                    bestTerm = term;
                    best = n.id();
                }
            }
        }
        return Optional.ofNullable(best);
    }

    /** Waits until the shard has a leader that is not `excluded`. Returns it, or empty on timeout. */
    public Optional<String> awaitLeader(int shard, String excluded, long timeoutMs) {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            Optional<String> l = leaderOf(shard);
            if (l.isPresent() && !l.get().equals(excluded)) return l;
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    @Override
    public synchronized void close() {
        for (NebulaNode n : running.values()) n.close();
        running.clear();
    }
}
