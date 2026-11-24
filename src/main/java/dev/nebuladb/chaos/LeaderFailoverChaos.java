package dev.nebuladb.chaos;

import dev.nebuladb.cluster.ClusterConfig;
import dev.nebuladb.cluster.LocalCluster;
import dev.nebuladb.cluster.NebulaClient;
import dev.nebuladb.kv.KvStateMachine;
import dev.nebuladb.raft.RaftTiming;
import dev.nebuladb.sharding.ShardRouter;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fault-injection test: repeatedly crash the leader of a shard while clients keep
 * writing, then check that no acknowledged write was lost.
 *
 * Each round: find the current leader of shard (round % shards), crash its whole
 * node (no flush, like kill -9), measure how long until another replica is
 * elected, keep writing for a moment with the node down, then restart it from its
 * on-disk state so it must recover and catch up.
 *
 * At the end:
 *  1. every write that returned success must be readable with its exact value
 *     (reads go through Raft, so they see the committed state), and
 *  2. once all replicas have caught up, every replica of every shard must hold
 *     the same value for every acknowledged key (no divergence).
 */
public final class LeaderFailoverChaos {

    public record Report(int failuresInjected, long ackedWrites, long failedWrites, long lostWrites,
                         long divergentReplicaReads, List<Long> failoverMs, long elapsedMs) {
        public boolean passed() {
            return failuresInjected > 0 && lostWrites == 0 && divergentReplicaReads == 0;
        }

        public String summary() {
            LongSummaryStatistics s = failoverMs.stream().mapToLong(Long::longValue).summaryStatistics();
            List<Long> sorted = new ArrayList<>(failoverMs);
            Collections.sort(sorted);
            long p50 = sorted.isEmpty() ? 0 : sorted.get(sorted.size() / 2);
            long p95 = sorted.isEmpty() ? 0 : sorted.get(Math.min(sorted.size() - 1, (int) Math.ceil(sorted.size() * 0.95) - 1));
            return String.format(
                    "leader failures injected : %d%n" +
                    "acknowledged writes      : %d%n" +
                    "failed (unacked) writes  : %d   (client gave up; may or may not have applied)%n" +
                    "LOST acknowledged writes : %d%n" +
                    "divergent replica reads  : %d%n" +
                    "failover time (ms)       : p50=%d p95=%d max=%d%n" +
                    "total time               : %.1f s%n" +
                    "RESULT                   : %s",
                    failuresInjected, ackedWrites, failedWrites, lostWrites, divergentReplicaReads,
                    p50, p95, s.getCount() == 0 ? 0 : s.getMax(), elapsedMs / 1000.0, passed() ? "PASS" : "FAIL");
        }
    }

    public static Report run(Path dir, int nodes, int shards, int rf, int failures, int writers,
                             RaftTiming timing, boolean fsync, PrintStream log) throws Exception {
        long started = System.currentTimeMillis();
        try (LocalCluster cluster = new LocalCluster(nodes, shards, rf, dir, timing, fsync)) {
            cluster.startAll();
            for (int s = 0; s < shards; s++) {
                if (cluster.awaitLeader(s, null, 10_000).isEmpty()) throw new IllegalStateException("no initial leader for shard " + s);
            }
            ClusterConfig config = cluster.config();
            Map<String, String> acked = new ConcurrentHashMap<>();
            AtomicLong failed = new AtomicLong();
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> threads = new ArrayList<>();
            try (NebulaClient client = new NebulaClient(config)) {
                for (int t = 0; t < writers; t++) {
                    final int w = t;
                    Thread th = new Thread(() -> {
                        Random rnd = new Random(w);
                        for (long i = 0; !stop.get(); i++) {
                            String key = "w" + w + "-" + i;
                            String value = "v" + i + "-" + Long.toHexString(rnd.nextLong());
                            try {
                                client.put(key, value);
                                acked.put(key, value); // recorded ONLY after the cluster acknowledged it
                            } catch (IOException e) {
                                failed.incrementAndGet();
                            }
                        }
                    }, "writer-" + t);
                    th.start();
                    threads.add(th);
                }

                List<Long> failover = new ArrayList<>();
                int injected = 0;
                for (int round = 0; round < failures; round++) {
                    int shard = round % shards;
                    Optional<String> leader = cluster.awaitLeader(shard, null, 10_000);
                    if (leader.isEmpty()) throw new IllegalStateException("shard " + shard + " has no leader");
                    long t0 = System.nanoTime();
                    cluster.crash(leader.get());
                    injected++;
                    Optional<String> next = cluster.awaitLeader(shard, leader.get(), 10_000);
                    long ms = (System.nanoTime() - t0) / 1_000_000;
                    if (next.isEmpty()) throw new IllegalStateException("no new leader for shard " + shard + " after crash");
                    failover.add(ms);
                    Thread.sleep(100);                // keep writing while the node is down
                    cluster.start(leader.get());      // restart from disk; it must recover and catch up
                    Thread.sleep(100);
                    if (log != null && (round + 1) % 10 == 0) {
                        log.printf("  %d/%d leader failures injected, %d writes acknowledged so far%n",
                                round + 1, failures, acked.size());
                    }
                }

                stop.set(true);
                for (Thread th : threads) th.join();

                // 1. Every acknowledged write must be readable (linearizable read through Raft).
                long lost = 0;
                for (Map.Entry<String, String> e : acked.entrySet()) {
                    Optional<String> v = client.get(e.getKey());
                    if (v.isEmpty() || !v.get().equals(e.getValue())) {
                        lost++;
                        if (log != null && lost <= 10) log.println("  LOST: " + e.getKey() + " expected " + e.getValue() + " got " + v);
                    }
                }

                // 2. All replicas converge to the same data.
                long divergent = checkReplicas(cluster, config, acked, log);
                return new Report(injected, acked.size(), failed.get(), lost, divergent, failover,
                        System.currentTimeMillis() - started);
            } finally {
                stop.set(true);
                for (Thread th : threads) th.join();
            }
        }
    }

    private static long checkReplicas(LocalCluster cluster, ClusterConfig config, Map<String, String> acked,
                                      PrintStream log) throws Exception {
        // Wait until every replica has applied everything its leader committed.
        for (int s = 0; s < config.shards(); s++) {
            long deadline = System.currentTimeMillis() + 15_000;
            while (true) {
                final int shard = s;
                String leader = cluster.awaitLeader(shard, null, 10_000).orElseThrow();
                long commit = cluster.node(leader).flatMap(n -> n.group(shard)).map(g -> g.status().commitIndex()).orElse(0L);
                boolean caughtUp = true;
                for (String r : config.replicas(s)) {
                    long applied = cluster.node(r).flatMap(n -> n.group(shard)).map(g -> g.status().lastApplied()).orElse(-1L);
                    if (applied < commit) caughtUp = false;
                }
                if (caughtUp) break;
                if (System.currentTimeMillis() > deadline) throw new IllegalStateException("replicas of shard " + s + " did not catch up");
                Thread.sleep(20);
            }
        }
        long divergent = 0;
        ShardRouter router = new ShardRouter(config.shards());
        for (Map.Entry<String, String> e : acked.entrySet()) {
            int shard = router.shardFor(e.getKey());
            for (String r : config.replicas(shard)) {
                Optional<KvStateMachine> sm = cluster.node(r).flatMap(n -> n.stateMachine(shard));
                Optional<String> v = sm.isPresent() ? sm.get().readLocal(e.getKey()) : Optional.empty();
                if (v.isEmpty() || !v.get().equals(e.getValue())) {
                    divergent++;
                    if (log != null && divergent <= 10) log.println("  DIVERGENT: " + r + " " + e.getKey() + " -> " + v);
                }
            }
        }
        return divergent;
    }

    /** CLI: java -cp nebuladb.jar dev.nebuladb.NebulaDbApplication chaos --failures 100 */
    public static int main(Map<String, String> args) throws Exception {
        int failures = Integer.parseInt(args.getOrDefault("failures", "100"));
        int nodes = Integer.parseInt(args.getOrDefault("nodes", "5"));
        int shards = Integer.parseInt(args.getOrDefault("shards", "10"));
        int rf = Integer.parseInt(args.getOrDefault("rf", "3"));
        int writers = Integer.parseInt(args.getOrDefault("writers", "8"));
        boolean fsync = !"false".equals(args.get("fsync"));
        Path dir = args.containsKey("dir") ? Path.of(args.get("dir")) : Files.createTempDirectory("nebula-chaos");
        System.out.printf("Chaos: %d nodes, %d shards, rf=%d, %d writers, %d leader failures, fsync=%s, data in %s%n",
                nodes, shards, rf, writers, failures, fsync, dir);
        Report r = run(dir, nodes, shards, rf, failures, writers, RaftTiming.defaults(), fsync, System.out);
        System.out.println(r.summary());
        return r.passed() ? 0 : 1;
    }
}
