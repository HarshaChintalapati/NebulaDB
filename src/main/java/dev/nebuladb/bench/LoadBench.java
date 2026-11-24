package dev.nebuladb.bench;

import dev.nebuladb.cluster.ClusterConfig;
import dev.nebuladb.cluster.LocalCluster;
import dev.nebuladb.cluster.NebulaClient;
import dev.nebuladb.raft.RaftTiming;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Write benchmark: loads N rows through the cluster client with T concurrent
 * clients, then reports throughput, latency percentiles, how rows spread over
 * shards, and verifies a random sample by reading it back.
 *
 * Numbers depend heavily on the machine and on fsync (every write waits for a
 * durable Raft log append on a majority), so always report them with the setup.
 */
public final class LoadBench {

    public static int main(Map<String, String> args) throws Exception {
        int rows = Integer.parseInt(args.getOrDefault("rows", "100000"));
        int threads = Integer.parseInt(args.getOrDefault("threads", "32"));
        int shards = Integer.parseInt(args.getOrDefault("shards", "10"));
        int rf = Integer.parseInt(args.getOrDefault("rf", "3"));
        LocalCluster local = null;
        ClusterConfig config;
        if (args.containsKey("cluster")) {
            config = ClusterConfig.parse(args.get("cluster"), shards, rf);
        } else {
            int nodes = Integer.parseInt(args.getOrDefault("nodes", "5"));
            Path dir = args.containsKey("dir") ? Path.of(args.get("dir")) : Files.createTempDirectory("nebula-bench");
            local = new LocalCluster(nodes, shards, rf, dir, RaftTiming.defaults(), !"false".equals(args.get("fsync")));
            local.startAll();
            for (int s = 0; s < shards; s++) local.awaitLeader(s, null, 10_000).orElseThrow();
            config = local.config();
        }
        System.out.printf("Benchmark: %d rows, %d client threads, %d nodes, %d shards, rf=%d%n",
                rows, threads, config.nodes().size(), shards, rf);
        try (NebulaClient client = new NebulaClient(config)) {
            long[] latNanos = new long[rows];
            int[] perShard = new int[shards];
            AtomicLong next = new AtomicLong();
            AtomicLong errors = new AtomicLong();
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            long start = System.nanoTime();
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                fs.add(pool.submit(() -> {
                    long i;
                    while ((i = next.getAndIncrement()) < rows) {
                        String key = "users:" + i;
                        long t0 = System.nanoTime();
                        try {
                            client.put(key, "user-" + i);
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                        latNanos[(int) i] = System.nanoTime() - t0;
                    }
                    return null;
                }));
            }
            for (Future<?> f : fs) f.get();
            long elapsed = System.nanoTime() - start;
            pool.shutdown();
            for (int i = 0; i < rows; i++) perShard[client.shardFor("users:" + i)]++;

            long[] sorted = latNanos.clone();
            Arrays.sort(sorted);
            System.out.printf("throughput     : %.0f writes/s (%.1f s total)%n", rows / (elapsed / 1e9), elapsed / 1e9);
            System.out.printf("latency (ms)   : p50=%.2f p95=%.2f p99=%.2f%n",
                    pct(sorted, 50), pct(sorted, 95), pct(sorted, 99));
            System.out.printf("errors         : %d%n", errors.get());
            System.out.printf("rows per shard : %s%n", Arrays.toString(perShard));

            Random rnd = new Random(42);
            int sample = Math.min(1000, rows), bad = 0;
            for (int k = 0; k < sample; k++) {
                int i = rnd.nextInt(rows);
                if (!client.get("users:" + i).map(v -> v.equals("user-" + i)).orElse(false)) bad++;
            }
            System.out.printf("read-back check: %d/%d sampled rows correct%n", sample - bad, sample);
            return errors.get() == 0 && bad == 0 ? 0 : 1;
        } finally {
            if (local != null) local.close();
        }
    }

    private static double pct(long[] sorted, int p) {
        int idx = Math.min(sorted.length - 1, Math.max(0, (int) Math.ceil(sorted.length * p / 100.0) - 1));
        return sorted[idx] / 1e6;
    }
}
