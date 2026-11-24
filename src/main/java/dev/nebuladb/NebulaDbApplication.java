package dev.nebuladb;

import dev.nebuladb.bench.LoadBench;
import dev.nebuladb.chaos.LeaderFailoverChaos;
import dev.nebuladb.cluster.ClusterConfig;
import dev.nebuladb.cluster.NebulaClient;
import dev.nebuladb.cluster.NebulaNode;
import dev.nebuladb.kv.KvStore;
import dev.nebuladb.kv.LocalShardedStore;
import dev.nebuladb.query.QueryCoordinator;
import dev.nebuladb.raft.RaftTiming;
import dev.nebuladb.sql.SimpleSqlParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Entry point.
 *
 *   server --id n1 --cluster n1=host:7001,n2=host:7002,... [--shards 10 --rf 3 --data ./data]
 *   shell  --cluster n1=host:7001,...  [--shards 10 --rf 3]      SQL shell against a cluster
 *   local  [--data ./data --shards 3]                            single process, no replication
 *   chaos  [--failures 100 --nodes 5 --shards 10 --rf 3]         leader-failure test (in one JVM)
 *   bench  [--rows 100000 --threads 32] [--cluster ...]          write benchmark
 */
public final class NebulaDbApplication {
    public static void main(String[] argv) throws Exception {
        String mode = argv.length == 0 ? "local" : argv[0];
        Map<String, String> args = parse(argv);
        int shards = Integer.parseInt(args.getOrDefault("shards", "local".equals(mode) ? "3" : "10"));
        int rf = Integer.parseInt(args.getOrDefault("rf", "3"));
        switch (mode) {
            case "server" -> {
                ClusterConfig config = ClusterConfig.parse(require(args, "cluster"), shards, rf);
                String id = require(args, "id");
                NebulaNode node = new NebulaNode(id, config, Path.of(args.getOrDefault("data", "data")).resolve(id),
                        RaftTiming.defaults(), true);
                node.start();
                System.out.printf("NebulaDB node %s up on %s, replicas of shards %s%n",
                        id, config.nodes().get(id), config.shardsOn(id));
                Runtime.getRuntime().addShutdownHook(new Thread(node::close));
                new CountDownLatch(1).await();
            }
            case "shell" -> {
                try (NebulaClient client = new NebulaClient(ClusterConfig.parse(require(args, "cluster"), shards, rf))) {
                    repl(client, "NebulaDB cluster shell. Type SQL or 'exit'.");
                }
            }
            case "local" -> {
                try (LocalShardedStore store = new LocalShardedStore(Path.of(args.getOrDefault("data", "data")), shards)) {
                    repl(store, "NebulaDB local mode (single process, no replication). Type SQL or 'exit'.");
                }
            }
            case "chaos" -> System.exit(LeaderFailoverChaos.main(args));
            case "bench" -> System.exit(LoadBench.main(args));
            default -> {
                System.err.println("usage: server | shell | local | chaos | bench  (see README)");
                System.exit(2);
            }
        }
    }

    private static void repl(KvStore store, String banner) throws Exception {
        QueryCoordinator coordinator = new QueryCoordinator(store);
        SimpleSqlParser parser = new SimpleSqlParser();
        System.out.println(banner);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.print("nebula> ");
                String line = br.readLine();
                if (line == null || line.equalsIgnoreCase("exit")) break;
                if (line.isBlank()) continue;
                try {
                    System.out.println(coordinator.execute(parser.parse(line)));
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        }
    }

    private static Map<String, String> parse(String[] argv) {
        Map<String, String> out = new HashMap<>();
        for (int i = 1; i < argv.length; i++) {
            if (argv[i].startsWith("--")) {
                String k = argv[i].substring(2);
                String v = i + 1 < argv.length && !argv[i + 1].startsWith("--") ? argv[++i] : "true";
                out.put(k, v);
            }
        }
        return out;
    }

    private static String require(Map<String, String> args, String key) {
        String v = args.get(key);
        if (v == null) throw new IllegalArgumentException("missing --" + key);
        return v;
    }
}
