package dev.nebuladb;

import dev.nebuladb.query.QueryCoordinator;
import dev.nebuladb.sql.SimpleSqlParser;
import dev.nebuladb.storage.LsmStorageEngine;
import dev.nebuladb.sharding.ShardRouter;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public final class NebulaDbApplication {
    public static void main(String[] args) throws Exception {
        int shardCount = 3;
        Map<Integer, LsmStorageEngine> stores = new HashMap<>();
        for (int i = 0; i < shardCount; i++) stores.put(i, new LsmStorageEngine(Path.of("data/shard-" + i), 128));
        QueryCoordinator coordinator = new QueryCoordinator(new ShardRouter(shardCount), stores);
        SimpleSqlParser parser = new SimpleSqlParser();

        System.out.println("NebulaDB educational MVP. Type SQL or 'exit'.");
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            while (true) {
                System.out.print("nebula> ");
                String line = br.readLine();
                if (line == null || line.equalsIgnoreCase("exit")) break;
                try { System.out.println(coordinator.execute(parser.parse(line))); }
                catch (Exception e) { System.out.println("ERROR: " + e.getMessage()); }
            }
        } finally {
            for (LsmStorageEngine s : stores.values()) s.close();
        }
    }
}
