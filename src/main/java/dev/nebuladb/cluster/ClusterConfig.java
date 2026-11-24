package dev.nebuladb.cluster;

import java.util.*;

/**
 * Static cluster layout: which nodes exist, how many shards, and how many
 * replicas each shard has. Every node and client gets the same config.
 *
 * Placement: shard s lives on nodes (s, s+1, ..., s+rf-1) mod N, in config order.
 * With 5 nodes, 10 shards and rf=3, every node holds 6 replicas, and losing any
 * single node leaves every shard with a 2-of-3 majority.
 */
public record ClusterConfig(LinkedHashMap<String, String> nodes, int shards, int replicationFactor) {
    public ClusterConfig {
        if (nodes.isEmpty()) throw new IllegalArgumentException("no nodes");
        if (replicationFactor < 1 || replicationFactor > nodes.size()) {
            throw new IllegalArgumentException("replication factor must be between 1 and the node count");
        }
        if (shards < 1) throw new IllegalArgumentException("need at least one shard");
    }

    /** Parses "n1=host:7001,n2=host:7002,...". */
    public static ClusterConfig parse(String nodeList, int shards, int rf) {
        LinkedHashMap<String, String> nodes = new LinkedHashMap<>();
        for (String part : nodeList.split(",")) {
            String[] kv = part.trim().split("=", 2);
            if (kv.length != 2) throw new IllegalArgumentException("bad node spec: " + part);
            nodes.put(kv[0].trim(), kv[1].trim());
        }
        return new ClusterConfig(nodes, shards, rf);
    }

    public List<String> nodeIds() {
        return new ArrayList<>(nodes.keySet());
    }

    public List<String> replicas(int shard) {
        List<String> ids = nodeIds();
        List<String> out = new ArrayList<>();
        for (int i = 0; i < replicationFactor; i++) out.add(ids.get((shard + i) % ids.size()));
        return out;
    }

    public List<Integer> shardsOn(String nodeId) {
        List<Integer> out = new ArrayList<>();
        for (int s = 0; s < shards; s++) if (replicas(s).contains(nodeId)) out.add(s);
        return out;
    }
}
