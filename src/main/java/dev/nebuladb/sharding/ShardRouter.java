package dev.nebuladb.sharding;

/** Hash partitioning: a key always maps to the same shard. */
public final class ShardRouter {
    private final int count;

    public ShardRouter(int count) {
        if (count <= 0) throw new IllegalArgumentException("shard count must be positive");
        this.count = count;
    }

    public int shardFor(String key) {
        return Math.floorMod(key.hashCode(), count);
    }

    public int shardCount() {
        return count;
    }
}
