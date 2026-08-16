package dev.nebuladb.sharding;
public final class ShardRouter{ private final int count; public ShardRouter(int count){if(count<=0)throw new IllegalArgumentException();this.count=count;} public int shardFor(String key){return Math.floorMod(key.hashCode(),count);} }
