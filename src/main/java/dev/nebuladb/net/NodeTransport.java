package dev.nebuladb.net;

import dev.nebuladb.raft.RaftTransport;

import java.util.concurrent.CompletableFuture;

/** Client side of a node's RPCs: Raft messages plus client key-value requests. */
public interface NodeTransport extends RaftTransport, AutoCloseable {
    CompletableFuture<Codec.KvResponse> kv(String target, Codec.KvRequest request);

    @Override
    void close();
}
