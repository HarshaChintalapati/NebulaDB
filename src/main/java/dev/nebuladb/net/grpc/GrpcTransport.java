package dev.nebuladb.net.grpc;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import dev.nebuladb.net.Codec;
import dev.nebuladb.net.NodeTransport;
import dev.nebuladb.net.grpc.proto.NebulaRpcGrpc;
import dev.nebuladb.raft.Messages;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * gRPC client side. One long-lived HTTP/2 channel per peer, reused for every RPC
 * (gRPC multiplexes concurrent calls over it). Every call has a deadline, so a
 * dead peer becomes a failed future instead of a hung call.
 */
public final class GrpcTransport implements NodeTransport {
    private final Map<String, String> addresses; // node id -> host:port
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final long timeoutMs;

    public GrpcTransport(Map<String, String> addresses, Duration timeout) {
        this.addresses = Map.copyOf(addresses);
        this.timeoutMs = timeout.toMillis();
    }

    private NebulaRpcGrpc.NebulaRpcFutureStub stub(String target) {
        ManagedChannel ch = channels.computeIfAbsent(target, t ->
                ManagedChannelBuilder.forTarget(addresses.get(t)).usePlaintext().build());
        // After a peer crashes, gRPC backs off reconnects exponentially (up to minutes).
        // Raft already retries every heartbeat, so reset the backoff and let the
        // restarted node be reachable again right away.
        if (ch.getState(false) == ConnectivityState.TRANSIENT_FAILURE) ch.resetConnectBackoff();
        return NebulaRpcGrpc.newFutureStub(ch).withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<Messages.VoteResponse> requestVote(String target, Messages.VoteRequest request) {
        if (!addresses.containsKey(target)) return unknown(target);
        return toCompletable(stub(target).requestVote(ProtoMapper.toProto(request)), ProtoMapper::fromProto);
    }

    @Override
    public CompletableFuture<Messages.AppendResponse> appendEntries(String target, Messages.AppendRequest request) {
        if (!addresses.containsKey(target)) return unknown(target);
        return toCompletable(stub(target).appendEntries(ProtoMapper.toProto(request)), ProtoMapper::fromProto);
    }

    @Override
    public CompletableFuture<Codec.KvResponse> kv(String target, Codec.KvRequest request) {
        if (!addresses.containsKey(target)) return unknown(target);
        return toCompletable(stub(target).kv(ProtoMapper.toProto(request)), ProtoMapper::fromProto);
    }

    private static <T> CompletableFuture<T> unknown(String target) {
        return CompletableFuture.failedFuture(new IllegalArgumentException("unknown node " + target));
    }

    /** Adapts gRPC's Guava ListenableFuture to the CompletableFuture the rest of the code uses. */
    private static <P, R> CompletableFuture<R> toCompletable(ListenableFuture<P> lf, Function<P, R> map) {
        CompletableFuture<R> cf = new CompletableFuture<>();
        Futures.addCallback(lf, new FutureCallback<P>() {
            @Override
            public void onSuccess(P result) {
                try {
                    cf.complete(map.apply(result));
                } catch (RuntimeException e) {
                    cf.completeExceptionally(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                cf.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor());
        return cf;
    }

    @Override
    public void close() {
        for (ManagedChannel ch : channels.values()) ch.shutdownNow();
        for (ManagedChannel ch : channels.values()) {
            try {
                ch.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }
}
