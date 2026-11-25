package dev.nebuladb.net.grpc;

import dev.nebuladb.net.RpcEndpoint;
import dev.nebuladb.net.RpcServer;
import dev.nebuladb.net.grpc.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * gRPC server side: implements the generated NebulaRpc service by delegating to
 * the node's handler. Handlers block (a client write waits for Raft commit), so
 * they run on a dedicated thread pool, not on gRPC's network threads.
 */
public final class GrpcRpcServer implements RpcEndpoint {
    private final Server server;
    private final ExecutorService executor;

    public GrpcRpcServer(int port, RpcServer.Handler handler) {
        this.executor = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "grpc-" + port);
            t.setDaemon(true);
            return t;
        });
        this.server = ServerBuilder.forPort(port)
                .executor(executor)
                .addService(new NebulaRpcGrpc.NebulaRpcImplBase() {
                    @Override
                    public void requestVote(VoteRequest req, StreamObserver<VoteResponse> out) {
                        reply(out, () -> ProtoMapper.toProto(handler.vote(ProtoMapper.fromProto(req))));
                    }

                    @Override
                    public void appendEntries(AppendRequest req, StreamObserver<AppendResponse> out) {
                        reply(out, () -> ProtoMapper.toProto(handler.append(ProtoMapper.fromProto(req))));
                    }

                    @Override
                    public void kv(KvRequest req, StreamObserver<KvResponse> out) {
                        reply(out, () -> ProtoMapper.toProto(handler.kv(ProtoMapper.fromProto(req))));
                    }
                })
                .build();
    }

    private static <T> void reply(StreamObserver<T> out, Supplier<T> call) {
        T result;
        try {
            result = call.get();
        } catch (RuntimeException e) {
            out.onError(Status.UNAVAILABLE.withDescription(String.valueOf(e.getMessage())).asRuntimeException());
            return;
        }
        out.onNext(result);
        out.onCompleted();
    }

    @Override
    public void start() throws IOException {
        server.start();
    }

    @Override
    public void stop() {
        server.shutdownNow();
        try {
            server.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        executor.shutdownNow();
    }
}
