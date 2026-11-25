package dev.nebuladb.net;

import dev.nebuladb.net.grpc.GrpcRpcServer;
import dev.nebuladb.net.grpc.GrpcTransport;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;

/**
 * Picks the RPC implementation. Both carry the same messages; Raft and the client
 * only see NodeTransport / RpcEndpoint.
 *  - GRPC: gRPC over HTTP/2 with Protocol Buffers (src/main/proto/nebula.proto). Default.
 *  - HTTP: plain JDK HTTP/1.1 with the hand-written Codec. Zero dependencies.
 */
public final class Transports {
    public enum Kind {
        GRPC, HTTP;

        public static Kind parse(String s) {
            return s == null ? GRPC : Kind.valueOf(s.toUpperCase(Locale.ROOT));
        }
    }

    private Transports() {}

    public static NodeTransport client(Kind kind, Map<String, String> addresses, Duration timeout) {
        return switch (kind) {
            case GRPC -> new GrpcTransport(addresses, timeout);
            case HTTP -> new HttpTransport(addresses, timeout);
        };
    }

    public static RpcEndpoint server(Kind kind, String host, int port, RpcServer.Handler handler) throws IOException {
        return switch (kind) {
            case GRPC -> new GrpcRpcServer(port, handler);
            case HTTP -> new RpcServer(host, port, handler);
        };
    }
}
