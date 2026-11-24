package dev.nebuladb.net;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.nebuladb.raft.Messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** Server side of HttpTransport: decodes requests and hands them to the node. */
public final class RpcServer {
    static {
        // Disable Nagle's algorithm on accepted sockets. Without this, small
        // request/response pairs hit the ~40 ms delayed-ACK stall on every RPC.
        System.setProperty("sun.net.httpserver.nodelay", "true");
    }

    public interface Handler {
        Messages.VoteResponse vote(Messages.VoteRequest request);

        Messages.AppendResponse append(Messages.AppendRequest request);

        Codec.KvResponse kv(Codec.KvRequest request);
    }

    private final HttpServer server;
    private final ExecutorService executor;

    public RpcServer(String host, int port, Handler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress(host, port), 128);
        executor = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "rpc-" + port);
            t.setDaemon(true);
            return t;
        });
        server.setExecutor(executor);
        route("/raft/vote", b -> Codec.encode(handler.vote(Codec.decodeVoteRequest(b))));
        route("/raft/append", b -> Codec.encode(handler.append(Codec.decodeAppendRequest(b))));
        route("/kv", b -> Codec.encode(handler.kv(Codec.decodeKvRequest(b))));
    }

    private void route(String path, Function<byte[], byte[]> fn) {
        server.createContext(path, ex -> {
            try (ex) {
                byte[] body;
                try (InputStream in = ex.getRequestBody()) {
                    body = in.readAllBytes();
                }
                byte[] out;
                try {
                    out = fn.apply(body);
                } catch (RuntimeException e) {
                    reply(ex, 500, String.valueOf(e.getMessage()).getBytes());
                    return;
                }
                reply(ex, 200, out);
            }
        });
    }

    private static void reply(HttpExchange ex, int code, byte[] body) throws IOException {
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    public void stop() {
        server.stop(0);
        executor.shutdownNow();
        try {
            executor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
