package dev.nebuladb.net;

import dev.nebuladb.raft.Messages;
import dev.nebuladb.raft.RaftTransport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Raft and client RPCs over HTTP/1.1 using only the JDK (java.net.http).
 * Bodies are binary (see Codec). Every call has a timeout so a dead peer turns
 * into a failed future instead of a hung request.
 */
public final class HttpTransport implements RaftTransport, AutoCloseable {
    private final Map<String, String> addresses; // node id -> host:port
    private final HttpClient http;
    private final ExecutorService executor;
    private final Duration timeout;

    public HttpTransport(Map<String, String> addresses, Duration timeout) {
        this.addresses = Map.copyOf(addresses);
        this.timeout = timeout;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "http-client");
            t.setDaemon(true);
            return t;
        });
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(timeout)
                .executor(executor)
                .build();
    }

    @Override
    public CompletableFuture<Messages.VoteResponse> requestVote(String target, Messages.VoteRequest request) {
        return post(target, "/raft/vote", Codec.encode(request), Codec::decodeVoteResponse);
    }

    @Override
    public CompletableFuture<Messages.AppendResponse> appendEntries(String target, Messages.AppendRequest request) {
        return post(target, "/raft/append", Codec.encode(request), Codec::decodeAppendResponse);
    }

    public CompletableFuture<Codec.KvResponse> kv(String target, Codec.KvRequest request) {
        return post(target, "/kv", Codec.encode(request), Codec::decodeKvResponse);
    }

    private <T> CompletableFuture<T> post(String target, String path, byte[] body, Function<byte[], T> decode) {
        String addr = addresses.get(target);
        if (addr == null) return CompletableFuture.failedFuture(new IllegalArgumentException("unknown node " + target));
        HttpRequest req = HttpRequest.newBuilder(URI.create("http://" + addr + path))
                .timeout(timeout)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return http.sendAsync(req, HttpResponse.BodyHandlers.ofByteArray()).thenApply(resp -> {
            if (resp.statusCode() != 200) throw new IllegalStateException(target + path + " -> HTTP " + resp.statusCode());
            return decode.apply(resp.body());
        });
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }
}
