package dev.nebuladb.raft;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Test transport: delivers RPCs between RaftNodes in the same JVM, asynchronously,
 * and can isolate a node (drop everything to and from it) to simulate a partition.
 */
final class InMemoryNetwork implements RaftTransport {
    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Set<String> isolated = ConcurrentHashMap.newKeySet();
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "net");
        t.setDaemon(true);
        return t;
    });

    void register(String id, RaftNode node) {
        nodes.put(id, node);
    }

    void unregister(String id) {
        nodes.remove(id);
    }

    void isolate(String id) {
        isolated.add(id);
    }

    void heal() {
        isolated.clear();
    }

    private boolean blocked(String from, String to) {
        return isolated.contains(from) || isolated.contains(to) || !nodes.containsKey(to);
    }

    @Override
    public CompletableFuture<Messages.VoteResponse> requestVote(String target, Messages.VoteRequest req) {
        if (blocked(req.candidateId(), target)) return CompletableFuture.failedFuture(new RuntimeException("unreachable"));
        return CompletableFuture.supplyAsync(() -> nodes.get(target).handleVote(req), pool);
    }

    @Override
    public CompletableFuture<Messages.AppendResponse> appendEntries(String target, Messages.AppendRequest req) {
        if (blocked(req.leaderId(), target)) return CompletableFuture.failedFuture(new RuntimeException("unreachable"));
        return CompletableFuture.supplyAsync(() -> {
            if (blocked(req.leaderId(), target)) throw new RuntimeException("unreachable");
            return nodes.get(target).handleAppend(req);
        }, pool);
    }

    void shutdown() {
        pool.shutdownNow();
    }
}
