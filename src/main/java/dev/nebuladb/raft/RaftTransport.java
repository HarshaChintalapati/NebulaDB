package dev.nebuladb.raft;

import java.util.concurrent.CompletableFuture;

/**
 * How a Raft node talks to its peers. Production uses HttpTransport (JDK HTTP);
 * the unit tests use an in-memory transport that can drop messages to simulate
 * partitions. A gRPC transport would implement this same interface.
 *
 * Futures complete exceptionally on network errors or timeouts.
 */
public interface RaftTransport {
    CompletableFuture<Messages.VoteResponse> requestVote(String target, Messages.VoteRequest request);

    CompletableFuture<Messages.AppendResponse> appendEntries(String target, Messages.AppendRequest request);
}
