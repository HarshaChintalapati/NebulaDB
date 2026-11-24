package dev.nebuladb.raft;

import java.util.List;

/**
 * Raft RPC messages (Raft paper, Figure 2). Every request carries the Raft group
 * id because one server hosts one replica of several shards, each shard being its
 * own Raft group.
 */
public final class Messages {
    private Messages() {}

    public record VoteRequest(int group, long term, String candidateId, long lastLogIndex, long lastLogTerm) {}

    public record VoteResponse(long term, boolean granted) {}

    public record AppendRequest(int group, long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) {}

    /**
     * matchIndex: on success, the follower's last index covered by this request.
     * On failure, a hint: the leader should retry from matchIndex + 1.
     */
    public record AppendResponse(long term, boolean success, long matchIndex) {}
}
