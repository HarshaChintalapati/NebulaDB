# Roadmap to the full resume version

The repository you have now is the **working educational MVP**. Build/understand it first, then extend it in this order:

1. Replace local direct writes with Raft state-machine application.
2. Add a real `RaftTransport` and gRPC `RequestVote` / `AppendEntries` RPCs.
3. Run 3 physical nodes in Docker Compose.
4. Implement `nextIndex` / `matchIndex`, retry/backtracking, follower catch-up, and heartbeats.
5. Persist Raft `currentTerm`, `votedFor`, and log before acknowledging peers.
6. Add Raft snapshots and log compaction.
7. Persist the SQL catalog/schema.
8. Add scans and scatter-gather query execution.
9. Add MVCC and single-shard transactions.
10. Benchmark throughput, p50/p95/p99, failover time, recovery time, and compaction overhead.

Only after these are implemented should the resume claim the corresponding features.
