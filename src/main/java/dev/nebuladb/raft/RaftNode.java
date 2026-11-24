package dev.nebuladb.raft;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

/**
 * One replica in one Raft group (Ongaro and Ousterhout, "In Search of an
 * Understandable Consensus Algorithm", Figure 2).
 *
 * Threads:
 *  - ticker: every few ms, followers/candidates check the election deadline and
 *    the leader sends heartbeats. RPC replies are also processed on this thread.
 *  - applier: applies committed entries to the state machine in order and
 *    completes the client futures waiting on them.
 * All Raft state is guarded by `this`. The state machine is called outside the
 * lock so a slow disk write does not block RPC handling.
 *
 * A client write is "acknowledged" only after its entry is committed (stored on a
 * majority) AND applied on the leader.
 */
public final class RaftNode implements Closeable {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    public record Status(String id, int group, Role role, long term, String leaderId,
                         long commitIndex, long lastApplied, long lastLogIndex) {}

    private record Pending(long term, CompletableFuture<String> future) {}

    /** Empty command = no-op. State machines must ignore it. */
    private static final byte[] NOOP = new byte[0];

    private final String id;
    private final int group;
    private final List<String> peers;
    private final RaftTransport transport;
    private final RaftLog log;
    private final RaftMeta meta;
    private final StateMachine stateMachine;
    private final RaftTiming timing;
    private final Random random = new Random();

    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;
    private long electionDeadline;
    private long lastBroadcast;
    private final Set<String> votes = new HashSet<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Set<String> inflight = new HashSet<>();
    private final Map<Long, Pending> pending = new HashMap<>();

    private final ScheduledExecutorService ticker;
    private final Thread applier;
    private volatile boolean running;

    public RaftNode(String id, int group, List<String> peers, RaftTransport transport, Path dir,
                    StateMachine stateMachine, RaftTiming timing, boolean fsync) throws IOException {
        this.id = id;
        this.group = group;
        this.peers = List.copyOf(peers);
        this.transport = transport;
        this.log = new RaftLog(dir.resolve("raft.log"), fsync);
        this.meta = new RaftMeta(dir.resolve("raft.meta"));
        this.stateMachine = stateMachine;
        this.timing = timing;
        // Anything already applied was committed before the crash.
        this.lastApplied = Math.min(stateMachine.lastApplied(), log.lastIndex());
        this.commitIndex = lastApplied;
        this.ticker = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "raft-" + id + "-g" + group));
        this.applier = daemon(this::applyLoop, "apply-" + id + "-g" + group);
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    // ------------------------------------------------------------------ lifecycle

    public synchronized void start() {
        running = true;
        resetElectionDeadline();
        ticker.scheduleAtFixedRate(this::tick, 5, 5, TimeUnit.MILLISECONDS);
        applier.start();
    }

    /** Graceful shutdown. */
    @Override
    public void close() {
        shutdown(false);
    }

    /** Simulated crash: stop everything without flushing the state machine. */
    public void crash() {
        shutdown(true);
    }

    private void shutdown(boolean crash) {
        synchronized (this) {
            if (!running) return;
            running = false;
            failPending(new NotLeaderException(null));
            notifyAll();
        }
        ticker.shutdownNow();
        applier.interrupt();
        try {
            ticker.awaitTermination(2, TimeUnit.SECONDS);
            applier.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (this) {
            try {
                log.close();
            } catch (IOException ignored) {
            }
        }
        if (crash) stateMachine.crash();
        else stateMachine.close();
    }

    // ------------------------------------------------------------------ client API

    /**
     * Appends a command to the log. The future completes with the state machine's
     * result once the entry is committed and applied, or fails with
     * NotLeaderException if leadership is lost first (the client then retries).
     */
    public synchronized CompletableFuture<String> propose(byte[] command) {
        if (!running || role != Role.LEADER) throw new NotLeaderException(leaderId);
        LogEntry e = new LogEntry(log.lastIndex() + 1, meta.currentTerm(), command);
        try {
            log.append(List.of(e));
        } catch (IOException ex) {
            return CompletableFuture.failedFuture(ex);
        }
        CompletableFuture<String> f = new CompletableFuture<>();
        pending.put(e.index(), new Pending(e.term(), f));
        if (peers.isEmpty()) advanceCommitIndex();
        for (String p : peers) sendAppend(p);
        return f;
    }

    public synchronized Status status() {
        return new Status(id, group, role, meta.currentTerm(), leaderId, commitIndex, lastApplied, log.lastIndex());
    }

    public synchronized boolean isLeader() {
        return running && role == Role.LEADER;
    }

    // ------------------------------------------------------------------ timers

    private void resetElectionDeadline() {
        long span = timing.electionMaxMs() - timing.electionMinMs();
        long timeout = timing.electionMinMs() + (span > 0 ? random.nextLong(span + 1) : 0);
        electionDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
    }

    private synchronized void tick() {
        if (!running) return;
        long now = System.nanoTime();
        if (role == Role.LEADER) {
            if (now - lastBroadcast >= TimeUnit.MILLISECONDS.toNanos(timing.heartbeatMs())) broadcastAppend();
        } else if (now >= electionDeadline) {
            startElection();
        }
    }

    // ------------------------------------------------------------------ elections

    private void startElection() {
        try {
            meta.set(meta.currentTerm() + 1, id); // vote for self, durably
        } catch (IOException e) {
            resetElectionDeadline();
            return;
        }
        role = Role.CANDIDATE;
        leaderId = null;
        votes.clear();
        votes.add(id);
        resetElectionDeadline(); // if this election splits, a new one starts later
        long term = meta.currentTerm();
        if (hasMajority(votes.size())) {
            becomeLeader();
            return;
        }
        Messages.VoteRequest req = new Messages.VoteRequest(group, term, id, log.lastIndex(), log.lastTerm());
        for (String p : peers) {
            transport.requestVote(p, req).whenCompleteAsync((resp, err) -> onVoteResponse(term, p, resp), ticker);
        }
    }

    private synchronized void onVoteResponse(long electionTerm, String peer, Messages.VoteResponse resp) {
        if (!running || resp == null) return;
        if (resp.term() > meta.currentTerm()) {
            stepDown(resp.term());
            return;
        }
        if (role != Role.CANDIDATE || meta.currentTerm() != electionTerm || !resp.granted()) return;
        votes.add(peer);
        if (hasMajority(votes.size())) becomeLeader();
    }

    private boolean hasMajority(int count) {
        return count > (peers.size() + 1) / 2;
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        inflight.clear();
        for (String p : peers) {
            nextIndex.put(p, log.lastIndex() + 1);
            matchIndex.put(p, 0L);
        }
        // A leader may only count replicas for entries of its OWN term (Raft 5.4.2).
        // Appending a no-op lets it commit, which also commits every earlier entry.
        try {
            log.append(List.of(new LogEntry(log.lastIndex() + 1, meta.currentTerm(), NOOP)));
        } catch (IOException e) {
            stepDown(meta.currentTerm());
            return;
        }
        if (peers.isEmpty()) advanceCommitIndex();
        broadcastAppend();
    }

    /** Become follower; adopt a newer term if we saw one. */
    private void stepDown(long term) {
        if (term > meta.currentTerm()) {
            try {
                meta.set(term, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (role != Role.FOLLOWER) {
            role = Role.FOLLOWER;
            inflight.clear();
            // Our uncommitted entries may still commit under the next leader, but we
            // can't promise that, so tell the clients to retry (writes are idempotent).
            failPending(new NotLeaderException(null));
        }
        resetElectionDeadline();
    }

    public synchronized Messages.VoteResponse handleVote(Messages.VoteRequest req) {
        if (!running) throw new IllegalStateException("stopped");
        if (req.term() > meta.currentTerm()) stepDown(req.term());
        boolean granted = false;
        if (req.term() == meta.currentTerm()) {
            String voted = meta.votedFor();
            boolean canVote = voted == null || voted.equals(req.candidateId());
            // Election restriction: only vote for a candidate whose log is at least as
            // up to date as ours, so a new leader always has every committed entry.
            boolean upToDate = req.lastLogTerm() > log.lastTerm()
                    || (req.lastLogTerm() == log.lastTerm() && req.lastLogIndex() >= log.lastIndex());
            if (canVote && upToDate) {
                try {
                    meta.set(req.term(), req.candidateId());
                    granted = true;
                    resetElectionDeadline();
                } catch (IOException ignored) {
                }
            }
        }
        return new Messages.VoteResponse(meta.currentTerm(), granted);
    }

    // ------------------------------------------------------------------ replication (leader)

    private void broadcastAppend() {
        lastBroadcast = System.nanoTime();
        for (String p : peers) sendAppend(p);
    }

    private void sendAppend(String peer) {
        if (role != Role.LEADER || inflight.contains(peer)) return; // one RPC in flight per peer
        long next = nextIndex.get(peer);
        long prev = next - 1;
        Messages.AppendRequest req = new Messages.AppendRequest(group, meta.currentTerm(), id, prev,
                log.termAt(prev), log.slice(next, timing.maxEntriesPerAppend()), commitIndex);
        inflight.add(peer);
        transport.appendEntries(peer, req).whenCompleteAsync((resp, err) -> onAppendResponse(peer, req, resp), ticker);
    }

    private synchronized void onAppendResponse(String peer, Messages.AppendRequest req, Messages.AppendResponse resp) {
        inflight.remove(peer);
        if (!running || resp == null) return; // network error: the next heartbeat retries
        if (resp.term() > meta.currentTerm()) {
            stepDown(resp.term());
            return;
        }
        if (role != Role.LEADER || req.term() != meta.currentTerm()) return; // stale reply
        if (resp.success()) {
            long m = req.prevLogIndex() + req.entries().size();
            if (m > matchIndex.get(peer)) matchIndex.put(peer, m);
            nextIndex.put(peer, Math.max(nextIndex.get(peer), m + 1));
            advanceCommitIndex();
        } else {
            // Log mismatch: back up (using the follower's hint) and retry.
            long next = Math.max(1, Math.min(nextIndex.get(peer) - 1, resp.matchIndex() + 1));
            nextIndex.put(peer, next);
        }
        if (nextIndex.get(peer) <= log.lastIndex()) sendAppend(peer); // keep catching up
    }

    /** Commit the highest index stored on a majority, if it is from the current term. */
    private void advanceCommitIndex() {
        for (long n = log.lastIndex(); n > commitIndex; n--) {
            if (log.termAt(n) != meta.currentTerm()) break; // older terms commit indirectly
            int count = 1; // self
            for (String p : peers) if (matchIndex.get(p) >= n) count++;
            if (hasMajority(count)) {
                commitIndex = n;
                notifyAll(); // wake the applier
                return;
            }
        }
    }

    // ------------------------------------------------------------------ replication (follower)

    public synchronized Messages.AppendResponse handleAppend(Messages.AppendRequest req) {
        if (!running) throw new IllegalStateException("stopped");
        if (req.term() < meta.currentTerm()) {
            return new Messages.AppendResponse(meta.currentTerm(), false, log.lastIndex());
        }
        if (req.term() > meta.currentTerm() || role != Role.FOLLOWER) stepDown(req.term());
        leaderId = req.leaderId();
        resetElectionDeadline();

        long prev = req.prevLogIndex();
        if (prev > log.lastIndex()) {
            return new Messages.AppendResponse(meta.currentTerm(), false, log.lastIndex());
        }
        if (log.termAt(prev) != req.prevLogTerm()) {
            // Skip back over the whole conflicting term in one round trip.
            return new Messages.AppendResponse(meta.currentTerm(), false, log.firstIndexOfTerm(prev) - 1);
        }
        try {
            List<LogEntry> toAppend = new ArrayList<>();
            for (LogEntry e : req.entries()) {
                if (e.index() <= log.lastIndex()) {
                    if (log.termAt(e.index()) == e.term()) continue; // already have it
                    if (e.index() <= commitIndex) {
                        throw new IllegalStateException("leader tried to overwrite committed entry " + e.index());
                    }
                    log.truncateFrom(e.index()); // conflicting uncommitted suffix
                }
                toAppend.add(e);
            }
            log.append(toAppend);
        } catch (IOException e) {
            return new Messages.AppendResponse(meta.currentTerm(), false, log.lastIndex());
        }
        long lastNew = prev + req.entries().size();
        long newCommit = Math.min(req.leaderCommit(), lastNew);
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            notifyAll();
        }
        return new Messages.AppendResponse(meta.currentTerm(), true, lastNew);
    }

    // ------------------------------------------------------------------ apply

    private void applyLoop() {
        while (running) {
            List<LogEntry> batch;
            synchronized (this) {
                while (running && lastApplied >= commitIndex) {
                    try {
                        wait(50);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (!running) return;
                batch = log.slice(lastApplied + 1, (int) Math.min(1024, commitIndex - lastApplied));
            }
            for (LogEntry e : batch) {
                String result;
                try {
                    result = stateMachine.apply(e);
                } catch (Exception ex) {
                    if (!running) return;
                    throw new IllegalStateException("state machine failed at index " + e.index(), ex);
                }
                synchronized (this) {
                    lastApplied = e.index();
                    Pending p = pending.remove(e.index());
                    if (p != null) {
                        if (p.term() == e.term()) p.future().complete(result);
                        else p.future().completeExceptionally(new NotLeaderException(leaderId));
                    }
                }
            }
        }
    }

    private void failPending(RuntimeException cause) {
        for (Pending p : pending.values()) p.future().completeExceptionally(cause);
        pending.clear();
    }
}
