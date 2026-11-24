package dev.nebuladb.raft;

/** Thrown when a request reaches a replica that is not (or is no longer) the leader. */
public final class NotLeaderException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super("not leader" + (leaderHint == null ? "" : "; leader is " + leaderHint));
        this.leaderHint = leaderHint;
    }

    /** Best guess at the current leader's node id, or null if unknown. */
    public String leaderHint() {
        return leaderHint;
    }
}
