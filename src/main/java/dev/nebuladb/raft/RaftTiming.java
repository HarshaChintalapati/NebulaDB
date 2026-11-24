package dev.nebuladb.raft;

/**
 * Election timeout is randomized in [min, max] so that two followers rarely time
 * out together and split the vote. Heartbeats must be much shorter than the
 * minimum election timeout, otherwise followers start needless elections.
 */
public record RaftTiming(long electionMinMs, long electionMaxMs, long heartbeatMs, int maxEntriesPerAppend) {
    public static RaftTiming defaults() {
        return new RaftTiming(300, 600, 75, 512);
    }

    public static RaftTiming fast() {
        return new RaftTiming(150, 300, 40, 512);
    }
}
