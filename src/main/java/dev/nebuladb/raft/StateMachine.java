package dev.nebuladb.raft;

import java.io.IOException;

/** What Raft replicates INTO. Entries are applied in log order, exactly once per process lifetime. */
public interface StateMachine {
    /** Last log index already reflected in durable state (0 if none). Raft resumes applying after it. */
    long lastApplied();

    /** Applies a committed entry and returns the result for the waiting client (may be null). */
    String apply(LogEntry entry) throws IOException;

    /** Graceful close. */
    void close();

    /** Simulated crash: release resources without flushing. */
    void crash();
}
