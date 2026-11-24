package dev.nebuladb.raft;

/** One Raft log entry. Indexes start at 1; index 0 means "before the first entry". */
public record LogEntry(long index, long term, byte[] command) {}
