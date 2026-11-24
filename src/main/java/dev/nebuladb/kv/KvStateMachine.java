package dev.nebuladb.kv;

import dev.nebuladb.raft.LogEntry;
import dev.nebuladb.raft.StateMachine;
import dev.nebuladb.storage.LsmStorageEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Applies committed Raft entries to this replica's LSM storage engine.
 *
 * After each write it also stores the applied log index under a reserved key, so
 * after a restart Raft resumes applying from the right place.
 *
 * The LSM WAL here is NOT fsynced per write (the Raft log is, and it is the source
 * of truth). That is safe because the WAL is append-only and replay stops at the
 * first torn record: if a crash loses a suffix of the WAL, it loses the data writes
 * AND the later applied-index markers together, so recovery just re-applies those
 * entries from the Raft log. PUT and DELETE are idempotent, so re-applying is fine.
 */
public final class KvStateMachine implements StateMachine {
    static final String APPLIED_KEY = "\u0000raft:applied";

    private final LsmStorageEngine store;
    private long lastApplied;

    public KvStateMachine(Path dir) throws IOException {
        this.store = new LsmStorageEngine(dir, 4096, false);
        this.lastApplied = store.get(APPLIED_KEY).map(Long::parseLong).orElse(0L);
    }

    @Override
    public synchronized long lastApplied() {
        return lastApplied;
    }

    @Override
    public synchronized String apply(LogEntry entry) throws IOException {
        Command c = Command.decode(entry.command());
        String result = null;
        if (c != null) {
            switch (c.op()) {
                case PUT -> store.put(c.key(), c.value());
                case DELETE -> store.delete(c.key());
                case GET -> result = store.get(c.key()).orElse(null);
            }
        }
        if (c == null || c.op() != Command.Op.GET) store.put(APPLIED_KEY, Long.toString(entry.index()));
        lastApplied = entry.index();
        return result;
    }

    /** Direct local read (bypasses Raft). Used by tests and the consistency checker only. */
    public synchronized Optional<String> readLocal(String key) throws IOException {
        return store.get(key);
    }

    @Override
    public synchronized void close() {
        store.close();
    }

    @Override
    public synchronized void crash() {
        store.abandon();
    }
}
