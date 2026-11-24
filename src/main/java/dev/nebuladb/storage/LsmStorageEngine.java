package dev.nebuladb.storage;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single-node LSM-tree storage engine.
 *
 * Write path: WAL append -> sorted MemTable -> (when full) flush to an SSTable,
 * then truncate the WAL. Read path: MemTable, then SSTables newest to oldest.
 * After 4 SSTables, all of them are merged into one (full compaction).
 *
 * SSTable files are named sst-<file number>.db and the number only grows, so the
 * file order on disk is the age order after a restart.
 */
public final class LsmStorageEngine implements StorageEngine {
    private static final int COMPACT_AT = 4;

    private final Path dir;
    private final int flushEntries;
    private final WriteAheadLog wal;
    private final AtomicLong seq = new AtomicLong();
    private long nextFileNo = 1;
    private ConcurrentSkipListMap<String, SSTable.Entry> mem = new ConcurrentSkipListMap<>();
    private final List<SSTable> tables = new ArrayList<>(); // oldest -> newest

    public LsmStorageEngine(Path dir, int flushEntries) throws IOException {
        this(dir, flushEntries, true);
    }

    public LsmStorageEngine(Path dir, int flushEntries, boolean syncWal) throws IOException {
        this.dir = dir;
        this.flushEntries = flushEntries;
        Files.createDirectories(dir);
        loadTables();
        wal = new WriteAheadLog(dir.resolve("wal.log"), syncWal);
        recover();
    }

    @Override
    public synchronized void put(String k, String v) throws IOException {
        long s = seq.incrementAndGet();
        wal.append(new WriteAheadLog.Record(s, WriteAheadLog.Op.PUT, k, v));
        mem.put(k, new SSTable.Entry(s, false, k, v));
        maybeFlush();
    }

    @Override
    public synchronized Optional<String> get(String k) throws IOException {
        SSTable.Entry m = mem.get(k);
        if (m != null) return m.tombstone() ? Optional.empty() : Optional.of(m.value());
        ListIterator<SSTable> it = tables.listIterator(tables.size());
        while (it.hasPrevious()) {
            Optional<SSTable.Entry> e = it.previous().get(k);
            if (e.isPresent()) return e.get().tombstone() ? Optional.empty() : Optional.of(e.get().value());
        }
        return Optional.empty();
    }

    @Override
    public synchronized void delete(String k) throws IOException {
        long s = seq.incrementAndGet();
        wal.append(new WriteAheadLog.Record(s, WriteAheadLog.Op.DELETE, k, null));
        mem.put(k, new SSTable.Entry(s, true, k, null));
        maybeFlush();
    }

    @Override
    public synchronized void flush() throws IOException {
        if (mem.isEmpty()) return;
        // ConcurrentSkipListMap values() are already in key order.
        SSTable t = SSTable.write(nextTablePath(), new ArrayList<>(mem.values()));
        tables.add(t);
        mem = new ConcurrentSkipListMap<>();
        wal.reset(); // safe: the SSTable was fsynced above
        if (tables.size() >= COMPACT_AT) compact();
    }

    public synchronized int sstableCount() {
        return tables.size();
    }

    private void maybeFlush() throws IOException {
        if (mem.size() >= flushEntries) flush();
    }

    /**
     * Merges all SSTables, keeping only the newest version of each key.
     * Tombstones are KEPT: if we crash after writing the merged file but before
     * deleting the old ones, a dropped tombstone would let an old value reappear.
     */
    private void compact() throws IOException {
        TreeMap<String, SSTable.Entry> latest = new TreeMap<>();
        for (SSTable t : tables) {
            for (SSTable.Entry e : t.all()) {
                SSTable.Entry cur = latest.get(e.key());
                if (cur == null || e.seq() > cur.seq()) latest.put(e.key(), e);
            }
        }
        SSTable merged = SSTable.write(nextTablePath(), latest.values());
        for (SSTable t : tables) Files.deleteIfExists(t.path());
        tables.clear();
        tables.add(merged);
    }

    private Path nextTablePath() {
        return dir.resolve(String.format("sst-%020d.db", nextFileNo++));
    }

    private void loadTables() throws IOException {
        List<Path> paths = new ArrayList<>();
        try (DirectoryStream<Path> s = Files.newDirectoryStream(dir, "sst-*.db")) {
            for (Path x : s) paths.add(x);
        }
        paths.sort(Comparator.comparingLong(LsmStorageEngine::fileNumber));
        for (Path p : paths) {
            SSTable t = new SSTable(p);
            tables.add(t);
            seq.set(Math.max(seq.get(), t.maxSeq()));
            nextFileNo = Math.max(nextFileNo, fileNumber(p) + 1);
        }
    }

    private static long fileNumber(Path p) {
        String name = p.getFileName().toString(); // sst-00000000000000000042.db
        return Long.parseLong(name.substring(4, name.length() - 3));
    }

    private void recover() throws IOException {
        for (WriteAheadLog.Record r : wal.replay()) {
            seq.set(Math.max(seq.get(), r.seq()));
            mem.put(r.key(), new SSTable.Entry(r.seq(), r.op() == WriteAheadLog.Op.DELETE, r.key(), r.value()));
        }
    }

    /** Graceful shutdown: flush the memtable, then close the WAL. */
    @Override
    public synchronized void close() {
        try {
            flush();
            wal.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Simulated crash: close file handles WITHOUT flushing (recovery must use the WAL). */
    public synchronized void abandon() {
        try {
            wal.close();
        } catch (IOException ignored) {
        }
    }
}
