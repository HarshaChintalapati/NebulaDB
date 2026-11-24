package dev.nebuladb.raft;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

/**
 * Durable Raft log: one append-only file plus an in-memory copy of the entries.
 *
 * Record: [payload length:int][crc32:long][payload = index:long, term:long, cmdLen:int, cmd]
 *
 * - append() fsyncs before returning: an entry must be durable before the node
 *   acknowledges it, or a "committed" write could vanish after a crash.
 * - truncateFrom() cuts the file at the byte offset of the first removed entry
 *   (needed when a follower's uncommitted suffix conflicts with the leader).
 * - On open, a torn last record (crash mid-write) is detected via length/CRC and cut off.
 *
 * No snapshots yet, so the log grows without bound (see README "Limitations").
 */
final class RaftLog implements Closeable {
    private final RandomAccessFile file;
    private final boolean fsync;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Long> offsets = new ArrayList<>(); // byte offset of entry i+1

    RaftLog(Path path, boolean fsync) throws IOException {
        this.fsync = fsync;
        Files.createDirectories(path.getParent());
        this.file = new RandomAccessFile(path.toFile(), "rw");
        load();
    }

    private void load() throws IOException {
        long validEnd = 0;
        file.seek(0);
        while (file.getFilePointer() < file.length()) {
            long start = file.getFilePointer();
            try {
                int len = file.readInt();
                long expected = file.readLong();
                if (len < 20 || len > 256 * 1024 * 1024) break;
                byte[] payload = new byte[len];
                file.readFully(payload);
                CRC32 crc = new CRC32();
                crc.update(payload);
                if (crc.getValue() != expected) break;
                DataInputStream d = new DataInputStream(new ByteArrayInputStream(payload));
                long index = d.readLong();
                long term = d.readLong();
                byte[] cmd = d.readNBytes(d.readInt());
                if (index != entries.size() + 1) break;
                entries.add(new LogEntry(index, term, cmd));
                offsets.add(start);
                validEnd = file.getFilePointer();
            } catch (EOFException torn) {
                break;
            }
        }
        if (validEnd < file.length()) {
            file.setLength(validEnd); // drop the torn tail
        }
        file.seek(validEnd);
    }

    long lastIndex() {
        return entries.size();
    }

    long lastTerm() {
        return entries.isEmpty() ? 0 : entries.get(entries.size() - 1).term();
    }

    /** Term of the entry at index, 0 for index 0. Caller ensures 0 <= index <= lastIndex. */
    long termAt(long index) {
        return index == 0 ? 0 : entries.get((int) index - 1).term();
    }

    LogEntry get(long index) {
        return entries.get((int) index - 1);
    }

    /** Entries from `from` (inclusive), at most `max` of them. */
    List<LogEntry> slice(long from, int max) {
        if (from > lastIndex()) return List.of();
        int start = (int) from - 1;
        int end = (int) Math.min(entries.size(), start + (long) max);
        return new ArrayList<>(entries.subList(start, end));
    }

    /** First index whose term equals the term at `index` (used to skip back a whole term at once). */
    long firstIndexOfTerm(long index) {
        long term = termAt(index);
        long i = index;
        while (i > 1 && termAt(i - 1) == term) i--;
        return i;
    }

    void append(List<LogEntry> batch) throws IOException {
        if (batch.isEmpty()) return;
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        long offset = file.getFilePointer();
        List<Long> newOffsets = new ArrayList<>();
        for (LogEntry e : batch) {
            if (e.index() != entries.size() + newOffsets.size() + 1) {
                throw new IllegalStateException("non-contiguous append at " + e.index());
            }
            ByteArrayOutputStream p = new ByteArrayOutputStream();
            DataOutputStream d = new DataOutputStream(p);
            d.writeLong(e.index());
            d.writeLong(e.term());
            d.writeInt(e.command().length);
            d.write(e.command());
            byte[] payload = p.toByteArray();
            CRC32 crc = new CRC32();
            crc.update(payload);
            DataOutputStream out = new DataOutputStream(all);
            newOffsets.add(offset + all.size());
            out.writeInt(payload.length);
            out.writeLong(crc.getValue());
            out.write(payload);
        }
        file.write(all.toByteArray());
        if (fsync) file.getFD().sync();
        entries.addAll(batch);
        offsets.addAll(newOffsets);
    }

    /** Removes entries with index >= from. */
    void truncateFrom(long from) throws IOException {
        if (from > lastIndex()) return;
        long cut = offsets.get((int) from - 1);
        file.setLength(cut);
        file.seek(cut);
        if (fsync) file.getFD().sync();
        while (entries.size() >= from) {
            entries.remove(entries.size() - 1);
            offsets.remove(offsets.size() - 1);
        }
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
