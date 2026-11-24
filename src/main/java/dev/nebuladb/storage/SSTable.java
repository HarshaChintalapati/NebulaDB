package dev.nebuladb.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

/**
 * Immutable sorted file of entries. On open we rebuild an in-memory Bloom filter
 * (skip files that cannot contain a key) and a sparse index (every 32nd key ->
 * file offset), so a lookup seeks near the key and scans at most 32 entries.
 */
public final class SSTable {
    public record Entry(long seq, boolean tombstone, String key, String value) {}

    private final Path path;
    private final BloomFilter bloom = new BloomFilter(1 << 18, 4);
    private final NavigableMap<String, Long> index = new TreeMap<>();
    private long maxSeq;

    public SSTable(Path path) throws IOException {
        this.path = path;
        buildIndex();
    }

    public Path path() {
        return path;
    }

    /** Highest sequence number in the file (used to restore the engine's counter on restart). */
    public long maxSeq() {
        return maxSeq;
    }

    /** Writes entries (already sorted by key) and fsyncs before returning. */
    public static SSTable write(Path p, Collection<Entry> entries) throws IOException {
        Files.createDirectories(p.getParent());
        try (FileOutputStream fos = new FileOutputStream(p.toFile());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos))) {
            for (Entry e : entries) {
                byte[] k = e.key().getBytes(StandardCharsets.UTF_8);
                byte[] v = e.value() == null ? new byte[0] : e.value().getBytes(StandardCharsets.UTF_8);
                out.writeLong(e.seq());
                out.writeBoolean(e.tombstone());
                out.writeInt(k.length);
                out.writeInt(v.length);
                out.write(k);
                out.write(v);
            }
            out.flush();
            // Durable BEFORE the caller truncates the WAL; otherwise a crash could lose data.
            fos.getFD().sync();
        }
        return new SSTable(p);
    }

    private void buildIndex() throws IOException {
        try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "r")) {
            int i = 0;
            while (f.getFilePointer() < f.length()) {
                long off = f.getFilePointer();
                Entry e = read(f);
                bloom.add(e.key());
                maxSeq = Math.max(maxSeq, e.seq());
                if (i++ % 32 == 0) index.put(e.key(), off);
            }
        }
    }

    public Optional<Entry> get(String key) throws IOException {
        if (!bloom.mightContain(key)) return Optional.empty();
        Map.Entry<String, Long> floor = index.floorEntry(key);
        long start = floor == null ? 0 : floor.getValue();
        try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "r")) {
            f.seek(start);
            while (f.getFilePointer() < f.length()) {
                Entry e = read(f);
                int c = e.key().compareTo(key);
                if (c == 0) return Optional.of(e);
                if (c > 0) return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public List<Entry> all() throws IOException {
        List<Entry> out = new ArrayList<>();
        try (RandomAccessFile f = new RandomAccessFile(path.toFile(), "r")) {
            while (f.getFilePointer() < f.length()) out.add(read(f));
        }
        return out;
    }

    private static Entry read(RandomAccessFile f) throws IOException {
        long seq = f.readLong();
        boolean tomb = f.readBoolean();
        int kl = f.readInt();
        int vl = f.readInt();
        byte[] k = new byte[kl];
        byte[] v = new byte[vl];
        f.readFully(k);
        f.readFully(v);
        return new Entry(seq, tomb, new String(k, StandardCharsets.UTF_8),
                tomb ? null : new String(v, StandardCharsets.UTF_8));
    }
}
