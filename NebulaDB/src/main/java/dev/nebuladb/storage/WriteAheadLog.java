package dev.nebuladb.storage;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;

public final class WriteAheadLog implements Closeable {
    public enum Op { PUT, DELETE }
    public record Record(long seq, Op op, String key, String value) {}
    private final Path path;
    private final RandomAccessFile file;

    public WriteAheadLog(Path path) throws IOException {
        this.path = path; Files.createDirectories(path.getParent());
        this.file = new RandomAccessFile(path.toFile(), "rw"); file.seek(file.length());
    }

    public synchronized void append(Record r) throws IOException {
        byte[] k = r.key().getBytes(StandardCharsets.UTF_8);
        byte[] v = r.value() == null ? new byte[0] : r.value().getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(buf)) {
            out.writeLong(r.seq()); out.writeByte(r.op().ordinal()); out.writeInt(k.length); out.writeInt(v.length); out.write(k); out.write(v);
        }
        byte[] payload = buf.toByteArray(); CRC32 crc = new CRC32(); crc.update(payload);
        file.writeInt(payload.length); file.writeLong(crc.getValue()); file.write(payload); file.getFD().sync();
    }

    public synchronized List<Record> replay() throws IOException {
        List<Record> out = new ArrayList<>();
        if (!Files.exists(path)) return out;
        try (RandomAccessFile in = new RandomAccessFile(path.toFile(), "r")) {
            while (in.getFilePointer() < in.length()) {
                try {
                    int len = in.readInt(); long expected = in.readLong();
                    if (len < 0 || len > 64 * 1024 * 1024) break;
                    byte[] payload = new byte[len]; in.readFully(payload);
                    CRC32 crc = new CRC32(); crc.update(payload); if (crc.getValue() != expected) break;
                    try (DataInputStream d = new DataInputStream(new ByteArrayInputStream(payload))) {
                        long seq = d.readLong(); Op op = Op.values()[d.readByte()]; int kl = d.readInt(); int vl = d.readInt();
                        byte[] k = d.readNBytes(kl); byte[] v = d.readNBytes(vl);
                        out.add(new Record(seq, op, new String(k, StandardCharsets.UTF_8), op == Op.DELETE ? null : new String(v, StandardCharsets.UTF_8)));
                    }
                } catch (EOFException partialTail) { break; }
            }
        }
        return out;
    }

    public synchronized void reset() throws IOException { file.setLength(0); file.seek(0); file.getFD().sync(); }
    public synchronized void close() throws IOException { file.close(); }
}
