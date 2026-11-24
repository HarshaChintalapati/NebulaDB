package dev.nebuladb.net;

import dev.nebuladb.raft.LogEntry;
import dev.nebuladb.raft.Messages;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary encoding of the RPC messages (big-endian, via DataOutputStream).
 * Hand-written to keep the project dependency-free; Protocol Buffers would
 * replace this class if the transport moved to gRPC.
 */
public final class Codec {
    private Codec() {}

    public record KvRequest(int shard, byte[] command) {}

    public enum KvStatus { OK, NOT_LEADER, ERROR }

    /** value: result of a GET (null if absent). info: leader hint or error message. */
    public record KvResponse(KvStatus status, String value, String info) {}

    // ---- vote
    public static byte[] encode(Messages.VoteRequest m) {
        return write(out -> {
            out.writeInt(m.group());
            out.writeLong(m.term());
            str(out, m.candidateId());
            out.writeLong(m.lastLogIndex());
            out.writeLong(m.lastLogTerm());
        });
    }

    public static Messages.VoteRequest decodeVoteRequest(byte[] b) {
        return read(b, in -> new Messages.VoteRequest(in.readInt(), in.readLong(), str(in), in.readLong(), in.readLong()));
    }

    public static byte[] encode(Messages.VoteResponse m) {
        return write(out -> {
            out.writeLong(m.term());
            out.writeBoolean(m.granted());
        });
    }

    public static Messages.VoteResponse decodeVoteResponse(byte[] b) {
        return read(b, in -> new Messages.VoteResponse(in.readLong(), in.readBoolean()));
    }

    // ---- append
    public static byte[] encode(Messages.AppendRequest m) {
        return write(out -> {
            out.writeInt(m.group());
            out.writeLong(m.term());
            str(out, m.leaderId());
            out.writeLong(m.prevLogIndex());
            out.writeLong(m.prevLogTerm());
            out.writeInt(m.entries().size());
            for (LogEntry e : m.entries()) {
                out.writeLong(e.index());
                out.writeLong(e.term());
                out.writeInt(e.command().length);
                out.write(e.command());
            }
            out.writeLong(m.leaderCommit());
        });
    }

    public static Messages.AppendRequest decodeAppendRequest(byte[] b) {
        return read(b, in -> {
            int group = in.readInt();
            long term = in.readLong();
            String leader = str(in);
            long prev = in.readLong();
            long prevTerm = in.readLong();
            int n = in.readInt();
            List<LogEntry> entries = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                long idx = in.readLong();
                long t = in.readLong();
                entries.add(new LogEntry(idx, t, in.readNBytes(in.readInt())));
            }
            return new Messages.AppendRequest(group, term, leader, prev, prevTerm, entries, in.readLong());
        });
    }

    public static byte[] encode(Messages.AppendResponse m) {
        return write(out -> {
            out.writeLong(m.term());
            out.writeBoolean(m.success());
            out.writeLong(m.matchIndex());
        });
    }

    public static Messages.AppendResponse decodeAppendResponse(byte[] b) {
        return read(b, in -> new Messages.AppendResponse(in.readLong(), in.readBoolean(), in.readLong()));
    }

    // ---- client kv
    public static byte[] encode(KvRequest m) {
        return write(out -> {
            out.writeInt(m.shard());
            out.writeInt(m.command().length);
            out.write(m.command());
        });
    }

    public static KvRequest decodeKvRequest(byte[] b) {
        return read(b, in -> new KvRequest(in.readInt(), in.readNBytes(in.readInt())));
    }

    public static byte[] encode(KvResponse m) {
        return write(out -> {
            out.writeByte(m.status().ordinal());
            optStr(out, m.value());
            optStr(out, m.info());
        });
    }

    public static KvResponse decodeKvResponse(byte[] b) {
        return read(b, in -> new KvResponse(KvStatus.values()[in.readByte()], optStr(in), optStr(in)));
    }

    // ---- helpers
    private interface Writer { void write(DataOutputStream out) throws IOException; }

    private interface Reader<T> { T read(DataInputStream in) throws IOException; }

    private static byte[] write(Writer w) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            w.write(out);
            out.flush();
            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static <T> T read(byte[] b, Reader<T> r) {
        try {
            return r.read(new DataInputStream(new ByteArrayInputStream(b)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void str(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String str(DataInputStream in) throws IOException {
        return new String(in.readNBytes(in.readInt()), StandardCharsets.UTF_8);
    }

    private static void optStr(DataOutputStream out, String s) throws IOException {
        out.writeBoolean(s != null);
        if (s != null) str(out, s);
    }

    private static String optStr(DataInputStream in) throws IOException {
        return in.readBoolean() ? str(in) : null;
    }
}
