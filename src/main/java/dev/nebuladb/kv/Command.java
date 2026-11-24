package dev.nebuladb.kv;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * A key-value command as stored in the Raft log.
 * Encoding: [op:byte][keyLen:int][key][hasValue:byte][valueLen:int][value]
 * An empty byte array is the Raft leader's no-op.
 */
public record Command(Op op, String key, String value) {
    public enum Op { PUT, DELETE, GET }

    public static Command put(String key, String value) {
        return new Command(Op.PUT, key, value);
    }

    public static Command delete(String key) {
        return new Command(Op.DELETE, key, null);
    }

    public static Command get(String key) {
        return new Command(Op.GET, key, null);
    }

    public byte[] encode() {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(buf);
            out.writeByte(op.ordinal());
            writeString(out, key);
            out.writeBoolean(value != null);
            if (value != null) writeString(out, value);
            return buf.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Returns null for the no-op (empty) command. */
    public static Command decode(byte[] bytes) {
        if (bytes.length == 0) return null;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            Op op = Op.values()[in.readByte()];
            String key = readString(in);
            String value = in.readBoolean() ? readString(in) : null;
            return new Command(op, key, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void writeString(DataOutputStream out, String s) throws IOException {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    static String readString(DataInputStream in) throws IOException {
        return new String(in.readNBytes(in.readInt()), StandardCharsets.UTF_8);
    }
}
