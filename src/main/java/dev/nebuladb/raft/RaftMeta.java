package dev.nebuladb.raft;

import java.io.*;
import java.nio.file.*;

/**
 * Durable currentTerm and votedFor. Raft requires these to be on disk BEFORE
 * replying to an RPC: if a node forgot its vote after a crash, it could vote
 * twice in the same term and two leaders could be elected.
 *
 * Written to a temp file, fsynced, then atomically renamed over the old file.
 */
final class RaftMeta {
    private final Path path;
    private long currentTerm;
    private String votedFor;

    RaftMeta(Path path) throws IOException {
        this.path = path;
        Files.createDirectories(path.getParent());
        if (Files.exists(path)) {
            try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
                currentTerm = in.readLong();
                String v = in.readUTF();
                votedFor = v.isEmpty() ? null : v;
            }
        }
    }

    long currentTerm() {
        return currentTerm;
    }

    String votedFor() {
        return votedFor;
    }

    void set(long term, String vote) throws IOException {
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp.toFile());
             DataOutputStream out = new DataOutputStream(fos)) {
            out.writeLong(term);
            out.writeUTF(vote == null ? "" : vote);
            out.flush();
            fos.getFD().sync();
        }
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        currentTerm = term;
        votedFor = vote;
    }
}
