package dev.nebuladb.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RaftLogTest {
    @TempDir
    Path dir;

    private static LogEntry e(long i, long t) {
        return new LogEntry(i, t, ("cmd" + i).getBytes());
    }

    @Test
    void appendReopenAndTruncate() throws Exception {
        Path p = dir.resolve("raft.log");
        try (RaftLog log = new RaftLog(p, true)) {
            log.append(List.of(e(1, 1), e(2, 1), e(3, 2)));
            assertEquals(3, log.lastIndex());
            assertEquals(2, log.lastTerm());
        }
        try (RaftLog log = new RaftLog(p, true)) {
            assertEquals(3, log.lastIndex());
            assertEquals(1, log.termAt(2));
            log.truncateFrom(2);
            assertEquals(1, log.lastIndex());
            log.append(List.of(e(2, 3)));
        }
        try (RaftLog log = new RaftLog(p, true)) {
            assertEquals(2, log.lastIndex());
            assertEquals(3, log.termAt(2));
            assertEquals(1, log.firstIndexOfTerm(1));
        }
    }

    @Test
    void tornTailIsDroppedOnOpen() throws Exception {
        Path p = dir.resolve("raft.log");
        try (RaftLog log = new RaftLog(p, true)) {
            log.append(List.of(e(1, 1), e(2, 1)));
        }
        try (RandomAccessFile f = new RandomAccessFile(p.toFile(), "rw")) {
            f.setLength(f.length() - 3); // simulate a crash in the middle of writing entry 2
        }
        try (RaftLog log = new RaftLog(p, true)) {
            assertEquals(1, log.lastIndex());
            log.append(List.of(e(2, 2))); // and the log is still appendable
            assertEquals(2, log.termAt(2));
        }
    }

    @Test
    void rejectsNonContiguousAppend() throws Exception {
        try (RaftLog log = new RaftLog(dir.resolve("raft.log"), false)) {
            assertThrows(IllegalStateException.class, () -> log.append(List.of(e(2, 1))));
        }
    }
}
