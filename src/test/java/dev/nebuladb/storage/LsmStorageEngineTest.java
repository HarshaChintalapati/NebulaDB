package dev.nebuladb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class LsmStorageEngineTest {
    @TempDir
    Path dir;

    @Test
    void persistsAndRecovers() throws Exception {
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 1000)) {
            s.put("users:42", "Harsha");
            assertEquals("Harsha", s.get("users:42").orElseThrow());
        }
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 1000)) {
            assertEquals("Harsha", s.get("users:42").orElseThrow());
            s.delete("users:42");
            assertTrue(s.get("users:42").isEmpty());
        }
    }

    @Test
    void recoversUnflushedWritesFromWalAfterCrash() throws Exception {
        LsmStorageEngine s = new LsmStorageEngine(dir, 1000);
        s.put("a", "1");
        s.put("b", "2");
        s.delete("a");
        s.abandon(); // crash: memtable never flushed
        try (LsmStorageEngine r = new LsmStorageEngine(dir, 1000)) {
            assertTrue(r.get("a").isEmpty());
            assertEquals("2", r.get("b").orElseThrow());
        }
    }

    @Test
    void compactionKeepsNewestVersionAcrossRestarts() throws Exception {
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 10)) {
            for (int round = 0; round < 6; round++) {
                for (int i = 0; i < 10; i++) s.put("k" + i, "v" + round);
            }
            s.delete("k3");
            assertTrue(s.sstableCount() < 4, "compaction should have merged tables");
        }
        // After compaction, new flushes must still be treated as NEWER than the merged table.
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 10)) {
            for (int i = 0; i < 10; i++) s.put("k" + i, "final");
        }
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 10)) {
            for (int i = 0; i < 10; i++) assertEquals("final", s.get("k" + i).orElseThrow());
        }
    }

    @Test
    void tombstonesSurviveCompaction() throws Exception {
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 5)) {
            for (int i = 0; i < 5; i++) s.put("x" + i, "old");
            for (int i = 0; i < 5; i++) s.delete("x" + i);
            for (int r = 0; r < 4; r++) for (int i = 0; i < 5; i++) s.put("y" + r + i, "z");
        }
        try (LsmStorageEngine s = new LsmStorageEngine(dir, 5)) {
            for (int i = 0; i < 5; i++) assertTrue(s.get("x" + i).isEmpty());
        }
    }
}
