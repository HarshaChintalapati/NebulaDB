package dev.nebuladb.storage;
import org.junit.jupiter.api.*; import org.junit.jupiter.api.io.TempDir; import java.nio.file.Path; import static org.junit.jupiter.api.Assertions.*;
class LsmStorageEngineTest{ @TempDir Path dir; @Test void persistsAndRecovers()throws Exception{ try(LsmStorageEngine s=new LsmStorageEngine(dir,1000)){s.put("users:42","Harsha");assertEquals("Harsha",s.get("users:42").orElseThrow());} try(LsmStorageEngine s=new LsmStorageEngine(dir,1000)){assertEquals("Harsha",s.get("users:42").orElseThrow());s.delete("users:42");assertTrue(s.get("users:42").isEmpty());} } }
