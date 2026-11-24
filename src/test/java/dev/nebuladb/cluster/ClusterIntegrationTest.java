package dev.nebuladb.cluster;

import dev.nebuladb.query.QueryCoordinator;
import dev.nebuladb.raft.RaftTiming;
import dev.nebuladb.sql.SimpleSqlParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/** Real nodes talking over HTTP on localhost. */
class ClusterIntegrationTest {
    @TempDir
    Path dir;

    private LocalCluster cluster;

    @AfterEach
    void tearDown() {
        if (cluster != null) cluster.close();
    }

    private NebulaClient startCluster(int nodes, int shards) throws Exception {
        cluster = new LocalCluster(nodes, shards, 3, dir, RaftTiming.fast(), true);
        cluster.startAll();
        for (int s = 0; s < shards; s++) assertTrue(cluster.awaitLeader(s, null, 5000).isPresent());
        return new NebulaClient(cluster.config());
    }

    @Test
    void placesEachShardOnRfNodes() {
        ClusterConfig c = ClusterConfig.parse("n1=h:1,n2=h:2,n3=h:3,n4=h:4,n5=h:5", 10, 3);
        for (int s = 0; s < 10; s++) assertEquals(3, c.replicas(s).size());
        for (String n : c.nodeIds()) assertEquals(6, c.shardsOn(n).size());
    }

    @Test
    void putGetDeleteAcrossShards() throws Exception {
        try (NebulaClient client = startCluster(3, 4)) {
            for (int i = 0; i < 100; i++) client.put("key" + i, "val" + i);
            for (int i = 0; i < 100; i++) assertEquals("val" + i, client.get("key" + i).orElseThrow());
            client.delete("key5");
            assertTrue(client.get("key5").isEmpty());
        }
    }

    @Test
    void sqlOverReplicatedCluster() throws Exception {
        try (NebulaClient client = startCluster(3, 2)) {
            QueryCoordinator q = new QueryCoordinator(client);
            SimpleSqlParser p = new SimpleSqlParser();
            assertEquals("table created", q.execute(p.parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);")));
            q.execute(p.parse("INSERT INTO users VALUES (42, 'Harsha');"));
            assertEquals("42,Harsha", q.execute(p.parse("SELECT * FROM users WHERE id = 42;")));
            q.execute(p.parse("UPDATE users SET name = 'Sai' WHERE id = 42;"));
            assertEquals("42,Sai", q.execute(p.parse("SELECT * FROM users WHERE id = 42;")));
            q.execute(p.parse("DELETE FROM users WHERE id = 42;"));
            assertEquals("(0 rows)", q.execute(p.parse("SELECT * FROM users WHERE id = 42;")));
            assertThrows(IllegalStateException.class, () -> q.execute(p.parse("SELECT * FROM nope WHERE id = 1;")));
        }
    }

    @Test
    void writesSurviveLeaderNodeCrashAndRestart() throws Exception {
        try (NebulaClient client = startCluster(3, 1)) {
            for (int i = 0; i < 50; i++) client.put("a" + i, "x" + i);
            String leader = cluster.leaderOf(0).orElseThrow();
            cluster.crash(leader);
            assertTrue(cluster.awaitLeader(0, leader, 5000).isPresent());
            for (int i = 0; i < 50; i++) client.put("b" + i, "y" + i); // still available with 2 of 3
            cluster.start(leader);
            for (int i = 0; i < 50; i++) {
                assertEquals("x" + i, client.get("a" + i).orElseThrow());
                assertEquals("y" + i, client.get("b" + i).orElseThrow());
            }
        }
    }
}
