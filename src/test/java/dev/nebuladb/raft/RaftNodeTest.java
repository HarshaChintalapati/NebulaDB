package dev.nebuladb.raft;

import dev.nebuladb.kv.Command;
import dev.nebuladb.kv.KvStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {
    @TempDir
    Path dir;

    private final InMemoryNetwork net = new InMemoryNetwork();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, KvStateMachine> sms = new HashMap<>();
    private final List<String> ids = List.of("a", "b", "c");

    private void start(String id) throws Exception {
        List<String> peers = new ArrayList<>(ids);
        peers.remove(id);
        KvStateMachine sm = new KvStateMachine(dir.resolve(id).resolve("state"));
        RaftNode n = new RaftNode(id, 0, peers, net, dir.resolve(id).resolve("raft"), sm, RaftTiming.fast(), true);
        nodes.put(id, n);
        sms.put(id, sm);
        net.register(id, n);
        n.start();
    }

    private void crash(String id) {
        net.unregister(id);
        nodes.remove(id).crash();
        sms.remove(id);
    }

    private String awaitLeader(String excluded) throws InterruptedException {
        long end = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < end) {
            for (RaftNode n : nodes.values()) {
                RaftNode.Status s = n.status();
                if (s.role() == RaftNode.Role.LEADER && !s.id().equals(excluded)) return s.id();
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no leader elected");
    }

    private void put(String leader, String k, String v) throws Exception {
        nodes.get(leader).propose(Command.put(k, v).encode()).get(3, TimeUnit.SECONDS);
    }

    private void awaitApplied(String id, String key, String value) throws Exception {
        long end = System.currentTimeMillis() + 5000;
        while (System.currentTimeMillis() < end) {
            if (sms.get(id).readLocal(key).map(value::equals).orElse(false)) return;
            Thread.sleep(10);
        }
        fail(id + " never applied " + key + "=" + value);
    }

    @AfterEach
    void tearDown() {
        for (RaftNode n : nodes.values()) n.close();
        net.shutdown();
    }

    @Test
    void electsExactlyOneLeader() throws Exception {
        for (String id : ids) start(id);
        String leader = awaitLeader(null);
        Thread.sleep(300); // several heartbeat intervals: leadership should be stable
        long leaders = nodes.values().stream().filter(RaftNode::isLeader).count();
        assertEquals(1, leaders);
        assertTrue(nodes.get(leader).isLeader());
    }

    @Test
    void replicatesCommittedWritesToEveryReplica() throws Exception {
        for (String id : ids) start(id);
        String leader = awaitLeader(null);
        for (int i = 0; i < 50; i++) put(leader, "k" + i, "v" + i);
        for (String id : ids) awaitApplied(id, "k49", "v49");
        for (String id : ids) assertEquals("v7", sms.get(id).readLocal("k7").orElseThrow());
    }

    @Test
    void followerRejectsClientWrites() throws Exception {
        for (String id : ids) start(id);
        String leader = awaitLeader(null);
        String follower = ids.stream().filter(i -> !i.equals(leader)).findFirst().orElseThrow();
        NotLeaderException e = assertThrows(NotLeaderException.class,
                () -> nodes.get(follower).propose(Command.put("x", "y").encode()));
        assertEquals(leader, e.leaderHint());
    }

    @Test
    void committedWritesSurviveLeaderCrashAndRestart() throws Exception {
        for (String id : ids) start(id);
        String oldLeader = awaitLeader(null);
        for (int i = 0; i < 20; i++) put(oldLeader, "k" + i, "v" + i);

        crash(oldLeader);
        String newLeader = awaitLeader(oldLeader);
        assertNotEquals(oldLeader, newLeader);
        for (int i = 20; i < 40; i++) put(newLeader, "k" + i, "v" + i);

        start(oldLeader); // recovers from its on-disk log and catches up
        awaitApplied(oldLeader, "k39", "v39");
        for (int i = 0; i < 40; i++) assertEquals("v" + i, sms.get(oldLeader).readLocal("k" + i).orElseThrow());
    }

    @Test
    void isolatedLeaderCannotCommitAndItsEntryIsDiscarded() throws Exception {
        for (String id : ids) start(id);
        String oldLeader = awaitLeader(null);
        put(oldLeader, "k", "before");

        net.isolate(oldLeader);
        var doomed = nodes.get(oldLeader).propose(Command.put("k", "lost").encode());
        assertThrows(TimeoutException.class, () -> doomed.get(500, TimeUnit.MILLISECONDS)); // no majority

        String newLeader = awaitLeader(oldLeader);
        put(newLeader, "k", "after");

        net.heal(); // old leader sees the higher term, steps down, and its uncommitted entry is overwritten
        for (String id : ids) awaitApplied(id, "k", "after");
        assertTrue(doomed.isCompletedExceptionally());
        long lastIndex = nodes.get(newLeader).status().lastLogIndex();
        long end = System.currentTimeMillis() + 3000;
        while (nodes.get(oldLeader).status().lastLogIndex() != lastIndex && System.currentTimeMillis() < end) Thread.sleep(10);
        assertEquals(lastIndex, nodes.get(oldLeader).status().lastLogIndex());
    }
}
