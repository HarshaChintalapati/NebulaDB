package dev.nebuladb.chaos;

import dev.nebuladb.net.Transports;
import dev.nebuladb.raft.RaftTiming;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

/**
 * A short version of the chaos run for CI, once per transport. The full run
 * (100 leader failures, 5 nodes, 10 shards) is:
 *   java -jar target/nebuladb-0.3.0.jar chaos --failures 100
 */
class LeaderFailoverChaosTest {
    @TempDir
    Path dir;

    private void runAndCheck(Transports.Kind transport) throws Exception {
        LeaderFailoverChaos.Report r = LeaderFailoverChaos.run(dir.resolve(transport.name()), 3, 3, 3, 6, 4,
                RaftTiming.fast(), true, transport, null);
        System.out.println(transport + "\n" + r.summary());
        assertEquals(6, r.failuresInjected());
        assertTrue(r.ackedWrites() > 0);
        assertEquals(0, r.lostWrites());
        assertEquals(0, r.divergentReplicaReads());
    }

    @Test
    void noAcknowledgedWriteIsLostOverGrpc() throws Exception {
        runAndCheck(Transports.Kind.GRPC);
    }

    @Test
    void noAcknowledgedWriteIsLostOverHttp() throws Exception {
        runAndCheck(Transports.Kind.HTTP);
    }
}
