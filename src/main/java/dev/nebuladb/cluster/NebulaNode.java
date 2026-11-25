package dev.nebuladb.cluster;

import dev.nebuladb.kv.KvStateMachine;
import dev.nebuladb.net.Codec;
import dev.nebuladb.net.NodeTransport;
import dev.nebuladb.net.RpcEndpoint;
import dev.nebuladb.net.RpcServer;
import dev.nebuladb.net.Transports;
import dev.nebuladb.raft.*;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * One database server. It hosts one Raft replica for every shard placed on it
 * (see ClusterConfig) and serves Raft RPCs and client requests over gRPC (or
 * plain HTTP; see Transports).
 *
 * Client requests for shard s are proposed to this node's Raft replica of s. If it
 * is not the leader, the client gets NOT_LEADER plus a hint and retries there.
 */
public final class NebulaNode implements Closeable, RpcServer.Handler {
    private final String id;
    private final ClusterConfig config;
    private final Path dataDir;
    private final RaftTiming timing;
    private final boolean fsync;
    private final Transports.Kind transportKind;
    private final Map<Integer, RaftNode> groups = new ConcurrentHashMap<>();
    private final Map<Integer, KvStateMachine> stateMachines = new ConcurrentHashMap<>();
    private NodeTransport transport;
    private RpcEndpoint server;

    public NebulaNode(String id, ClusterConfig config, Path dataDir, RaftTiming timing, boolean fsync,
                      Transports.Kind transportKind) {
        if (!config.nodes().containsKey(id)) throw new IllegalArgumentException("node " + id + " not in config");
        this.id = id;
        this.config = config;
        this.dataDir = dataDir;
        this.timing = timing;
        this.fsync = fsync;
        this.transportKind = transportKind;
    }

    public synchronized void start() throws IOException {
        transport = Transports.client(transportKind, config.nodes(), Duration.ofMillis(Math.max(200, timing.electionMinMs())));
        for (int shard : config.shardsOn(id)) {
            Path dir = dataDir.resolve("shard-" + shard);
            KvStateMachine sm = new KvStateMachine(dir.resolve("state"));
            List<String> peers = new ArrayList<>(config.replicas(shard));
            peers.remove(id);
            RaftNode node = new RaftNode(id, shard, peers, transport, dir.resolve("raft"), sm, timing, fsync);
            stateMachines.put(shard, sm);
            groups.put(shard, node);
        }
        String addr = config.nodes().get(id);
        int port = Integer.parseInt(addr.substring(addr.lastIndexOf(':') + 1));
        server = Transports.server(transportKind, "0.0.0.0", port, this);
        groups.values().forEach(RaftNode::start);
        server.start();
    }

    /** Graceful stop. */
    @Override
    public void close() {
        stop(false);
    }

    /** Simulated process crash: no flushing; recovery must come from the logs. */
    public void crash() {
        stop(true);
    }

    private synchronized void stop(boolean crash) {
        if (server != null) server.stop();
        for (RaftNode n : groups.values()) {
            if (crash) n.crash();
            else n.close();
        }
        groups.clear();
        stateMachines.clear();
        if (transport != null) transport.close();
        server = null;
    }

    public String id() {
        return id;
    }

    public Optional<RaftNode> group(int shard) {
        return Optional.ofNullable(groups.get(shard));
    }

    public Optional<KvStateMachine> stateMachine(int shard) {
        return Optional.ofNullable(stateMachines.get(shard));
    }

    // ---------------------------------------------------------------- RPC handlers

    @Override
    public Messages.VoteResponse vote(Messages.VoteRequest request) {
        return requireGroup(request.group()).handleVote(request);
    }

    @Override
    public Messages.AppendResponse append(Messages.AppendRequest request) {
        return requireGroup(request.group()).handleAppend(request);
    }

    @Override
    public Codec.KvResponse kv(Codec.KvRequest request) {
        RaftNode node = groups.get(request.shard());
        if (node == null) return new Codec.KvResponse(Codec.KvStatus.ERROR, null, "no replica of shard " + request.shard());
        try {
            String value = node.propose(request.command()).get(5, TimeUnit.SECONDS);
            return new Codec.KvResponse(Codec.KvStatus.OK, value, null);
        } catch (NotLeaderException e) {
            return new Codec.KvResponse(Codec.KvStatus.NOT_LEADER, null, e.leaderHint());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof NotLeaderException nl) {
                return new Codec.KvResponse(Codec.KvStatus.NOT_LEADER, null, nl.leaderHint());
            }
            return new Codec.KvResponse(Codec.KvStatus.ERROR, null, String.valueOf(e.getCause()));
        } catch (TimeoutException e) {
            return new Codec.KvResponse(Codec.KvStatus.ERROR, null, "timed out waiting for commit");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new Codec.KvResponse(Codec.KvStatus.ERROR, null, "interrupted");
        }
    }

    private RaftNode requireGroup(int shard) {
        RaftNode n = groups.get(shard);
        if (n == null) throw new IllegalStateException("no replica of shard " + shard + " on " + id);
        return n;
    }
}
