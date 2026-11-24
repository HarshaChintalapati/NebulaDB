# NebulaDB

A sharded, Raft-replicated key-value database with a small SQL layer, written in Java 21. The storage engine and the Raft implementation are written from scratch on the plain JDK. Nodes talk over **gRPC** (Protocol Buffers over HTTP/2) by default, with a dependency-free HTTP/1.1 transport as an alternative.

- **Storage:** LSM-tree engine per replica, with a write-ahead log, sorted memtable, SSTables (Bloom filter and sparse index), compaction, and crash recovery from the WAL
- **Replication:** Raft per shard: leader election, log replication, majority commit, durable term, vote and log, conflict truncation, and leader no-op on election
- **Sharding:** hash partitioning; each shard is its own Raft group of `rf` replicas spread across nodes
- **RPC:** gRPC service defined in [`src/main/proto/nebula.proto`](src/main/proto/nebula.proto) (`RequestVote`, `AppendEntries`, `Kv`), behind a `RaftTransport` interface. A JDK-only HTTP/1.1 transport implements the same interface (`--transport http`)
- **Client:** routes keys to shards, finds leaders, follows `NOT_LEADER` hints, and retries through elections
- **Fault testing:** a chaos runner crashes shard leaders under concurrent writes and verifies that no acknowledged write is lost and that replicas converge

## Architecture

```text
 client / SQL shell
      | key -> shard (hash)          NebulaClient: find leader, retry on NOT_LEADER / failure
      v
 +-----------+   +-----------+   +-----------+   +-----------+   +-----------+
 |  node n1  |   |  node n2  |   |  node n3  |   |  node n4  |   |  node n5  |
 | shard 0 L |   | shard 0 F |   | shard 0 F |   | shard 1 F |   |   ...     |
 | shard 4 F |   | shard 1 L |   | shard 1 F |   | shard 2 F |   |           |
 |   ...     |   |   ...     |   |   ...     |   |   ...     |   |           |
 +-----------+   +-----------+   +-----------+   +-----------+   +-----------+
   each replica:  RaftNode (raft.log + raft.meta, fsynced)
                     | committed entries, in order
                     v
                  KvStateMachine -> LsmStorageEngine (WAL -> MemTable -> SSTables -> compaction)
```

Placement: shard `s` lives on nodes `s, s+1, ..., s+rf-1` (mod N). With 5 nodes, 10 shards and rf=3, each node holds 6 replicas, and any single node can fail without any shard losing its majority.

### Write path

1. The client hashes the key to a shard and sends the command to that shard's leader.
2. The leader appends to its Raft log (fsync) and sends `AppendEntries` to its followers (each fsyncs).
3. Once a majority has the entry, the leader advances `commitIndex`.
4. The applier thread applies the entry to the LSM engine, and only then does the client get success. That success is what "acknowledged" means below.

Reads also go through the log, so they are linearizable (simple, but it costs a replication round trip).

## Correctness notes

- **Election restriction:** a node votes only for candidates whose log is at least as up to date as its own, so a new leader always holds every committed entry.
- **Commit rule:** a leader only counts replicas for entries from its own term (Raft paper §5.4.2). It appends a no-op on election so that earlier entries commit through it.
- **Durability:** `currentTerm` and `votedFor` are written to a temp file, fsynced, and atomically renamed before any reply. Log entries are fsynced before they are acknowledged.
- **Torn writes:** every Raft log and WAL record carries a length and a CRC32. On restart, a half-written tail is detected and cut off.
- **State machine recovery:** the applied index is stored in the LSM engine itself. The engine's own WAL is not fsynced per write, because the Raft log is the source of truth. A crash can only lose a suffix of the WAL (data and applied markers together), and those entries are simply re-applied. PUT and DELETE are idempotent.
- **Storage fixes made along the way:** SSTables are fsynced before the WAL is truncated; SSTable files are numbered so restart order equals age order; compaction keeps tombstones, so a crash mid-compaction cannot resurrect deleted keys.
- **RPC latency (HTTP transport):** the HTTP server sets `TCP_NODELAY`. Without it, every small RPC hit the ~40 ms delayed-ACK stall, which made writes about 10x slower.
- **gRPC reconnects:** after a peer crashes, gRPC backs off reconnect attempts exponentially (up to minutes). Raft already retries on every heartbeat, so the transport resets that backoff, and a restarted node becomes reachable again right away.
- **Blocking handlers:** a client write blocks until Raft commits, so gRPC handlers run on a dedicated thread pool, not on gRPC's network threads.

## Build and test

```bash
mvn clean test package
```

Tests cover the LSM engine (WAL recovery, compaction order, tombstones), the Raft log (truncation, torn tails), Raft itself over an in-memory network (single leader, replication, leader crash and restart, an isolated leader that cannot commit and whose entry is discarded), and the cluster and a short chaos run over **both** gRPC and HTTP.

The build generates the gRPC stubs from `nebula.proto` (protobuf-maven-plugin) and produces one runnable jar.

## Run the failure test

```bash
java -jar target/nebuladb-0.3.0.jar chaos --failures 100
# defaults: --nodes 5 --shards 10 --rf 3 --writers 8 --transport grpc
```

This starts a 5-node cluster in one JVM, with each node on its own port and data directory and nodes talking over gRPC. Eight writers insert unique keys continuously. Each round crashes the current leader of a shard (no flush), measures the time until a new leader is elected, keeps writing while the node is down, and then restarts the node from disk. At the end it checks that every acknowledged write reads back with its exact value, and that every replica of every shard holds the same data. It prints a summary ending in `RESULT: PASS` or `FAIL`.

## Benchmark

```bash
java -jar target/nebuladb-0.3.0.jar bench --rows 1000000 --threads 32
# or against a running cluster: --cluster n1=host:7001,...
```

It reports throughput, p50/p95/p99 write latency, rows per shard, and a read-back check of 1,000 random rows. Every write waits for fsynced log appends on a majority, so results depend heavily on the disk. Report them together with your hardware.

## Results

Fill this in from your own runs, on your own machine.

| Run | Setup | Result |
|---|---|---|
| Chaos | gRPC, 5 nodes, 10 shards, rf=3, 100 leader failures | _acked writes / lost writes / failover p50, p95_ |
| Bench | gRPC, 5 nodes, 10 shards, rf=3, N rows, 32 threads | _writes/s, p50/p95/p99_ |

## Run a real cluster

```bash
docker compose up --build              # 5 nodes, 10 shards, rf=3
docker compose run --rm shell          # SQL shell against the cluster
docker compose kill n1                 # watch shards fail over; then: docker compose start n1
```

Or by hand:

```bash
java -jar target/nebuladb-0.3.0.jar server --id n1 --cluster n1=localhost:7001,n2=localhost:7002,n3=localhost:7003 --shards 4 --rf 3
# ...same for n2 and n3, then:
java -jar target/nebuladb-0.3.0.jar shell --cluster n1=localhost:7001,n2=localhost:7002,n3=localhost:7003 --shards 4 --rf 3
```

```sql
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);
INSERT INTO users VALUES (42, 'Harsha');
SELECT * FROM users WHERE id = 42;
UPDATE users SET name = 'Sai' WHERE id = 42;
DELETE FROM users WHERE id = 42;
```

Single process, without replication: `java -jar target/nebuladb-0.3.0.jar local`

## Code map

```text
storage/   LsmStorageEngine, WriteAheadLog, SSTable, BloomFilter
raft/      RaftNode (election, replication, commit, apply), RaftLog, RaftMeta, Messages, RaftTransport
kv/        Command encoding, KvStateMachine (Raft -> LSM), LocalShardedStore
net/       Transports (pick gRPC or HTTP), NodeTransport / RpcEndpoint interfaces
net/grpc/  GrpcTransport (channels, deadlines), GrpcRpcServer, ProtoMapper (records <-> protobuf)
net/       HttpTransport + RpcServer + Codec (JDK-only alternative)
proto/     nebula.proto (service and message definitions)
cluster/   ClusterConfig (placement), NebulaNode (one server), NebulaClient (routing and retries), LocalCluster
sql/ query/ minimal SQL parser; rows stored as table:pk, catalog as __catalog:table
chaos/     LeaderFailoverChaos
bench/     LoadBench
```

## Limitations and next steps

- **No snapshots or log compaction:** the Raft log grows forever, and restarted nodes replay from the start.
- **Static membership:** nodes can't be added or removed while running (Raft joint consensus isn't implemented).
- **No rebalancing:** shard placement is fixed by the config.
- **Reads go through the log:** ReadIndex or leader leases would make them cheaper.
- **One fsync per proposal:** batching proposals (group commit) would raise throughput a lot.
- **Single-key operations only:** no transactions, no MVCC, no range scans or secondary indexes, and the SQL is a tiny subset.
- **Unary RPCs only:** a streaming `AppendEntries` would cut per-batch overhead for followers that are far behind.
