# NebulaDB

NebulaDB is an educational distributed SQL database MVP in Java 21.

Implemented in this repository:
- restricted SQL: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
- hash-based sharding
- educational Raft core: follower/candidate/leader, RequestVote, AppendEntries, quorum commit
- LSM-style local storage: WAL, sorted MemTable, SSTables, Bloom filters, compaction
- crash recovery from WAL
- tests and Dockerfile

## Architecture

```text
Client -> SQL Parser -> Query Coordinator -> Shard Router
                                      -> Raft Group -> Storage Engine
                                                       |
                                            WAL -> MemTable -> SSTables -> Compaction
```

## Scope

This is intentionally an interview/learning implementation, not production infrastructure. It does not yet include multi-process gRPC transport, Raft snapshots, membership changes, MVCC, or multi-shard transactions. `docs/ROADMAP.md` explains how to extend it.

## Build

```bash
mvn clean test package
java -jar target/nebuladb-0.1.0.jar
```

## Demo

```text
CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR);
INSERT INTO users VALUES (42, 'Harsha');
SELECT * FROM users WHERE id = 42;
UPDATE users SET name = 'Sai' WHERE id = 42;
DELETE FROM users WHERE id = 42;
exit
```
