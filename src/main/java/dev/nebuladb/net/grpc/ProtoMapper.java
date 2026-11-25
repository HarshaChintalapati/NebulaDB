package dev.nebuladb.net.grpc;

import com.google.protobuf.ByteString;
import dev.nebuladb.net.Codec;
import dev.nebuladb.net.grpc.proto.*;
import dev.nebuladb.raft.LogEntry;
import dev.nebuladb.raft.Messages;

import java.util.ArrayList;
import java.util.List;

/** Converts between the internal message records and the generated protobuf classes. */
final class ProtoMapper {
    private ProtoMapper() {}

    // ---- vote
    static VoteRequest toProto(Messages.VoteRequest m) {
        return VoteRequest.newBuilder()
                .setGroup(m.group())
                .setTerm(m.term())
                .setCandidateId(m.candidateId())
                .setLastLogIndex(m.lastLogIndex())
                .setLastLogTerm(m.lastLogTerm())
                .build();
    }

    static Messages.VoteRequest fromProto(VoteRequest p) {
        return new Messages.VoteRequest(p.getGroup(), p.getTerm(), p.getCandidateId(),
                p.getLastLogIndex(), p.getLastLogTerm());
    }

    static VoteResponse toProto(Messages.VoteResponse m) {
        return VoteResponse.newBuilder().setTerm(m.term()).setGranted(m.granted()).build();
    }

    static Messages.VoteResponse fromProto(VoteResponse p) {
        return new Messages.VoteResponse(p.getTerm(), p.getGranted());
    }

    // ---- append
    static AppendRequest toProto(Messages.AppendRequest m) {
        AppendRequest.Builder b = AppendRequest.newBuilder()
                .setGroup(m.group())
                .setTerm(m.term())
                .setLeaderId(m.leaderId())
                .setPrevLogIndex(m.prevLogIndex())
                .setPrevLogTerm(m.prevLogTerm())
                .setLeaderCommit(m.leaderCommit());
        for (LogEntry e : m.entries()) {
            b.addEntries(Entry.newBuilder()
                    .setIndex(e.index())
                    .setTerm(e.term())
                    .setCommand(ByteString.copyFrom(e.command())));
        }
        return b.build();
    }

    static Messages.AppendRequest fromProto(AppendRequest p) {
        List<LogEntry> entries = new ArrayList<>(p.getEntriesCount());
        for (Entry e : p.getEntriesList()) {
            entries.add(new LogEntry(e.getIndex(), e.getTerm(), e.getCommand().toByteArray()));
        }
        return new Messages.AppendRequest(p.getGroup(), p.getTerm(), p.getLeaderId(),
                p.getPrevLogIndex(), p.getPrevLogTerm(), entries, p.getLeaderCommit());
    }

    static AppendResponse toProto(Messages.AppendResponse m) {
        return AppendResponse.newBuilder()
                .setTerm(m.term())
                .setSuccess(m.success())
                .setMatchIndex(m.matchIndex())
                .build();
    }

    static Messages.AppendResponse fromProto(AppendResponse p) {
        return new Messages.AppendResponse(p.getTerm(), p.getSuccess(), p.getMatchIndex());
    }

    // ---- client kv
    static KvRequest toProto(Codec.KvRequest m) {
        return KvRequest.newBuilder().setShard(m.shard()).setCommand(ByteString.copyFrom(m.command())).build();
    }

    static Codec.KvRequest fromProto(KvRequest p) {
        return new Codec.KvRequest(p.getShard(), p.getCommand().toByteArray());
    }

    static KvResponse toProto(Codec.KvResponse m) {
        KvResponse.Builder b = KvResponse.newBuilder().setStatus(switch (m.status()) {
            case OK -> KvStatus.OK;
            case NOT_LEADER -> KvStatus.NOT_LEADER;
            case ERROR -> KvStatus.ERROR;
        });
        if (m.value() != null) b.setHasValue(true).setValue(m.value());
        if (m.info() != null) b.setInfo(m.info());
        return b.build();
    }

    static Codec.KvResponse fromProto(KvResponse p) {
        Codec.KvStatus status = switch (p.getStatus()) {
            case OK -> Codec.KvStatus.OK;
            case NOT_LEADER -> Codec.KvStatus.NOT_LEADER;
            default -> Codec.KvStatus.ERROR;
        };
        return new Codec.KvResponse(status, p.getHasValue() ? p.getValue() : null,
                p.getInfo().isEmpty() ? null : p.getInfo());
    }
}
