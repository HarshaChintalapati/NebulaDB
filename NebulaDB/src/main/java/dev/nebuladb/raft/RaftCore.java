package dev.nebuladb.raft;

import java.util.*;

public final class RaftCore {
    public enum Role { FOLLOWER, CANDIDATE, LEADER }
    public record LogEntry(long index,long term,String command){}
    public record VoteRequest(long term,String candidateId,long lastLogIndex,long lastLogTerm){}
    public record VoteResponse(long term,boolean granted){}
    public record AppendRequest(long term,String leaderId,long prevLogIndex,long prevLogTerm,List<LogEntry> entries,long leaderCommit){}
    public record AppendResponse(long term,boolean success,long matchIndex){}

    private final String nodeId; private Role role=Role.FOLLOWER; private long term; private String votedFor; private final List<LogEntry> log=new ArrayList<>(); private long commitIndex; private long lastApplied;
    public RaftCore(String nodeId){this.nodeId=nodeId;}
    public synchronized Role role(){return role;} public synchronized long term(){return term;} public synchronized long commitIndex(){return commitIndex;}
    public synchronized void startElection(){role=Role.CANDIDATE;term++;votedFor=nodeId;}
    public synchronized void becomeLeader(){role=Role.LEADER;}
    public synchronized VoteResponse onVote(VoteRequest r){ if(r.term()<term)return new VoteResponse(term,false); if(r.term()>term){term=r.term();role=Role.FOLLOWER;votedFor=null;} boolean can=votedFor==null||votedFor.equals(r.candidateId()); boolean up=r.lastLogTerm()>lastTerm()||(r.lastLogTerm()==lastTerm()&&r.lastLogIndex()>=lastIndex()); if(can&&up){votedFor=r.candidateId();return new VoteResponse(term,true);} return new VoteResponse(term,false); }
    public synchronized AppendResponse onAppend(AppendRequest r){ if(r.term()<term)return new AppendResponse(term,false,lastIndex()); term=r.term();role=Role.FOLLOWER; if(r.prevLogIndex()>0&&(r.prevLogIndex()>lastIndex()||termAt(r.prevLogIndex())!=r.prevLogTerm()))return new AppendResponse(term,false,lastIndex()); for(LogEntry e:r.entries()){int p=(int)e.index()-1;if(p<log.size()&&log.get(p).term()!=e.term())while(log.size()>p)log.remove(log.size()-1);if(p>=log.size())log.add(e);} commitIndex=Math.min(r.leaderCommit(),lastIndex()); return new AppendResponse(term,true,lastIndex()); }
    public synchronized LogEntry appendLeaderCommand(String command){ if(role!=Role.LEADER)throw new IllegalStateException("not leader"); LogEntry e=new LogEntry(lastIndex()+1,term,command);log.add(e);return e; }
    public synchronized void commitThrough(long index){commitIndex=Math.min(index,lastIndex());}
    private long lastIndex(){return log.isEmpty()?0:log.get(log.size()-1).index();} private long lastTerm(){return lastIndex()==0?0:log.get(log.size()-1).term();} private long termAt(long i){return i==0?0:log.get((int)i-1).term();}
}
