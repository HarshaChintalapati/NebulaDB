package dev.nebuladb.storage;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public final class LsmStorageEngine implements StorageEngine {
    private final Path dir; private final int flushEntries; private final WriteAheadLog wal; private final AtomicLong seq=new AtomicLong();
    private ConcurrentSkipListMap<String,SSTable.Entry> mem=new ConcurrentSkipListMap<>(); private final List<SSTable> tables=new ArrayList<>();
    public LsmStorageEngine(Path dir,int flushEntries)throws IOException{ this.dir=dir;this.flushEntries=flushEntries;Files.createDirectories(dir); loadTables(); wal=new WriteAheadLog(dir.resolve("wal.log")); recover(); }
    public synchronized void put(String k,String v)throws IOException{ long s=seq.incrementAndGet(); wal.append(new WriteAheadLog.Record(s,WriteAheadLog.Op.PUT,k,v)); mem.put(k,new SSTable.Entry(s,false,k,v)); maybeFlush(); }
    public synchronized Optional<String> get(String k)throws IOException{ SSTable.Entry m=mem.get(k); if(m!=null)return m.tombstone()?Optional.empty():Optional.of(m.value()); ListIterator<SSTable> it=tables.listIterator(tables.size()); while(it.hasPrevious()){ Optional<SSTable.Entry> e=it.previous().get(k); if(e.isPresent()) return e.get().tombstone()?Optional.empty():Optional.of(e.get().value()); } return Optional.empty(); }
    public synchronized void delete(String k)throws IOException{ long s=seq.incrementAndGet(); wal.append(new WriteAheadLog.Record(s,WriteAheadLog.Op.DELETE,k,null)); mem.put(k,new SSTable.Entry(s,true,k,null)); maybeFlush(); }
    public synchronized void flush()throws IOException{ if(mem.isEmpty())return; List<SSTable.Entry> entries=new ArrayList<>(mem.values()); entries.sort(Comparator.comparing(SSTable.Entry::key)); SSTable t=SSTable.write(dir.resolve(String.format("sst-%020d.db",seq.get())),entries); tables.add(t); mem=new ConcurrentSkipListMap<>(); wal.reset(); if(tables.size()>=4)compact(); }
    private void maybeFlush()throws IOException{ if(mem.size()>=flushEntries)flush(); }
    private void compact()throws IOException{ TreeMap<String,SSTable.Entry> latest=new TreeMap<>(); for(SSTable t:tables) for(SSTable.Entry e:t.all()){ SSTable.Entry cur=latest.get(e.key()); if(cur==null||e.seq()>cur.seq())latest.put(e.key(),e); } List<SSTable.Entry> keep=latest.values().stream().filter(e->!e.tombstone()).sorted(Comparator.comparing(SSTable.Entry::key)).toList(); SSTable merged=SSTable.write(dir.resolve(String.format("sst-compact-%020d.db",seq.incrementAndGet())),keep); for(SSTable t:tables)Files.deleteIfExists(t.path()); tables.clear(); tables.add(merged); }
    private void loadTables()throws IOException{ try(DirectoryStream<Path>s=Files.newDirectoryStream(dir,"sst-*.db")){ List<Path> p=new ArrayList<>(); for(Path x:s)p.add(x); p.sort(Comparator.comparing(Path::getFileName)); for(Path x:p)tables.add(new SSTable(x)); } }
    private void recover()throws IOException{ for(WriteAheadLog.Record r:wal.replay()){ seq.set(Math.max(seq.get(),r.seq())); mem.put(r.key(),new SSTable.Entry(r.seq(),r.op()==WriteAheadLog.Op.DELETE,r.key(),r.value())); } }
    public synchronized void close(){ try{flush();wal.close();}catch(IOException e){throw new RuntimeException(e);} }
}
