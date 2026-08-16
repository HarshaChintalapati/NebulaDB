package dev.nebuladb.query;
import dev.nebuladb.sharding.ShardRouter; import dev.nebuladb.sql.Statement; import dev.nebuladb.storage.LsmStorageEngine; import java.util.*;
public final class QueryCoordinator{
 private final ShardRouter router; private final Map<Integer,LsmStorageEngine> stores; private final Set<String> tables=new HashSet<>();
 public QueryCoordinator(ShardRouter r,Map<Integer,LsmStorageEngine>s){router=r;stores=s;}
 public synchronized String execute(Statement st)throws Exception{
  if(st instanceof Statement.Create c){ if(!tables.add(c.table()))throw new IllegalStateException("table exists"); return "table created"; }
  if(st instanceof Statement.Insert i){need(i.table()); String k=key(i.table(),i.key()); stores.get(router.shardFor(k)).put(k,i.value()); return "inserted";}
  if(st instanceof Statement.Update u){need(u.table()); String k=key(u.table(),u.key()); stores.get(router.shardFor(k)).put(k,u.value()); return "updated";}
  if(st instanceof Statement.Delete d){need(d.table()); String k=key(d.table(),d.key()); stores.get(router.shardFor(k)).delete(k); return "deleted";}
  if(st instanceof Statement.Select s){need(s.table()); String k=key(s.table(),s.key()); return stores.get(router.shardFor(k)).get(k).map(v->s.key()+","+v).orElse("(0 rows)");}
  throw new IllegalArgumentException();
 }
 private void need(String t){if(!tables.contains(t))throw new IllegalStateException("unknown table "+t);} private static String key(String t,String k){return t+":"+k;}
}
