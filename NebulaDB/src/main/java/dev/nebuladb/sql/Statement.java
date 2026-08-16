package dev.nebuladb.sql;
public sealed interface Statement permits Statement.Create,Statement.Insert,Statement.Select,Statement.Update,Statement.Delete{
 record Create(String table) implements Statement{} record Insert(String table,String key,String value)implements Statement{} record Select(String table,String key)implements Statement{} record Update(String table,String key,String value)implements Statement{} record Delete(String table,String key)implements Statement{}
}
