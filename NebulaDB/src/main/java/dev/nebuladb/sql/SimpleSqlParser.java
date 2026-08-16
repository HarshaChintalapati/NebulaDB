package dev.nebuladb.sql;
import java.util.regex.*;
public final class SimpleSqlParser{
 private static final Pattern C=Pattern.compile("(?i)^CREATE\\s+TABLE\\s+(\\w+)\\s*\\(.+\\)\\s*;?$");
 private static final Pattern I=Pattern.compile("(?i)^INSERT\\s+INTO\\s+(\\w+)\\s+VALUES\\s*\\(\\s*([^,]+)\\s*,\\s*'([^']*)'\\s*\\)\\s*;?$");
 private static final Pattern S=Pattern.compile("(?i)^SELECT\\s+\\*\\s+FROM\\s+(\\w+)\\s+WHERE\\s+\\w+\\s*=\\s*([^;]+)\\s*;?$");
 private static final Pattern U=Pattern.compile("(?i)^UPDATE\\s+(\\w+)\\s+SET\\s+\\w+\\s*=\\s*'([^']*)'\\s+WHERE\\s+\\w+\\s*=\\s*([^;]+)\\s*;?$");
 private static final Pattern D=Pattern.compile("(?i)^DELETE\\s+FROM\\s+(\\w+)\\s+WHERE\\s+\\w+\\s*=\\s*([^;]+)\\s*;?$");
 public Statement parse(String sql){ String s=sql.trim(); Matcher m=C.matcher(s); if(m.matches())return new Statement.Create(m.group(1)); m=I.matcher(s); if(m.matches())return new Statement.Insert(m.group(1),clean(m.group(2)),m.group(3)); m=S.matcher(s); if(m.matches())return new Statement.Select(m.group(1),clean(m.group(2))); m=U.matcher(s); if(m.matches())return new Statement.Update(m.group(1),clean(m.group(3)),m.group(2)); m=D.matcher(s); if(m.matches())return new Statement.Delete(m.group(1),clean(m.group(2))); throw new IllegalArgumentException("Unsupported SQL: "+sql); }
 private static String clean(String x){ x=x.trim(); return x.startsWith("'")&&x.endsWith("'")?x.substring(1,x.length()-1):x; }
}
