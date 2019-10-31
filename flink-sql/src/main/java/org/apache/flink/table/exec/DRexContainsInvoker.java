package org.apache.flink.table.exec;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.util.Preconditions;

public class DRexContainsInvoker implements DRexInvoker {

  private final SqlTypeName resultType;
  private final DRexInvoker needle;
  private final List<DRexInvoker> haystack;


  DRexContainsInvoker(SqlTypeName resultType, DRexInvoker needle, List<DRexInvoker> haystack) {
    Preconditions.checkState(resultType == SqlTypeName.BOOLEAN);
    Preconditions.checkState(needle.getResultType() == haystack.get(0).getResultType());

    for (DRexInvoker invoker : haystack) {
      Preconditions.checkState(needle.getResultType() == invoker.getResultType());
    }

    this.resultType = resultType;
    this.needle = needle;
    this.haystack = haystack;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    Set<Object> candidates = new HashSet<>();
    for (DRexInvoker invoker : haystack) {
      candidates.add(invoker.invoke(recordTuple));
    }

    Object target = needle.invoke(recordTuple);

    return candidates.contains(target);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
