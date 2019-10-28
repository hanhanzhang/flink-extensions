package org.apache.flink.table.codegen;

import org.apache.flink.types.DRecordTuple;

public class DConditionInvoker implements DRexFilterInvoker {

  private final DRexFilterInvoker filterInvoker;

  public DConditionInvoker(DRexFilterInvoker filterInvoker) {
    this.filterInvoker = filterInvoker;
  }

  @Override
  public Boolean invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    return filterInvoker.invoke(recordTuple);
  }

}
