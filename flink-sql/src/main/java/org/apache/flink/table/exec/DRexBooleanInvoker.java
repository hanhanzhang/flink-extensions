package org.apache.flink.table.exec;


import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexBooleanInvoker implements DRexInvoker{

  private final boolean resultValue;

  private final DRexInvoker invoker;

  DRexBooleanInvoker(boolean resultValue, DRexInvoker invoker) {
    this.resultValue = resultValue;
    this.invoker = invoker;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    Object object = invoker.invoke(recordTuple);
    if (object instanceof Boolean) {
      Boolean value = (Boolean) object;
      return value == resultValue;
    }

    return new DRexInvokeException("rex call result type must be boolean, actual type: " + invoker.getResultType());
  }

  @Override
  public SqlTypeName getResultType() {
    return SqlTypeName.BOOLEAN;
  }
}
