package org.apache.flink.table.exec;


import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.util.Preconditions;

public class DRexBooleanInvoker implements DRexInvoker{

  private final SqlTypeName resultType;
  private final DRexInvoker invoker;
  private final RexBooleanType type;

  DRexBooleanInvoker(SqlTypeName resultType, DRexInvoker invoker, RexBooleanType type) {
    Preconditions.checkState(resultType == SqlTypeName.BOOLEAN);
    Preconditions.checkState(invoker.getResultType() == SqlTypeName.BOOLEAN);

    this.resultType = resultType;
    this.invoker = invoker;
    this.type = type;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    boolean value = (boolean) invoker.invoke(recordTuple);

    switch (type) {
      case FALSE:
      case NOT:
        return !value;
      case TRUE:
        return value;
      default:
        throw new DRexUnsupportedException("Unsupported RexBooleanType: " + type);
    }
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

  public enum RexBooleanType {

    NOT, TRUE, FALSE

  }

}
