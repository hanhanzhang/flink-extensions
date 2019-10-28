package org.apache.flink.table.codegen;

import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DRecordTuple;

@Internal
public class DRexLogicalInvoker implements DRexFilterInvoker {

  private final RexLogicalType rexLogicalType;

  private List<DRexInvoker> operatorInvokers;

  DRexLogicalInvoker(RexLogicalType rexLogicalType, List<DRexInvoker> operatorInvokers) {
    this.rexLogicalType = rexLogicalType;
    this.operatorInvokers = operatorInvokers;
  }

  @Override
  public Boolean invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    switch (rexLogicalType) {
      case AND:
        for (DRexInvoker invoker : operatorInvokers) {
          Boolean result = (Boolean) invoker.invoke(recordTuple);
          if (!result) {
            return false;
          }
        }
        return true;
      case OR:
        for (DRexInvoker invoker : operatorInvokers) {
          Boolean result = (Boolean) invoker.invoke(recordTuple);
          if (result) {
            return true;
          }
        }
        return false;
    }
    return null;
  }

  @Override
  public SqlTypeName getResultType() {
    return SqlTypeName.BOOLEAN;
  }

  public enum RexLogicalType {

    AND, OR

  }

}
