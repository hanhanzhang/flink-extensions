package org.apache.flink.table.codegen;

import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DRecordTuple;

@Internal
public class DRexLogicalInvoker implements DRexFilterInvoker {

  private final OperatorType operatorType;

  private List<DRexFilterInvoker> operatorInvokers;

  public DRexLogicalInvoker(OperatorType operatorType, List<DRexFilterInvoker> operatorInvokers) {
    this.operatorType = operatorType;
    this.operatorInvokers = operatorInvokers;
  }

  @Override
  public Boolean invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    switch (operatorType) {
      case AND:
        for (DRexFilterInvoker invoker : operatorInvokers) {
          if (!invoker.invoke(recordTuple)) {
            return false;
          }
        }
        return true;
      case OR:
        for (DRexFilterInvoker invoker : operatorInvokers) {
          if (invoker.invoke(recordTuple)) {
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

  public enum OperatorType {

    AND, OR

  }

}
