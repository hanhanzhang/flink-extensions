package org.apache.flink.table.exec;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexLiteralInvoker implements DRexInvoker {

  private final Object literalValue;
  private final SqlTypeName resultType;

  DRexLiteralInvoker(Object literalValue, SqlTypeName resultType) {
    this.literalValue = literalValue;
    this.resultType = resultType;
  }

  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    return literalValue;
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }
}
