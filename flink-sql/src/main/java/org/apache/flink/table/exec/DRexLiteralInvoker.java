package org.apache.flink.table.exec;

import static org.apache.flink.types.DSqlTypeUtils.castObject;

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
    return castObject(literalValue, resultType);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }
}
