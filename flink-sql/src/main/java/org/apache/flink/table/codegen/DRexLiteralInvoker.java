package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexLiteralInvoker implements DRexInvoker<String> {

  private final Object literalValue;
  private final SqlTypeName resultType;

  DRexLiteralInvoker(Object literalValue, SqlTypeName resultType) {
    this.literalValue = literalValue;
    this.resultType = resultType;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    // TODO: 2019-10-28 类型处理
    return String.valueOf(literalValue);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }
}
