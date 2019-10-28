package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexLiteralInvoker implements DRexInvoker<String> {

  private final String literalValue;
  private final SqlTypeName resultType;

  public DRexLiteralInvoker(String literalValue, SqlTypeName resultType) {
    this.literalValue = literalValue;
    this.resultType = resultType;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    return literalValue;
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }
}
