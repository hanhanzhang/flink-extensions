package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexInputRefInvoker implements DRexInvoker {

  private final String fieldName;
  private final SqlTypeName resultType;

  DRexInputRefInvoker(String fieldName,  SqlTypeName resultType) {
    this.fieldName = fieldName;
    this.resultType = resultType;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    return recordTuple.getRecordValue(fieldName);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
