package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DSimpleProjectFieldExpressionInvoker implements DProjectFieldExpressionInvoker {

  private final String fieldName;
  private final SqlTypeName resultType;

  public DSimpleProjectFieldExpressionInvoker(String fieldName,  SqlTypeName resultType) {
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

  @Override
  public String getProjectFieldName() {
    return fieldName;
  }

}
