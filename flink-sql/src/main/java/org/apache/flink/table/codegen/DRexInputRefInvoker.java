package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DRexInputRefInvoker implements DRexInvoker<String> {

  private final String fieldName;
  private final SqlTypeName resultType;

  public DRexInputRefInvoker(String fieldName,  SqlTypeName resultType) {
    this.fieldName = fieldName;
    this.resultType = resultType;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    return recordTuple.getRecordValue(fieldName);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
