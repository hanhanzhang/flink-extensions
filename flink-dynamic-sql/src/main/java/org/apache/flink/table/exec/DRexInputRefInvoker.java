package org.apache.flink.table.exec;

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
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    String fieldValue = recordTuple.getRecordValue(fieldName);
    return getRecordValue(fieldValue);
  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
