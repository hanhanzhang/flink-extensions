package org.apache.flink.table.codegen;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DProjectFieldDExpression implements DExpressionInvoker<String> {

  /*
   * 结果类型:
   *
   * 1: 若是映射字段, 则返回字段类型
   *
   * 2: 若是UDF调用, 则返回UDF调用结果类型
   * */
  private DExpressionInvoker<String> dExpressionInvoker;

  DProjectFieldDExpression(DExpressionInvoker<String> dExpressionInvoker) {
    this.dExpressionInvoker = dExpressionInvoker;
  }

  @Override
  public String invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    return dExpressionInvoker.invoke(recordTuple);
  }

  @Override
  public SqlTypeName getResultType() {
    return dExpressionInvoker.getResultType();
  }

}
