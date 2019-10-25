package org.apache.flink.table.codegen;


import static org.apache.flink.types.SqlTypeToJavaTypeConverts.sqlTypeToJavaType;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DFunctionDExpressionInvoker implements DExpressionInvoker {

  private static final String DEFAULT_METHOD_NAME = "eval";

  private final String className;

  private final SqlTypeName resultType;

  private final List<DExpressionInvoker<?>> parameterExpressionInvokes;

  DFunctionDExpressionInvoker(String className, List<DExpressionInvoker<?>> parameterExpressionInvokes, SqlTypeName resultType) {
    this.className = className;
    this.parameterExpressionInvokes = parameterExpressionInvokes;
    this.resultType = resultType;
  }


  @Override
  public Object invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    // TODO: 没必要都要构造Method, Method修饰目前支持静态方法, Method.open()
    try {
      Class<?> cls = Class.forName(className);
      Class<?>[] paramTypes = new Class[parameterExpressionInvokes.size()];
      for (int i = 0; i < paramTypes.length; ++i) {
        paramTypes[i] = sqlTypeToJavaType(parameterExpressionInvokes.get(i).getResultType());
      }
      Method method = cls.getMethod(DEFAULT_METHOD_NAME, paramTypes);

      Object[] params = new Object[parameterExpressionInvokes.size()];
      for (int i = 0; i < params.length; ++i) {
        params[i] = parameterExpressionInvokes.get(i).invoke(recordTuple);
      }
      return method.invoke(null, params);
    } catch (Exception e) {
      throw new DExpressionInvokeException("invoke user define function failure", e);
    }

  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }
}
