package org.apache.flink.table.codegen;


import static org.apache.flink.types.DTypeConverts.sqlTypeToJavaType;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DSqlFunctionInvoker implements DRexInvoker<String> {

  private static final String DEFAULT_METHOD_NAME = "eval";

  private final String className;

  private final SqlTypeName resultType;

  private final List<DRexInvoker<?>> parameterRexInvokes;

  public DSqlFunctionInvoker(String className, List<DRexInvoker<?>> parameterRexInvokes, SqlTypeName resultType) {
    this.className = className;
    this.parameterRexInvokes = parameterRexInvokes;
    this.resultType = resultType;
  }


  @Override
  public String invoke(DRecordTuple recordTuple) throws DExpressionInvokeException {
    // TODO: 没必要都要构造Method, Method修饰目前支持静态方法, Method.open()
    try {
      Class<?> cls = Class.forName(className);
      Class<?>[] paramTypes = new Class[parameterRexInvokes.size()];
      for (int i = 0; i < paramTypes.length; ++i) {
        paramTypes[i] = sqlTypeToJavaType(parameterRexInvokes.get(i).getResultType());
      }
      Method method = cls.getMethod(DEFAULT_METHOD_NAME, paramTypes);

      Object[] params = new Object[parameterRexInvokes.size()];
      for (int i = 0; i < params.length; ++i) {
        params[i] = parameterRexInvokes.get(i).invoke(recordTuple);
      }
      // TODO: 强转string?
      return String.valueOf(method.invoke(null, params));
    } catch (Exception e) {
      throw new DExpressionInvokeException("invoke user define function failure", e);
    }

  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
