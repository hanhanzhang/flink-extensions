package org.apache.flink.table.exec;


import static org.apache.flink.types.DSqlTypeUtils.sqlTypeToJavaType;

import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.types.DRecordTuple;

public class DSqlFunctionInvoker implements DRexInvoker {

  private static final String DEFAULT_METHOD_NAME = "eval";

  private final String className;

  private final SqlTypeName resultType;

  private final List<DRexInvoker> parameterInvokes;

  public DSqlFunctionInvoker(String className, List<DRexInvoker> parameterInvokes, SqlTypeName resultType) {
    this.className = className;
    this.parameterInvokes = parameterInvokes;
    this.resultType = resultType;
  }


  @Override
  public Object invoke(DRecordTuple recordTuple) throws DRexInvokeException {
    // TODO: 没必要都要构造Method, Method修饰目前支持静态方法, Method.open()
    try {
      Class<?> cls = Class.forName(className);
      Class<?>[] paramTypes = new Class[parameterInvokes.size()];
      for (int i = 0; i < paramTypes.length; ++i) {
        paramTypes[i] = sqlTypeToJavaType(parameterInvokes.get(i).getResultType());
      }
      Method method = cls.getMethod(DEFAULT_METHOD_NAME, paramTypes);

      Object[] params = new Object[parameterInvokes.size()];
      for (int i = 0; i < params.length; ++i) {
        params[i] = parameterInvokes.get(i).invoke(recordTuple);
      }
      return method.invoke(null, params);
    } catch (Exception e) {
      throw new DRexInvokeException("invoke user define function failure", e);
    }

  }

  @Override
  public SqlTypeName getResultType() {
    return resultType;
  }

}
