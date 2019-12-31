package com.sdu.flink.table.functions.template;

import com.sdu.flink.table.functions.UserDefinedFunctionEnhancer;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

public abstract class QTableFunction<T> extends TableFunction<T>
    implements UserDefinedFunctionEnhancer {

  @Override
  public void open(FunctionContext context) throws Exception {

  }

  @Override
  public void before() {

  }

  @Override
  public void after() {

  }

  @Override
  public void onException(Throwable cause) {

  }

  public abstract T getDefaultValue();

}
