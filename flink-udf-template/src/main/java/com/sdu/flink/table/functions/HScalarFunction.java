package com.sdu.flink.table.functions;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public abstract class HScalarFunction extends ScalarFunction {

  protected FunctionContext context;


  @Override
  public void open(FunctionContext context) throws Exception {
    this.context = context;
  }
}
