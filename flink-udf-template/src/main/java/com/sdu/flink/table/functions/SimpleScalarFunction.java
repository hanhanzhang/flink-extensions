package com.sdu.flink.table.functions;

import com.sdu.flink.udf.enhance.FunctionEnhance;

@FunctionEnhance
public class SimpleScalarFunction extends HScalarFunction {

  public String eval(String str) {
    return String.format("%s_%s_%s", "$", str, "%s");
  }

  @Override
  public boolean isThrowException() {
    return true;
  }


}
