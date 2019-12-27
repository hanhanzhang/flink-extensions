package com.sdu.flink.table.functions;

import com.sdu.flink.table.functions.enhance.FunctionEnhance;

@FunctionEnhance
public class SimpleScalarFunction extends QScalarFunction {

  public String eval(String str) {
    return String.format("%s_%s_%s", "$", str, "%s");
  }

  @Override
  public String getMetricPrefix() {
    return "SimpleScalarFunction";
  }

}
