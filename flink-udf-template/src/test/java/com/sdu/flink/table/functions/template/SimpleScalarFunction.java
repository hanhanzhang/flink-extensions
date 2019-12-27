package com.sdu.flink.table.functions.template;

import com.sdu.flink.table.functions.enhance.ScalarFunctionEnhance;

@ScalarFunctionEnhance
public class SimpleScalarFunction extends QScalarFunction {

  public String eval(String str) {
    return String.format("%s_%s_%s", "$", str, "%s");
  }

  public int eval(String str1, String str2) {
    return 1;
  }


  @Override
  public String getDefaultStringValue() {
    return "EMPTY";
  }

  @Override
  public int getDefaultIntValue() {
    return -1;
  }


  @Override
  public String getMetricPrefix() {
    return "SimpleScalarFunction";
  }

}
