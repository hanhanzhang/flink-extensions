package com.sdu.flink.table.functions;

import enhance.FunctionEnhance;

@FunctionEnhance
public class SimpleScalarFunction extends HScalarFunction{

  public String eval(String str) {
    return String.format("%s_%s_%s", "$", str, "%s");
  }

}
