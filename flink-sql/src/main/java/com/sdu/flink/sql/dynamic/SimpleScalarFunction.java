package com.sdu.flink.sql.dynamic;

import org.apache.flink.table.functions.ScalarFunction;

public class SimpleScalarFunction extends ScalarFunction {

  public static String eval(String text) {
    return "$_" + text;
  }

}
