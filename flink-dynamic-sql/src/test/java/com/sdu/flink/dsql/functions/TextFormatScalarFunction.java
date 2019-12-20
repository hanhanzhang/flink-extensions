package com.sdu.flink.dsql.functions;

import static java.lang.String.format;

import org.apache.flink.table.functions.ScalarFunction;

public class TextFormatScalarFunction extends ScalarFunction {

  public static String eval(String text) {
    return format("%s%s%s", "uid", "_", text);
  }

}
