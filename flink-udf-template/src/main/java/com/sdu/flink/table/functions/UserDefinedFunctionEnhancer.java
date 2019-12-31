package com.sdu.flink.table.functions;

public interface UserDefinedFunctionEnhancer {

  void before();

  void after();

  void onException(Throwable cause);
}
