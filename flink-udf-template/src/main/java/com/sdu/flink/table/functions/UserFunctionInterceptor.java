package com.sdu.flink.table.functions;

public interface UserFunctionInterceptor {

  void before();

  void after();

  void onException(Throwable cause);
}
