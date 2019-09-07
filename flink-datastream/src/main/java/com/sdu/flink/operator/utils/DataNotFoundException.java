package com.sdu.flink.operator.utils;

public class DataNotFoundException extends RuntimeException {

  public DataNotFoundException(String message) {
    super(message);
  }

}
