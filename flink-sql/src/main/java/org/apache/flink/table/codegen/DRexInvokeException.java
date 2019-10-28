package org.apache.flink.table.codegen;

public class DRexInvokeException extends RuntimeException {

  public DRexInvokeException(String message) {
    super(message);
  }

  public DRexInvokeException(String message, Throwable cause) {
    super(message, cause);
  }
}
