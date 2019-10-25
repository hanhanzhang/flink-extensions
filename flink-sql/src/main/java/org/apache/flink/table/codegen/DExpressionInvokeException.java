package org.apache.flink.table.codegen;

public class DExpressionInvokeException extends RuntimeException {

  public DExpressionInvokeException(String message) {
    super(message);
  }

  public DExpressionInvokeException(String message, Throwable cause) {
    super(message, cause);
  }
}
