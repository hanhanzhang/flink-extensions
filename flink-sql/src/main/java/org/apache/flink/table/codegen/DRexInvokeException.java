package org.apache.flink.table.codegen;

class DRexInvokeException extends RuntimeException {

  DRexInvokeException(String message) {
    super(message);
  }

  DRexInvokeException(String message, Throwable cause) {
    super(message, cause);
  }
}
