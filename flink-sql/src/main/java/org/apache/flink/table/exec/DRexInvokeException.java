package org.apache.flink.table.exec;

class DRexInvokeException extends RuntimeException {

  DRexInvokeException(String message) {
    super(message);
  }

  DRexInvokeException(String message, Throwable cause) {
    super(message, cause);
  }
}
