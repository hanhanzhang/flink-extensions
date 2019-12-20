package org.apache.flink.types;

class UnsupportedTypeException extends RuntimeException {

  UnsupportedTypeException(String sqlTypeName) {
    super("Unsupported type: " + sqlTypeName);
  }

}
