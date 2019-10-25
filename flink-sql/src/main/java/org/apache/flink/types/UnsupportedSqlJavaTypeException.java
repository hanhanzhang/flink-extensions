package org.apache.flink.types;

import org.apache.calcite.sql.type.SqlTypeName;

public class UnsupportedSqlJavaTypeException extends RuntimeException {

  public UnsupportedSqlJavaTypeException(SqlTypeName sqlTypeName) {
    super("Unsupported sql type: " + sqlTypeName.getName());
  }

}
