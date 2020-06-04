package org.apache.flink.table.types.schema;

import java.util.Objects;

public class SqlCalcSchema implements SqlSchema {

  private String name;
  private String code;

  public SqlCalcSchema(String name, String code) {
    this.name = Objects.requireNonNull(name);
    this.code = Objects.requireNonNull(code);
  }

  public String getName() {
    return name;
  }

  public String getCode() {
    return code;
  }

}
