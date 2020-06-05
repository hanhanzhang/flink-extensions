package org.apache.flink.table.types.schema;

public class SqlSinkSchema implements SqlSchema {

  private String code;

  public SqlSinkSchema(String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }


}
