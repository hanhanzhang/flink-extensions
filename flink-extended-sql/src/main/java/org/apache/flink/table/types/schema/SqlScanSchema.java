package org.apache.flink.table.types.schema;

import java.util.List;
import java.util.Objects;

public class SqlScanSchema implements SqlSchema {

  private List<String> sourceFieldNames;
  private List<String> selectFieldNames;

  public SqlScanSchema(List<String> sourceFieldNames, List<String> selectFieldNames) {
    this.sourceFieldNames = Objects.requireNonNull(sourceFieldNames);
    this.selectFieldNames = Objects.requireNonNull(selectFieldNames);
  }

  public List<String> getSourceFieldNames() {
    return sourceFieldNames;
  }

  public List<String> getSelectFieldNames() {
    return selectFieldNames;
  }

}
