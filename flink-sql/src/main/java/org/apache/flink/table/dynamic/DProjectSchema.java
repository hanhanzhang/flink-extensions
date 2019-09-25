package org.apache.flink.table.dynamic;

public class DProjectSchema implements DSchema{


  private int version;
  private String[] columnNames;

  public DProjectSchema(int version, String[] columnNames) {
    this.version = version;
    this.columnNames = columnNames;
  }

  @Override
  public int getVersion() {
    return version;
  }

  public String[] getColumnNames() {
    return columnNames;
  }

}
