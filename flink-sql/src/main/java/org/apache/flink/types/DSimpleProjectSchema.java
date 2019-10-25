package org.apache.flink.types;

public class DSimpleProjectSchema implements DProjectSchema {

  private final String fieldName;
  private final String fieldType;

  public DSimpleProjectSchema(String fieldName, String fieldType) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

  @Override
  public String fieldName() {
    return fieldName;
  }

  @Override
  public String fileType() {
    return fieldType;
  }

}
