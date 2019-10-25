package org.apache.flink.types;

import java.util.List;

public class DFunctionProjectSchema implements DProjectSchema {

  // 别名
  private final String fieldName;
  private final String fileType;

  private final String className;
  private final List<DProjectSchema> parameterProjectSchemas;

  public DFunctionProjectSchema(String fieldName, String fileType, String className, List<DProjectSchema> parameterProjectSchemas) {
    this.fieldName = fieldName;
    this.fileType = fileType;
    this.className = className;
    this.parameterProjectSchemas = parameterProjectSchemas;
  }

  public String getClassName() {
    return className;
  }

  public List<DProjectSchema> getParameterProjectSchemas() {
    return parameterProjectSchemas;
  }

  @Override
  public String fieldName() {
    return fieldName;
  }

  @Override
  public String fileType() {
    return className;
  }

}
