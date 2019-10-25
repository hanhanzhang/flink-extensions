package org.apache.flink.types;

import java.util.List;
import lombok.Data;

@Data
public class DFunctionProjectSchema extends DProjectSchema {

  // 别名
  private final String className;
  private final List<DProjectSchema> parameterProjectSchemas;

  public DFunctionProjectSchema(String fieldName, String fileType, String className, List<DProjectSchema> parameterProjectSchemas) {
    super(fieldName, fileType);
    this.className = className;
    this.parameterProjectSchemas = parameterProjectSchemas;
  }

}
