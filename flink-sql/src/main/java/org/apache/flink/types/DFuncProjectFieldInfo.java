package org.apache.flink.types;

import java.util.List;
import lombok.Data;

@Data
public class DFuncProjectFieldInfo extends DProjectFieldInfo {

  private final String className;
  private final List<DProjectFieldInfo> parameterProjectFields;

  public DFuncProjectFieldInfo(String fieldName, String fileType, String className, List<DProjectFieldInfo> parameterProjectFields) {
    super(fieldName, fileType);
    this.className = className;
    this.parameterProjectFields = parameterProjectFields;
  }

}
