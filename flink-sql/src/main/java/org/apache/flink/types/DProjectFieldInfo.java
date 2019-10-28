package org.apache.flink.types;

import java.util.Objects;
import lombok.Data;

@Data
public class DProjectFieldInfo {

  private final String fieldName;
  private final String fieldType;

  DProjectFieldInfo(String fieldName, String fieldType) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DProjectFieldInfo that = (DProjectFieldInfo) o;
    return fieldName.equals(that.fieldName) &&
        fieldType.equals(that.fieldType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, fieldType);
  }
}
