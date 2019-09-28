package org.apache.flink.table.runtime;

import java.util.Objects;

public class Exepression {

  private String fieldName;

  private RexType type;

  public Exepression(String fieldName, RexType type) {
    this.fieldName = fieldName;
    this.type = type;
  }

  public String getFieldName() {
    return fieldName;
  }

  public RexType getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Exepression that = (Exepression) o;
    return Objects.equals(fieldName, that.fieldName) &&
        type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldName, type);
  }
}
