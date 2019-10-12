package org.apache.flink.types;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author hanhan.zhang
 * */
public class CompositeDRow implements DRow {

  private boolean isSchema;

  private String[] selectFields;

  private Map<String, String> fieldValues;

  private CompositeDRow(String[] selectFields) {
    this.isSchema = true;
    this.selectFields = selectFields;
  }

  private CompositeDRow(Map<String, String> fieldValues) {
    this.isSchema = false;
    this.fieldValues = fieldValues;
  }


  public static CompositeDRow ofSchema(String[] selectFields) {
    return new CompositeDRow(selectFields);
  }

  public static CompositeDRow copy(String[] selectFields) {
    String[] newSelectFields = new String[selectFields.length];
    System.arraycopy(selectFields, 0, newSelectFields, 0, selectFields.length);
    return new CompositeDRow(newSelectFields);
  }

  public static CompositeDRow ofDRow(Map<String, String> fieldValues) {
    return new CompositeDRow(fieldValues);
  }


  @Override
  public boolean isSchema() {
    return isSchema;
  }

  @Override
  public boolean isRow() {
    return !isSchema;
  }

  public String[] getSelectFields() {
    return selectFields;
  }

  public Map<String, String> getFieldValues() {
    return fieldValues;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CompositeDRow that = (CompositeDRow) o;
    return isSchema == that.isSchema &&
        Arrays.equals(selectFields, that.selectFields) &&
        Objects.equals(fieldValues, that.fieldValues);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(isSchema, fieldValues);
    result = 31 * result + Arrays.hashCode(selectFields);
    return result;
  }
}
