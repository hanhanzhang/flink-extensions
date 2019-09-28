package org.apache.flink.types;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author hanhan.zhang
 * */
public class SimpleSqlElement implements DSqlElement {

  private boolean isSchema;

  private String[] selectFields;

  private Map<String, String> fieldValues;

  private SimpleSqlElement(String[] selectFields) {
    this.isSchema = true;
    this.selectFields = selectFields;
  }

  private SimpleSqlElement(Map<String, String> fieldValues) {
    this.isSchema = false;
    this.fieldValues = fieldValues;
  }


  public static SimpleSqlElement ofSchema(String[] selectFields) {
    return new SimpleSqlElement(selectFields);
  }

  public static SimpleSqlElement copy(String[] selectFields) {
    String[] newSelectFields = new String[selectFields.length];
    System.arraycopy(selectFields, 0, newSelectFields, 0, selectFields.length);
    return new SimpleSqlElement(newSelectFields);
  }

  public static SimpleSqlElement ofElement(Map<String, String> fieldValues) {
    return new SimpleSqlElement(fieldValues);
  }


  @Override
  public boolean isSchema() {
    return isSchema;
  }

  @Override
  public boolean isRecord() {
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
    SimpleSqlElement that = (SimpleSqlElement) o;
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
