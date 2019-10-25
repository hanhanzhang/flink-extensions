package org.apache.flink.types;

import lombok.Data;

@Data
public class DProjectSchema {

  private final String fieldName;
  private final String fieldType;

  DProjectSchema(String fieldName, String fieldType) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
  }

}
