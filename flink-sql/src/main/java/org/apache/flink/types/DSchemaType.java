package org.apache.flink.types;

import org.apache.flink.annotation.Internal;

@Internal
public enum DSchemaType {

  PROJECT("project"), CONDITION("filter");

  private final String schemaTypeName;

  DSchemaType(String schemaType) {
    this.schemaTypeName = schemaType;
  }

  public String getSchemaTypeName() {
    return schemaTypeName;
  }

}
