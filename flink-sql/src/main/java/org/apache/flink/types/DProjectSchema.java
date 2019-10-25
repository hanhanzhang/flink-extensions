package org.apache.flink.types;

import java.util.Map;
import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DProjectSchema implements DSchemaData {

  private final Map<String, String> projectNameToTypes;

  public DProjectSchema(Map<String, String> projectNameToTypes) {
    this.projectNameToTypes = projectNameToTypes;
  }

  @Override
  public DSchemaType schemaType() {
    return DSchemaType.PROJECT;
  }

}
