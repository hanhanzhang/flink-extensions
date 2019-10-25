package org.apache.flink.types;

import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DConditionSchema implements DSchemaData {

  @Override
  public DSchemaType schemaType() {
    return DSchemaType.CONDITION;
  }

}
