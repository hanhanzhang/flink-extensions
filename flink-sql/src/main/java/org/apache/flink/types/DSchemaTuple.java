package org.apache.flink.types;

import com.sdu.flink.utils.JsonUtils;
import java.io.Serializable;
import java.util.EnumMap;
import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DSchemaTuple implements Serializable {

  private final EnumMap<DSchemaType, String> schemaRecords;

  public DSchemaTuple() {
    schemaRecords = new EnumMap<>(DSchemaType.class);
  }

  public void addProjectSchema(DProjectSchema projectSchema) {
    schemaRecords.put(DSchemaType.PROJECT, JsonUtils.toJson(projectSchema));
  }

  public DProjectSchema getProjectSchema() {
    String projectSchemaData = schemaRecords.get(DSchemaType.PROJECT);
    if (projectSchemaData == null) {
      return null;
    }

    return JsonUtils.fromJson(projectSchemaData, DProjectSchema.class);
  }


  public DConditionSchema getConditionSchema() {
    String conditionSchemaData = schemaRecords.get(DSchemaType.CONDITION);
    if (conditionSchemaData == null) {
      return null;
    }

    return JsonUtils.fromJson(conditionSchemaData, DConditionSchema.class);
  }

}
