package org.apache.flink.types;

import com.sdu.flink.utils.JsonUtils;
import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DSchemaTuple implements Serializable {

  private final Map<String, Map<String, String>> schemaNodeSchemas;

  public DSchemaTuple() {
    schemaNodeSchemas = new HashMap<>();
  }

  public <T> T getStreamNodeSchema(String streamNode, DSchemaType schemaType, Type type) {
    Map<String, String> streamNodeSchema = schemaNodeSchemas.get(streamNode);
    if (streamNodeSchema == null) {
      return null;
    }

    String schemaJson = streamNodeSchema.get(schemaType.getSchemaTypeName());
    if (schemaJson == null) {
      return null;
    }

    return JsonUtils.fromJson(schemaJson, type);
  }

  public void addStreamNodeSchema(String streamNode, Map<String, String> streamNodeSchema) {
    schemaNodeSchemas.put(streamNode, streamNodeSchema);
  }

}
