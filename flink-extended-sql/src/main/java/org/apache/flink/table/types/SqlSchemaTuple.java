package org.apache.flink.table.types;

import org.apache.flink.table.util.JsonUtils;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.table.api.TableException;

public class SqlSchemaTuple implements Serializable {

  private Map<String, String> streamToSchemas;

  public SqlSchemaTuple(Map<String, String> streamToSchemas) {
    this.streamToSchemas = Objects.requireNonNull(streamToSchemas);
  }

  public <T> T getStreamNodeSchema(String stream, Class<T> clazz) {
    String schema = streamToSchemas.get(stream);
    if (schema == null || schema.isEmpty()) {
      throw new TableException("DataStream execute plan schema empty, node: " + stream);
    }

    return JsonUtils.fromJson(schema, clazz);
  }

}
