package org.apache.flink.types;

import java.util.Map;
import lombok.Data;
import org.apache.flink.annotation.Internal;

@Internal
@Data
public class DProjectSchemaData implements DSchemaData {

  /*
   * ProjectSchema信息
   *
   * 1: 节点映射字段名及类型
   *
   * 2: UDF信息
   *
   * NOTE:
   *
   *   Project字段若使用UDF处理, 会使用别名, 这种情况, 收到Schema信息后修改字段名及映射关系
   * **/

  // 节点输入的字段名及类型
  private final Map<String, DProjectSchema> inputProjectNameToTypes;

  public DProjectSchemaData(Map<String, String> inputProjectNameToTypes) {
    throw new RuntimeException();
  }

  @Override
  public DSchemaType schemaType() {
    return DSchemaType.PROJECT;
  }

}
