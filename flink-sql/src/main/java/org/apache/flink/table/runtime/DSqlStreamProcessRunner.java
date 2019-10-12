package org.apache.flink.table.runtime;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets.SetView;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.CompositeDRow;
import org.apache.flink.util.Collector;

public class DSqlStreamProcessRunner extends ProcessFunction<CompositeDRow, CompositeDRow> {

  private final Map<String, RexType> columnToTypes;

  public DSqlStreamProcessRunner(List<Exepression> projectExpression) {
    columnToTypes = projectExpression.stream()
        .collect(Collectors.toConcurrentMap(Exepression::getFieldName, Exepression::getType));
  }



  @Override
  public void processElement(CompositeDRow value, Context ctx, Collector<CompositeDRow> out)
      throws Exception {

    if (value.isSchema()) {
      Set<String> selectFields = Sets.newHashSet(value.getSelectFields());
      // 删除
      SetView<String> removeFields = Sets.difference(columnToTypes.keySet(), selectFields);
      for (String removeField : removeFields) {
        columnToTypes.remove(removeField);
      }

      // 增加
      SetView<String> addFields = Sets.difference(selectFields, columnToTypes.keySet());
      for (String addField : addFields) {
        columnToTypes.put(addField, RexType.NAME);
      }
      return;
    }

    // TODO: condition, udf
    Map<String, String> fieldValues =  value.getFieldValues();
    Map<String, String> newValues = Maps.newHashMapWithExpectedSize(columnToTypes.size());
    for (Entry<String, RexType> entry : columnToTypes.entrySet()) {
      switch (entry.getValue()) {
        case NAME:
          newValues.put(entry.getKey(), fieldValues.get(entry.getKey()));
          break;
        case UDF:
          break;
        default:

      }
    }

    //
    out.collect(CompositeDRow.ofDRow(newValues));
  }



}
