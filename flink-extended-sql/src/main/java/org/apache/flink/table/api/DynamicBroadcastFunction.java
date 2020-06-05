package org.apache.flink.table.api;

import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;

import com.sdu.flink.utils.JsonUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.types.RowDataType;
import org.apache.flink.table.types.SqlSchemaTuple;
import org.apache.flink.table.types.schema.SqlScanSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class DynamicBroadcastFunction extends BroadcastProcessFunction<CRow, SqlSchemaTuple, CRow> {

  // CRow[type, data]
  private String streamNodePath;
  private transient MapStateDescriptor<String, Tuple2<Map<String, Integer>, List<String>>> stateDesc;

  private Map<String, Integer> sourceFieldNameToIndexes;
  private List<String> selectFieldNames;

  private transient Row projectRow;
  private transient Row outRow;
  private transient CRow outCRow;

  public DynamicBroadcastFunction(
      String streamNodePath,
      List<String> sourceFieldNames,
      List<String> selectFieldNames) {
    this.streamNodePath = Objects.requireNonNull(streamNodePath);
    Preconditions.checkArgument(isNotEmpty(sourceFieldNames));
    Preconditions.checkArgument(isNotEmpty(selectFieldNames));
    sourceFieldNameToIndexes = new HashMap<>();
    int index = 0;
    for (String fieldName : sourceFieldNames) {
      sourceFieldNameToIndexes.put(fieldName, index++);
    }
    this.selectFieldNames = new ArrayList<>(selectFieldNames);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    stateDesc = new MapStateDescriptor<>(
        "BroadcastState",
        Types.STRING,
        Types.TUPLE(
            Types.MAP(Types.STRING, Types.INT),
            Types.LIST(Types.STRING)
        )
    );
    projectRow = new Row(selectFieldNames.size());

    outRow = new Row(2);
    outCRow = new CRow(outRow, true);
  }

  @Override
  public void processElement(CRow value, ReadOnlyContext ctx, Collector<CRow> out)
      throws Exception {
    ReadOnlyBroadcastState<String, Tuple2<Map<String, Integer>, List<String>>> state = ctx.getBroadcastState(stateDesc);
    Map<String, Integer> sourceFieldToIndexes;
    List<String> selectFields;
    Tuple2<Map<String, Integer>, List<String>> rules;
    if (state == null || (rules = state.get(streamNodePath)) == null) {
      sourceFieldToIndexes = sourceFieldNameToIndexes;
      selectFields = selectFieldNames;
    } else {
      sourceFieldToIndexes = rules.f0;
      selectFields = rules.f1;
    }
    buildOutRow(value.row(), sourceFieldToIndexes, selectFields);
    out.collect(outCRow);
  }

  @Override
  public void processBroadcastElement(SqlSchemaTuple schemaTuple, Context ctx, Collector<CRow> out)
      throws Exception {
    BroadcastState<String, Tuple2<Map<String, Integer>, List<String>>> broadcastState = ctx.getBroadcastState(stateDesc);

    SqlScanSchema scan = schemaTuple.getStreamNodeSchema(streamNodePath, SqlScanSchema.class);
    Preconditions.checkState(scan != null);
    Preconditions.checkArgument(isNullOrEmpty(scan.getSourceFieldNames()));
    Preconditions.checkArgument(isNullOrEmpty(scan.getSelectFieldNames()));
    Map<String, Integer> sourceFieldNameToIndexes = new HashMap<>();
    int index = 0;
    for (String fieldName : scan.getSourceFieldNames()) {
      sourceFieldNameToIndexes.put(fieldName, index++);
    }
    broadcastState.put(streamNodePath, Tuple2.of(sourceFieldNameToIndexes, scan.getSelectFieldNames()));
    projectRow = new Row(scan.getSelectFieldNames().size());

    // 向下发送数据
    outRow.setField(0, RowDataType.SCHEMA.name());
    outRow.setField(1, JsonUtils.toJson(schemaTuple));
    out.collect(outCRow);
  }

  private void buildOutRow(Row in, Map<String, Integer> sourceFieldToIndexes, List<String> selectFields) {
    int selectFieldPos = 0;
    for (String selectField : selectFields) {
      Integer index = sourceFieldToIndexes.get(selectField);
      if (index == null) {
        throw new RuntimeException("stream source can't find field: " + selectField);
      }
      projectRow.setField(selectFieldPos++, in.getField(index));
    }
    outRow.setField(0, RowDataType.DATA.name());
    outRow.setField(1, JsonUtils.toJson(projectRow));
  }

}
