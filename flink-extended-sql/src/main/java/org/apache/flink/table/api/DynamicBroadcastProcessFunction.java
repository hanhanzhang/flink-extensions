package org.apache.flink.table.api;

import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;

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
import org.apache.flink.table.utils.JsonUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class DynamicBroadcastProcessFunction extends BroadcastProcessFunction<CRow, SqlSchemaTuple, CRow> {

  // CRow[type, data]
  private final String streamNodePath;
  private transient MapStateDescriptor<String, Tuple2<Map<String, Integer>, List<String>>> stateDesc;

  private final Map<String, Integer> sourceFieldNameToIndexes;
  private final List<String> selectFieldNames;

  public DynamicBroadcastProcessFunction(
      String streamNodePath,
      List<String> sourceFieldNames,
      List<String> selectFieldNames) {
    this.streamNodePath = Objects.requireNonNull(streamNodePath);
    Preconditions.checkArgument(isNullOrEmpty(sourceFieldNames));
    Preconditions.checkArgument(isNullOrEmpty(selectFieldNames));
    sourceFieldNameToIndexes = new HashMap<>();
    int index = 0;
    for (String fieldName : sourceFieldNames) {
      sourceFieldNameToIndexes.put(fieldName, index++);
    }
    this.selectFieldNames = selectFieldNames;
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
    out.collect(buildOutRow(value.row(), sourceFieldToIndexes, selectFields));
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

    // 向下发送数据
    Row outRow = Row.of(RowDataType.SCHEMA.name(), JsonUtils.toJson(schemaTuple));
    out.collect(new CRow(outRow, true));
  }

  private CRow buildOutRow(Row in, Map<String, Integer> sourceFieldToIndexes, List<String> selectFields) {
    // TODO: 2020-06-04
    throw new RuntimeException("");
  }

}
