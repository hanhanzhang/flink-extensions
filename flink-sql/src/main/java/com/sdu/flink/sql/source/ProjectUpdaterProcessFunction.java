package com.sdu.flink.sql.source;

import static org.apache.flink.configuration.DSqlOptions.BROADCAST_STATE_NAME;

import java.util.Map;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.dynamic.DProjectSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author hanhan.zhang
 * */
public class ProjectUpdaterProcessFunction extends BroadcastProcessFunction<Row, DProjectSchema, Row> {

  private transient MapStateDescriptor<Void, DProjectSchema> projectSchemaStateDescriptor;

  private final String[] selectNames;
  private final Map<String, Integer> nameToIndex;

  public ProjectUpdaterProcessFunction(String[] selectNames, Map<String, Integer> nameToIndex) {
    this.selectNames = selectNames;
    this.nameToIndex = nameToIndex;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    projectSchemaStateDescriptor = new MapStateDescriptor<>(
        BROADCAST_STATE_NAME, Types.VOID, TypeInformation.of(DProjectSchema.class));
  }

  @Override
  public void processElement(Row value, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
    ReadOnlyBroadcastState<Void, DProjectSchema> broadcastState = ctx.getBroadcastState(projectSchemaStateDescriptor);
    if (broadcastState != null && broadcastState.get(null) != null) {
      DProjectSchema schema = broadcastState.get(null);
      Row newRow = createNewRow(value, schema.getColumnNames(), nameToIndex);
      out.collect(newRow);
    } else {
      Row newRow = createNewRow(value, selectNames, nameToIndex);
      out.collect(newRow);
    }
  }

  @Override
  public void processBroadcastElement(DProjectSchema value, Context ctx, Collector<Row> out)
      throws Exception {
    BroadcastState<Void, DProjectSchema> broadcastState = ctx.getBroadcastState(projectSchemaStateDescriptor);
    broadcastState.put(null, value);

    /*
     * 向下游发送变更映射字段信息, Row协议定义:
     *
     * 0: true / false, 若 true 则为变更的 Schema, 否则为数据流
     * **/
    Row schemaRow = new Row(2);
    schemaRow.setField(0, true);
    schemaRow.setField(1, value);

    out.collect(schemaRow);
  }

  private static Row createNewRow(Row value, String[] selectNames, Map<String, Integer> nameToIndex) {
    Row newRow = new Row(selectNames.length + 1);
    newRow.setField(0, false);

    for (int i = 1; i <= selectNames.length; ++i) {
      newRow.setField(i, value.getField(nameToIndex.get(selectNames[i - 1])));
    }

    return newRow;
  }

}
