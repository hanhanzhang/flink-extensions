package org.apache.flink.table.sources;

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.CompositeDRow;
import org.apache.flink.util.Collector;

/**
 * @author hanhan.zhang
 * */
public class DSqlProjectFieldsUpdaterProcessFunction extends BroadcastProcessFunction<CompositeDRow, CompositeDRow, CompositeDRow> {

  private transient MapStateDescriptor<Void, String[]> projectSchemaStateDescriptor;

  private final String[] selectNames;

  public DSqlProjectFieldsUpdaterProcessFunction(String[] selectNames) {
    this.selectNames = selectNames;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    projectSchemaStateDescriptor = new MapStateDescriptor<>(
        "BroadcastSqlProjectSchemaState", Types.VOID, TypeInformation.of(String[].class));
  }


  @Override
  public void processElement(CompositeDRow value, ReadOnlyContext ctx,
      Collector<CompositeDRow> out) throws Exception {
    if (value.isSchema()) {
      return;
    }

    ReadOnlyBroadcastState<Void, String[]> state = ctx.getBroadcastState(projectSchemaStateDescriptor);
    if (state == null || state.get(null) == null || state.get(null).length == 0) {
      out.collect(createNewElement(selectNames, value));
    } else {
      String[] selectFields = state.get(null);
      out.collect(createNewElement(selectFields, value));
    }
  }

  @Override
  public void processBroadcastElement(CompositeDRow value, Context ctx,
      Collector<CompositeDRow> out) throws Exception {
    if (value.isSchema()) {
      BroadcastState<Void, String[]> broadcastState = ctx.getBroadcastState(projectSchemaStateDescriptor);
      broadcastState.put(null, value.getSelectFields());

      out.collect(CompositeDRow.copy(value.getSelectFields()));
    }
  }

  private static CompositeDRow createNewElement(String[] selectNames, CompositeDRow value) {
    Map<String, String> newFieldValues = new HashMap<>();
    for (String field : selectNames) {
      newFieldValues.put(field, value.getFieldValues().get(field));
    }
    return CompositeDRow.ofDRow(newFieldValues);
  }

}
