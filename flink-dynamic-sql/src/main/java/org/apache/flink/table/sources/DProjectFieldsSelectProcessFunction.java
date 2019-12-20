package org.apache.flink.table.sources;

import static org.apache.flink.types.DSchemaType.PROJECT;

import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DStreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * @author hanhan.zhang
 */
public class DProjectFieldsSelectProcessFunction extends
    BroadcastProcessFunction<DRecordTuple, DSchemaTuple, DStreamRecord> {

  private final String streamNodePath;
  private transient MapStateDescriptor<Void, Map<String, String>> projectFieldStateDesc;

  private final Map<String, String> projectNameToTypes;

  public DProjectFieldsSelectProcessFunction(String streamNodePath, Map<String, String> nameToTypes) {
    this.streamNodePath = streamNodePath;
    this.projectNameToTypes = nameToTypes;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    projectFieldStateDesc = new MapStateDescriptor<>(
        "BroadcastSqlProjectSchemaState", Types.VOID, Types.MAP(Types.STRING, Types.STRING));
  }


  @Override
  public void processElement(DRecordTuple recordTuple, ReadOnlyContext ctx,
      Collector<DStreamRecord> out) throws Exception {
    ReadOnlyBroadcastState<Void, Map<String, String>> state = ctx
        .getBroadcastState(projectFieldStateDesc);
    if (state == null || state.get(null) == null || state.get(null).isEmpty()) {
      out.collect(buildStreamTupleRecord(projectNameToTypes, recordTuple));
    } else {
      Map<String, String> nameToType = state.get(null);
      out.collect(buildStreamTupleRecord(nameToType, recordTuple));
    }
  }

  @Override
  public void processBroadcastElement(DSchemaTuple schemaTuple, Context ctx,
      Collector<DStreamRecord> out) throws Exception {
    BroadcastState<Void, Map<String, String>> broadcastState = ctx.getBroadcastState(projectFieldStateDesc);

    // TableScan负责更新映射字段
    Map<String, String> projectSchema = schemaTuple.getStreamNodeSchema(streamNodePath, PROJECT,
        new TypeToken<Map<String, String>>(){}.getType());
    Preconditions.checkState(projectSchema != null && !projectSchema.isEmpty());
    broadcastState.put(null, projectSchema);

    out.collect(new DStreamRecord(schemaTuple));
  }

  private static DStreamRecord buildStreamTupleRecord(Map<String, String> nameToType,
      DRecordTuple recordTuple) {
    Map<String, String> recordValues = new HashMap<>();
    Map<String, String> recordTypes = new HashMap<>();
    for (Entry<String, String> entry : nameToType.entrySet()) {
      String fieldName = entry.getKey();
      String fieldType = entry.getValue();

      recordValues.put(fieldName, recordTuple.getRecordValue(fieldName));
      recordTypes.put(fieldName, fieldType);
    }

    return new DStreamRecord(new DRecordTuple(recordTypes, recordValues));
  }

}
