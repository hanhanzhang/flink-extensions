package com.sdu.flink.functions.co;

import com.sdu.flink.entry.CaptureActionEntry;
import com.sdu.flink.entry.UserActionEntry;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class CaptureActionBroadcastProcessFunction extends
    KeyedBroadcastProcessFunction<String, UserActionEntry, CaptureActionEntry, Row> {

  private transient MapStateDescriptor<Void, CaptureActionEntry> patternDescriptor;

  @Override
  public void open(Configuration parameters) throws Exception {
    patternDescriptor = new MapStateDescriptor<>("ActionBroadcastState", Types.VOID,
        TypeInformation.of(CaptureActionEntry.class));
  }

  @Override
  public void processElement(UserActionEntry entry, ReadOnlyContext ctx, Collector<Row> out)
      throws Exception {
    CaptureActionEntry actionEntry = ctx.getBroadcastState(patternDescriptor).get(null);
    if (actionEntry != null) {
      if (actionEntry.getAction().equals(entry.getAction())) {
        Row row = Row.of(
            actionEntry.getVersion(),
            entry.getUname(),
            entry.getSex(),
            entry.getAge(),
            entry.getAction(),
            actionEntry.getAction()
        );
        out.collect(row);
      }
    }
  }

  @Override
  public void processBroadcastElement(CaptureActionEntry entry, Context ctx, Collector<Row> out)
      throws Exception {
    BroadcastState<Void, CaptureActionEntry> broadcastState = ctx.getBroadcastState(patternDescriptor);
    broadcastState.put(null, entry);
  }

}
