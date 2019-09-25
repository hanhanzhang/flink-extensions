package com.sdu.flink.operator;

import com.sdu.flink.source.UserActionSourceFunction;
import com.sdu.flink.utils.UserActionEntry;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * @author hanhan.zhang
 * **/
public class BroadcastStateBootstrap {

  public static class CaptureAction implements Serializable {

    private int version;
    private String action;

    CaptureAction(int version, String action) {
      this.version = version;
      this.action = action;
    }

    public int getVersion() {
      return version;
    }

    public String getAction() {
      return action;
    }
  }

  public static class DynamicActionPatternSourceFunction implements SourceFunction<CaptureAction> {

    private final long interval;
    private final String[] actions;

    private boolean running;

    private long lastUpdateTimestamp;

    private int version;

    DynamicActionPatternSourceFunction(long interval, String[] actions) {
      this.interval = interval;
      this.actions = actions;
      running = true;
      lastUpdateTimestamp = 0;
      version = 0;
    }

    @Override
    public void run(SourceContext<CaptureAction> ctx) throws Exception {
      while (running) {
        boolean update = System.currentTimeMillis() - lastUpdateTimestamp >= interval;
        if (update) {
          String action = actions[version % actions.length];
          ctx.collect(new CaptureAction(version++, action));
          lastUpdateTimestamp = System.currentTimeMillis();
        }

        safeSleep(System.currentTimeMillis() - lastUpdateTimestamp);
      }
    }

    @Override
    public void cancel() {
      running = false;
    }

    private static void safeSleep(long millSeconds) {
      try {
        TimeUnit.MILLISECONDS.sleep(millSeconds);
      } catch (Exception e) {
        // ignore
      }
    }
  }

  public static class UserActionCaptureProcessFunction extends KeyedBroadcastProcessFunction<String, UserActionEntry, CaptureAction, String> {

    private transient MapStateDescriptor<Void, CaptureAction> patternDescriptor;

    @Override
    public void open(Configuration parameters) throws Exception {
      patternDescriptor = new MapStateDescriptor<>("ActionBroadcastState", Types.VOID,
          TypeInformation.of(CaptureAction.class));
    }

    @Override
    public void processElement(UserActionEntry value, ReadOnlyContext ctx, Collector<String> out)
        throws Exception {
      CaptureAction action = ctx.getBroadcastState(patternDescriptor).get(null);
      if (action != null) {
        if (value.getAction().equals(action.getAction())) {
          StringBuilder sb = new StringBuilder();
          sb.append("version: ").append(action.getVersion()).append("\t");
          sb.append("captureAction: ").append(action.getAction()).append("\t");
          sb.append("uid: ").append(value.getUid()).append("\t");
          sb.append("action: ").append(value.getAction()).append("\t");
          sb.append("timestamp: ").append(value.getTimestamp());
          out.collect(sb.toString());
        }
      } else {
        // TODO
      }
    }

    @Override
    public void processBroadcastElement(CaptureAction value, Context ctx, Collector<String> out)
        throws Exception {
      BroadcastState<Void, CaptureAction> broadcastState = ctx.getBroadcastState(patternDescriptor);
      broadcastState.put(null, value);
    }

  }

  public static void main(String[] args) throws Exception {

    String[] actions = new String[] {"Login", "Click", "BUY", "Pay", "Exit"};

    /*
     * 筛选用户行为, 两种数据流
     *
     * 1: 用户行为数据流
     *
     *    user_id, action, timestamp
     *
     * 2: 行为控制流
     *
     *    version, action
     *
     * 基于BroadcastState实现动态筛选用户行为
     * **/
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<UserActionEntry> actionStream = env.addSource(new UserActionSourceFunction(actions))
        .returns(UserActionEntry.class);

    KeySelector<UserActionEntry, String> keySelector = UserActionEntry::getUid;
    DataStream<UserActionEntry> userActionStream = actionStream.keyBy(keySelector);


    DataStream<CaptureAction> captureActionStream = env.addSource(new DynamicActionPatternSourceFunction(2000, actions))
        .returns(CaptureAction.class);

    MapStateDescriptor<Void, CaptureAction> broadStateDescriptor = new MapStateDescriptor<>(
        "ActionBroadcastState", Types.VOID, TypeInformation.of(CaptureAction.class));

    BroadcastStream<CaptureAction> broadcastStream = captureActionStream.broadcast(broadStateDescriptor);

    DataStream<String> actionResultStream = userActionStream.connect(broadcastStream)
        .process(new UserActionCaptureProcessFunction());

    actionResultStream.addSink(new PrintSinkFunction<>());

    env.execute("BroadcastStateBootstrap");
  }

}
