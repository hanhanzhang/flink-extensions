package com.sdu.flink.stream;

import com.sdu.flink.entry.CaptureActionEntry;
import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.functions.co.CaptureActionBroadcastProcessFunction;
import com.sdu.flink.functions.sink.ConsoleOutputSinkFunction;
import com.sdu.flink.functions.source.CaptureActionSourceFunction;
import com.sdu.flink.functions.source.UserActionSourceFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 *
 *
 * @author hanhan.zhang
 * **/
public class BroadcastStateBootstrap {


  public static void main(String[] args) throws Exception {

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

    // 用户行为数据流
    DataStream<UserActionEntry> actionStream = env.addSource(new UserActionSourceFunction())
        .returns(UserActionEntry.class);

    // 行为拦截规则流
    KeySelector<UserActionEntry, String> keySelector = UserActionEntry::getUid;
    DataStream<UserActionEntry> userActionStream = actionStream.keyBy(keySelector);
    DataStream<CaptureActionEntry> captureActionStream = env.addSource(new CaptureActionSourceFunction(2000))
        .returns(CaptureActionEntry.class);

    MapStateDescriptor<Void, CaptureActionEntry> broadStateDescriptor = new MapStateDescriptor<>(
        "ActionBroadcastState", Types.VOID, TypeInformation.of(CaptureActionEntry.class));
    BroadcastStream<CaptureActionEntry> broadcastStream = captureActionStream.broadcast(broadStateDescriptor);

    DataStream<Row> actionResultStream = userActionStream.connect(broadcastStream)
        .process(new CaptureActionBroadcastProcessFunction());

    String[] columnNames = new String[] {
        "version", "uname", "sex", "age", "userAction", "captureAction"
    };
    actionResultStream.addSink(new ConsoleOutputSinkFunction(columnNames));

    env.execute("BroadcastStateBootstrap");
  }

}
