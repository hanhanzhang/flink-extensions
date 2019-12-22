package com.sdu.flink.stream;

import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.functions.source.FixedUserActionSourceFunction;
import com.sdu.flink.stream.functions.UserActionReduceFunction;
import com.sdu.flink.stream.functions.UserActionStreamAssignWatermarks;
import com.sdu.flink.stream.functions.windowing.UserActionProcessWindowFunction;
import com.sdu.flink.stream.functions.windowing.triggers.UserActionEventTimeTrigger;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowStreamBootstrap {

  public static void main(String[] args) throws Exception {
    /*
     * 统计在每分钟内的用户行为数
     * */

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    /*
     * Watermark发送有两种方式:
     *
     * 1: 数据流源头统一向下发送(SourceContext)
     *
     * 2: Operator分配水位线
     * */
    DataStream<UserActionEntry> source = env.addSource(new FixedUserActionSourceFunction(20))
        .assignTimestampsAndWatermarks(new UserActionStreamAssignWatermarks());

    KeySelector<UserActionEntry, Tuple2<String, String>> selector = (UserActionEntry entry) -> {
      String uname = entry.getUname();
      String action = entry.getAction();
      return Tuple2.of(uname, action);
    };

    DataStream<Tuple4<String, String, Long, Integer>> userActionCntStream = source.keyBy(selector, Types.TUPLE(Types.STRING, Types.STRING))
        .timeWindow(Time.seconds(10))
        .allowedLateness(Time.seconds(5))
        .trigger(new UserActionEventTimeTrigger(5 * 1000L))
        /*
         * WindowedSteam窗口元素处理策略有两种方式:
         *
         * 1: 逐条处理
         *
         *    aggregate()
         *
         * 2: 全量处理
         *
         *    apply() / process()
         * */
        .aggregate(new UserActionReduceFunction(), new UserActionProcessWindowFunction());

    userActionCntStream.addSink(new PrintSinkFunction<>());

    env.execute("WindowStreamBootstrap");
  }

}
