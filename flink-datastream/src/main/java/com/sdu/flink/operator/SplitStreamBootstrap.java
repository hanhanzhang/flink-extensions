package com.sdu.flink.operator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * @author hanhan.zhang
 */
public class SplitStreamBootstrap {


  private static List<Tuple3<String, String, Float>> DATA_SOURCES;

  static {
    DATA_SOURCES = new ArrayList<>();

    DATA_SOURCES.add(Tuple3.of("李明", "语文", 83.5f));
    DATA_SOURCES.add(Tuple3.of("王龙", "语文", 65.5f));
    DATA_SOURCES.add(Tuple3.of("史明", "语文", 87.5f));
    DATA_SOURCES.add(Tuple3.of("龚兵", "语文", 99.5f));

    DATA_SOURCES.add(Tuple3.of("李明", "数学", 90.5f));
    DATA_SOURCES.add(Tuple3.of("王龙", "数学", 68.5f));
    DATA_SOURCES.add(Tuple3.of("史明", "数学", 91.5f));
    DATA_SOURCES.add(Tuple3.of("龚兵", "数学", 96.5f));
  }

  public static class SimpleSink implements SinkFunction<Tuple3<String, String, Float>> {

    private final String subject;

    SimpleSink(String subject) {
      this.subject = subject;
    }

    @Override
    public void invoke(Tuple3<String, String, Float> value, Context context) throws Exception {
      assert value.f1.equals(subject);

      System.out.println("姓名: " + value.f0 + ", 学科: " + value.f1 + ", 成绩: " + value.f2);
    }

  }


  public static void main(String[] args) throws Exception {

    // 拆分语文、数学数据流

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<Tuple3<String, String, Float>> sourceStream = env.fromCollection(DATA_SOURCES);

    // 按照学科拆分数据流
    SplitStream<Tuple3<String, String, Float>> subjectStream = sourceStream.split(
        (Tuple3<String, String, Float> t) -> {
          if (t.f1.equals("语文")) {
            return Collections.singletonList("chinese");
          } else {
            return Collections.singletonList("math");
          }
        }
    );
    DataStream<Tuple3<String, String, Float>> chineseStream = subjectStream.select("chinese");
    DataStream<Tuple3<String, String, Float>> mathStream = subjectStream.select("math");

    //
    chineseStream.addSink(new SimpleSink("语文"));
    mathStream.addSink(new SimpleSink("数学"));

    env.execute("SplitStreamBootstrap");

  }

}
