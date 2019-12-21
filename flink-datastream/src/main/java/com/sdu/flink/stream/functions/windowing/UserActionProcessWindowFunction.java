package com.sdu.flink.stream.functions.windowing;

import java.util.Iterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserActionProcessWindowFunction
    extends ProcessWindowFunction<Integer, Tuple4<String, String, Long, Integer>, Tuple2<String, String>, TimeWindow> {

  @Override
  public void process(Tuple2<String, String> key, Context context,
      Iterable<Integer> elements, Collector<Tuple4<String, String, Long, Integer>> out)
      throws Exception {
    Iterator<Integer> iterator = elements.iterator();
    while (iterator.hasNext()) {
      // 只有一个元素
      long windowStart = context.window().getStart();
      int userActionCnt = iterator.next();
      Tuple4<String, String, Long, Integer> output = Tuple4.of(
          key.f0, key.f1, windowStart, userActionCnt);
      out.collect(output);
    }
  }
}
