package org.apache.flink.table;

import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.RandomUtils.nextLong;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

public class DynamicSourceFunction extends RichParallelSourceFunction<Row> {

  private transient List<Tuple3<String, String, Integer>> peoples;
  private transient List<String> actions;

  private transient boolean running;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
    this.peoples = new ArrayList<>();
    this.actions = new ArrayList<>();

    this.peoples.add(Tuple3.of("张小龙", "男", 20));
    this.peoples.add(Tuple3.of("王小丽", "女", 21));
    this.peoples.add(Tuple3.of("李连杰", "男", 22));
    this.peoples.add(Tuple3.of("刘丽丽", "女", 23));
    this.peoples.add(Tuple3.of("刘萌萌", "女", 24));
    this.peoples.add(Tuple3.of("李明明", "男", 25));
    this.peoples.add(Tuple3.of("孙梦莎", "女", 25));
    this.peoples.add(Tuple3.of("孙虎龙", "男", 27));
    this.peoples.add(Tuple3.of("孙珊珊", "女", 28));

    this.actions.add("登录");
    this.actions.add("搜索");
    this.actions.add("点击");
    this.actions.add("下单");
    this.actions.add("支付");
    this.actions.add("退出");
  }

  @Override
  public void run(SourceContext<Row> ctx) throws Exception {
    while (running) {
      long uid = nextLong(1, 10);
      Tuple3<String, String, Integer> people = peoples.get(nextInt(0, peoples.size()));
      String action = actions.get(nextInt(0, actions.size()));

      Row out = Row.of(
          uid,
          people.f0,
          people.f1,
          people.f2,
          action,
          System.currentTimeMillis()
      );

      ctx.collect(out);

      safeSleep();
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (Exception e) {
      // ignore
    }
  }
}
