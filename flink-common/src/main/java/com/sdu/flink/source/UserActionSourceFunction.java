package com.sdu.flink.source;

import com.sdu.flink.utils.UserActionEntry;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *
 * @author hanhan.zhang
 * **/
public class UserActionSourceFunction extends RichParallelSourceFunction<UserActionEntry> {

  private final String[] actions;
  private volatile boolean running;

  public UserActionSourceFunction(String[] actions) {
    this.actions = actions;
    running = true;
  }

  @Override
  public void run(SourceContext<UserActionEntry> ctx) throws Exception {
    Random random = new Random();
    int count = 0;
    while (running) {
      String uid = UUID.randomUUID().toString();
//      int age = random.nextInt(100);
      int age = 10;
      boolean isForeigners = count++ % 2 == 0;
      String action = actions[random.nextInt(actions.length)];
      long timestamp = System.currentTimeMillis();
      ctx.collectWithTimestamp(new UserActionEntry(uid, age, isForeigners, action, timestamp), timestamp);

      safeSleep();
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(1000);
    } catch (Exception e) {
      // ignore
    }
  }

}
