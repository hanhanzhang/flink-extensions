package com.sdu.flink.functions.source;

import static com.sdu.flink.utils.FakeDataUtils.buildAction;
import static com.sdu.flink.utils.FakeDataUtils.buildNameAndSex;
import static com.sdu.flink.utils.RandomUtils.nextInt;

import com.sdu.flink.entry.UserActionEntry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *
 * @author hanhan.zhang
 * **/
public class UserActionSourceFunction extends RichParallelSourceFunction<UserActionEntry> {

  private volatile boolean running;

  public UserActionSourceFunction() {
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
  }

  @Override
  public void run(SourceContext<UserActionEntry> ctx) throws Exception {
    while (running) {
      Pair<String, String> nameAndSex = buildNameAndSex();

      UserActionEntry entry = new UserActionEntry();
      entry.setUid(RandomUtils.nextInt(1000, 10000));
      entry.setUname(nameAndSex.getLeft());
      entry.setSex(nameAndSex.getRight());
      entry.setAge(nextInt(50, 15));
      entry.setAction(buildAction());
      entry.setTimestamp(System.currentTimeMillis());

      ctx.collectWithTimestamp(entry, entry.getTimestamp());

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
