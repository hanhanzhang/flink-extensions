package com.sdu.flink.functions.source;

import static com.sdu.flink.utils.FakeDataUtils.buildAction;
import static com.sdu.flink.utils.FakeDataUtils.buildNameAndSex;
import static com.sdu.flink.utils.RandomUtils.nextInt;

import com.sdu.flink.entry.UserActionEntry;
import com.sdu.flink.utils.RandomUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 *
 * @author hanhan.zhang
 * **/
public class FixedUserActionSourceFunction extends RichParallelSourceFunction<UserActionEntry> {

  private final int length;

  private volatile boolean running;
  private transient Map<Integer, UserActionEntry> userActionEntries;

  public FixedUserActionSourceFunction(int length) {
    this.length = length;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
    this.userActionEntries = new HashMap<>();
  }

  @Override
  public void run(SourceContext<UserActionEntry> ctx) throws Exception {
    while (running) {
      int uid = RandomUtils.nextInt(length, 10000);
      UserActionEntry actionEntry = userActionEntries.get(uid);
      if (actionEntry == null) {
        actionEntry = new UserActionEntry();
        actionEntry.setUid(uid);

        Pair<String, String> nameAndSex = buildNameAndSex();
        actionEntry.setUname(nameAndSex.getLeft());
        actionEntry.setSex(nameAndSex.getRight());

        actionEntry.setAge(nextInt(50, 15));
        actionEntry.setAction(buildAction());
        actionEntry.setTimestamp(System.currentTimeMillis());

        userActionEntries.put(uid, actionEntry);
      }
      ctx.collectWithTimestamp(actionEntry, actionEntry.getTimestamp());
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
