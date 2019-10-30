package com.sdu.flink.sql.dynamic;

import com.sdu.flink.utils.UserActionEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.table.api.DRecordTupleFactory;
import org.apache.flink.table.api.DStreamSourceFunction;
import org.apache.flink.types.DRecordTuple;

/**
 *
 * @author hanhan.zhang
 * **/
public class DUserActionSourceFunction extends DStreamSourceFunction<UserActionEntry> {

  private final String[] actions;
  private volatile boolean running;

  DUserActionSourceFunction(String[] actions) {
    this.actions = actions;
    running = true;
  }

  @Override
  public void run(SourceContext<UserActionEntry> ctx) throws Exception {
    Random random = new Random();
    int count = 0;
    while (running) {
      String uid = UUID.randomUUID().toString();
      // 10 - 15
      int age = random.nextInt(6) + 10;
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

  @Override
  public DRecordTupleFactory<UserActionEntry> getRecordTupleFactory() {
    return (UserActionEntry entry) -> {
      Map<String, String> recordValues = new HashMap<>();
      recordValues.put("uid", entry.getUid());
      recordValues.put("age", String.valueOf(entry.getAge()));
      recordValues.put("isForeigners", String.valueOf(entry.isForeigners()));
      recordValues.put("action", entry.getAction());
      recordValues.put("timestamp", String.valueOf(entry.getTimestamp()));

      Map<String, String> recordTypes = new HashMap<>();
      recordTypes.put("uid", "String");
      recordTypes.put("age", "Integer");
      recordTypes.put("isForeigners", "Boolean");
      recordTypes.put("action", "String");
      recordTypes.put("timestamp", "Long");

      return new DRecordTuple(recordTypes, recordValues);
    };
  }

  @Override
  public int getStreamParallelism() {
    return 8;
  }
}
