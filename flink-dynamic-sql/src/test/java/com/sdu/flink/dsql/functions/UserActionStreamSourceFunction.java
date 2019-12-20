package com.sdu.flink.dsql.functions;

import static com.sdu.flink.utils.FakeDataUtils.buildAction;
import static com.sdu.flink.utils.FakeDataUtils.buildNameAndSex;

import com.sdu.flink.utils.RandomUtils;
import com.sdu.flink.entry.UserActionEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DRecordTupleFactory;
import org.apache.flink.table.api.DStreamSourceFunction;
import org.apache.flink.types.DRecordTuple;

public class UserActionStreamSourceFunction extends DStreamSourceFunction<UserActionEntry> {

  private volatile boolean running;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
  }

  @Override
  public DRecordTupleFactory<UserActionEntry> getRecordTupleFactory() {
    return (UserActionEntry entry) -> {
      Map<String, String> recordValues = new HashMap<>();
      recordValues.put("uid", entry.getUid());
      recordValues.put("uname", entry.getUname());
      recordValues.put("sex", entry.getSex());
      recordValues.put("age", String.valueOf(entry.getAge()));
      recordValues.put("action", entry.getAction());
      recordValues.put("timestamp", String.valueOf(entry.getTimestamp()));

      Map<String, String> recordTypes = new HashMap<>();
      recordTypes.put("uid", "String");
      recordTypes.put("uname", "String");
      recordTypes.put("sex", "String");
      recordTypes.put("age", "Integer");
      recordTypes.put("action", "String");
      recordTypes.put("timestamp", "Long");

      return new DRecordTuple(recordTypes, recordValues);
    };
  }

  @Override
  public int getStreamParallelism() {
    return 8;
  }

  @Override
  public void run(SourceContext<UserActionEntry> ctx) throws Exception {
    while (running) {
      Pair<String, String> nameAndSex = buildNameAndSex();
      // 10 - 15
      int age = RandomUtils.nextInt(6, 10);

      UserActionEntry entry = new UserActionEntry();
      entry.setUid(UUID.randomUUID().toString());
      entry.setUname(nameAndSex.getLeft());
      entry.setSex(nameAndSex.getRight());
      entry.setAge(age);
      entry.setAction(buildAction());
      entry.setTimestamp(System.currentTimeMillis());

      ctx.collectWithTimestamp(entry, entry.getTimestamp());

      safeSleep();
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.MILLISECONDS.sleep(1000);
    } catch (Exception e) {
      // ignore
    }
  }

}
