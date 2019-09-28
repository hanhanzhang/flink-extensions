package com.sdu.flink.sql.dynamic;

import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.SimpleSqlElement;

public class SchemaSourceFunction implements SourceFunction<SimpleSqlElement> {

  private final long interval;
  private boolean running;

  private long lastCheckTimestamp;

  public SchemaSourceFunction(long interval) {
    this.interval = interval;
    this.running = true;
    this.lastCheckTimestamp = 0;
  }

  @Override
  public void run(SourceContext<SimpleSqlElement> ctx) throws Exception {
    // TODO: 测试
    String[] fields = new String[] {"uid", "action", "timestamp"};
    Random random = new Random();

    while (running) {
      boolean shouldUpdate = System.currentTimeMillis() - lastCheckTimestamp >= interval;
      if (shouldUpdate) {
        int end = random.nextInt(fields.length);
        String[] selectFields = new String[end + 1];
        for (int i = 0; i <= end; ++i) {
          selectFields[i] = fields[i];
        }
        ctx.collect(SimpleSqlElement.ofSchema(selectFields));
        lastCheckTimestamp = System.currentTimeMillis();
      }

      safeSleep(System.currentTimeMillis() - lastCheckTimestamp);
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private static void safeSleep(long millSeconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(millSeconds);
    } catch (Exception e) {
      //
    }
  }
}
