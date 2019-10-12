package com.sdu.flink.sql.dynamic;

import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.CompositeDRow;

public class SchemaSourceFunction implements SourceFunction<CompositeDRow> {

  private final long interval;
  private boolean running;

  private long lastCheckTimestamp;

  public SchemaSourceFunction(long interval) {
    this.interval = interval;
    this.running = true;
    this.lastCheckTimestamp = 0;
  }

  @Override
  public void run(SourceContext<CompositeDRow> ctx) throws Exception {
    // TODO: 测试
    String[] fields = new String[] {"uid", "action", "timestamp"};
    int end = -1;

    while (running) {
      boolean shouldUpdate = System.currentTimeMillis() - lastCheckTimestamp >= interval;
      if (shouldUpdate) {
        end += 1;
        int len = end % fields.length;
        String[] selectFields = new String[len + 1];
        for (int i = 0; i <= len; ++i) {
          selectFields[i] = fields[i];
        }
        ctx.collect(CompositeDRow.ofSchema(selectFields));
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
