package com.sdu.flink.sql.source;

import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.dynamic.DProjectSchema;

/**
 * Select a, b, c From table_a;
 *
 * Select a, b, c, d From table_a;
 *
 * @author hanhan.zhang
 * **/
public class ProjectCheckSourceFunction implements SourceFunction<DProjectSchema> {

  private long checkInterval;
  private long lastCheckTimestamp;

  private boolean running;

  public ProjectCheckSourceFunction(long checkInterval) {
    this.checkInterval = checkInterval;
    this.lastCheckTimestamp = 0;
    this.running = true;
  }

  @Override
  public void run(SourceContext<DProjectSchema> ctx) throws Exception {
    while (running) {
      boolean shouldCheck = System.currentTimeMillis() - lastCheckTimestamp >= checkInterval;
      if (shouldCheck) {
        lastCheckTimestamp = System.currentTimeMillis();

      }

      saftSleep(System.currentTimeMillis() - lastCheckTimestamp);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  private static void saftSleep(long millSeconds) {
    try {
      TimeUnit.MILLISECONDS.sleep(millSeconds);
    } catch (Exception e) {
      // ignore
    }
  }

}
