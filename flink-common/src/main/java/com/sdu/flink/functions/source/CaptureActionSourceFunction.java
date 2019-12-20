package com.sdu.flink.functions.source;

import static com.sdu.flink.utils.FakeDataUtils.buildAction;

import com.sdu.flink.entry.CaptureActionEntry;
import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class CaptureActionSourceFunction extends RichSourceFunction<CaptureActionEntry> {

  private final long interval;

  private volatile boolean running;
  private long lastUpdateTimestamp;
  private int version;

  public CaptureActionSourceFunction(long interval) {
    this.interval = interval;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
    this.version = 1;
    this.lastUpdateTimestamp = 0L;
  }

  @Override
  public void run(SourceContext<CaptureActionEntry> ctx) throws Exception {
    while (running) {
      boolean update = System.currentTimeMillis() - lastUpdateTimestamp >= interval;
      if (update) {
        CaptureActionEntry entry = new CaptureActionEntry();
        entry.setVersion(version++);
        entry.setAction(buildAction());
        ctx.collect(entry);
        lastUpdateTimestamp = System.currentTimeMillis();
        safeSleep(System.currentTimeMillis() - lastUpdateTimestamp);
      }
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
      // ignore
    }
  }

}
