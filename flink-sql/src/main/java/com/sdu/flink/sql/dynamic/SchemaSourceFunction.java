package com.sdu.flink.sql.dynamic;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.DProjectSchemaData;
import org.apache.flink.types.DSchemaTuple;

public class SchemaSourceFunction implements SourceFunction<DSchemaTuple> {

  private final long interval;
  private boolean running;

  private long lastCheckTimestamp;

  public SchemaSourceFunction(long interval) {
    this.interval = interval;
    this.running = true;
    this.lastCheckTimestamp = 0;
  }

  @Override
  public void run(SourceContext<DSchemaTuple> ctx) throws Exception {
    // TODO: 解析SQL
    String[] fieldNames = new String[] {"uid", "action", "timestamp"};
    String[] fieldTypes = new String[] {"String", "String", "String"};
    int end = -1;

    while (running) {
      boolean shouldUpdate = System.currentTimeMillis() - lastCheckTimestamp >= interval;
      if (shouldUpdate) {
        end += 1;
        int len = end % fieldNames.length;

        DSchemaTuple schemaTuple = new DSchemaTuple();

        Map<String, String> nameToTypes = new HashMap<>();
        for (int i = 0; i <= len; ++i) {
          nameToTypes.put(fieldNames[i], fieldTypes[i]);
        }

        schemaTuple.addProjectSchema(new DProjectSchemaData(nameToTypes));

        ctx.collect(schemaTuple);

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
