package org.apache.flink.table.api;

import java.util.concurrent.TimeUnit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.runtime.DynamicSqlParser;
import org.apache.flink.table.types.SqlSchemaTuple;

public class DynamicSqlMonitorFunction extends RichParallelSourceFunction<SqlSchemaTuple> {

  // TODO: 设计如何给用户开放, 这里仅为测试

  private transient boolean running;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.running = true;
  }

  @Override
  public void run(SourceContext<SqlSchemaTuple> ctx) throws Exception {
    while (running) {
      DynamicSqlParser sqlParser = new DynamicSqlParser();
      SqlSchemaTuple tuple = sqlParser.getNextExecuteSql();
      ctx.collect(tuple);

      safeSleep();
    }
  }

  @Override
  public void cancel() {
    this.running = false;
  }

  private static void safeSleep() {
    try {
      TimeUnit.SECONDS.sleep(10);
    } catch (Exception e) {
      // ignore
    }
  }

}
