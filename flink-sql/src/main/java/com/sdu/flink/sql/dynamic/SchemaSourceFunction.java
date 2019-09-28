package com.sdu.flink.sql.dynamic;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.SimpleSqlElement;

public class SchemaSourceFunction implements SourceFunction<SimpleSqlElement> {

  private final long interval;

  public SchemaSourceFunction(long interval) {
    this.interval = interval;
  }

  @Override
  public void run(SourceContext<SimpleSqlElement> ctx) throws Exception {

  }

  @Override
  public void cancel() {

  }

}
