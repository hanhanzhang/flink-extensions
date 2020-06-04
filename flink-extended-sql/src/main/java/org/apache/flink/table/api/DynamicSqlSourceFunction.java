package org.apache.flink.table.api;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.types.SqlSchemaTuple;

public class DynamicSqlSourceFunction extends RichParallelSourceFunction<SqlSchemaTuple> {

  @Override
  public void run(SourceContext<SqlSchemaTuple> ctx) throws Exception {

  }

  @Override
  public void cancel() {

  }

}
