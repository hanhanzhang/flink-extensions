package com.sdu.flink.sql.dynamic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.DynamicAppendStreamTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.SimpleSqlElement;

public class SimplePrintDynamicTableSink implements DynamicAppendStreamTableSink {

  private final TableSchema tableSchema;

  public SimplePrintDynamicTableSink(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<SimpleSqlElement> dataStream) {
    return null;
  }


  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }
}
