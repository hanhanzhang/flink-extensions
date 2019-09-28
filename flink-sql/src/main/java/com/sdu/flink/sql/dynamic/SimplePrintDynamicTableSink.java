package com.sdu.flink.sql.dynamic;

import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
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
    return new SimpleDataStreamSink(dataStream);
  }


  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public static class SimpleDataStreamSink extends DataStreamSink<SimpleSqlElement> {

    public SimpleDataStreamSink(DataStream<SimpleSqlElement> inputStream) {
      super(inputStream, new StreamSink<>(new SimpleStreamSink()));
    }

  }

  public static class SimpleStreamSink implements SinkFunction<SimpleSqlElement> {

    @Override
    public void invoke(SimpleSqlElement value, Context context) throws Exception {
      if (value.isRecord()) {
        Map<String, String> fieldValues = value.getFieldValues();
        System.out.println("Received: " + fieldValues);
      }
    }

  }

}
