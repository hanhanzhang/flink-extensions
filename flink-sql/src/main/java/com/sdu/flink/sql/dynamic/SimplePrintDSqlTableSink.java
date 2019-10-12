package com.sdu.flink.sql.dynamic;

import java.util.Map;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.table.DSqlAppendStreamTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.CompositeDRow;

public class SimplePrintDSqlTableSink implements DSqlAppendStreamTableSink {

  private final TableSchema tableSchema;

  public SimplePrintDSqlTableSink(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<CompositeDRow> dataStream) {
    return new SimpleDataStreamSink(dataStream);
  }


  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public static class SimpleDataStreamSink extends DataStreamSink<CompositeDRow> {

    public SimpleDataStreamSink(DataStream<CompositeDRow> inputStream) {
      super(inputStream, new StreamSink<>(new SimpleStreamSink()));
    }

  }

  public static class SimpleStreamSink implements SinkFunction<CompositeDRow> {

    @Override
    public void invoke(CompositeDRow value, Context context) throws Exception {
      if (value.isRow()) {
        Map<String, String> fieldValues = value.getFieldValues();
        System.out.println("Received: " + fieldValues);
      }
    }

  }

}
