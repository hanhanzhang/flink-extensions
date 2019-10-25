package com.sdu.flink.sql.dynamic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.table.DAppendStreamTableSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.DRecordTuple;

public class SimplePrintDTableSink implements DAppendStreamTableSink {

  private final TableSchema tableSchema;

  public SimplePrintDTableSink(TableSchema tableSchema) {
    this.tableSchema = tableSchema;
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<DRecordTuple> dataStream) {
    return new SimpleDataStreamSink(dataStream);
  }


  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  public static class SimpleDataStreamSink extends DataStreamSink<DRecordTuple> {

    public SimpleDataStreamSink(DataStream<DRecordTuple> inputStream) {
      super(inputStream, new StreamSink<>(new SimpleStreamSink()));
    }

  }

  public static class SimpleStreamSink implements SinkFunction<DRecordTuple> {

    @Override
    public void invoke(DRecordTuple recordTuple, Context context) throws Exception {
      System.out.println("Received: " + recordTuple);
    }

  }

}
