package com.sdu.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.types.Row;

public class SimpleDataStreamSink extends DataStreamSink<Row> {

	public SimpleDataStreamSink(DataStream<Row> inputStream, String[] columnNames) {
		super(inputStream, new StreamSink<>(new SimpleSinkFunction(columnNames)));
	}


}
