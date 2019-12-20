package com.sdu.flink.sink;

import com.sdu.flink.functions.sink.ConsoleOutputSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.types.Row;

class ConsoleOutputDataStreamSink extends DataStreamSink<Row> {

	ConsoleOutputDataStreamSink(DataStream<Row> inputStream, String[] columnNames) {
		super(inputStream, new StreamSink<>(new ConsoleOutputSinkFunction(columnNames)));
	}


}
