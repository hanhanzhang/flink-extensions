package com.sdu.flink.sink;

import com.sdu.flink.functions.sink.ConsoleOutputSinkFunction;
import com.sdu.flink.utils.SqlTypeUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author hanhan.zhang
 * */
public class ConsoleOutputAppendStreamTableSink implements AppendStreamTableSink<Row> {

	final TableSchema tableSchema;

	public ConsoleOutputAppendStreamTableSink(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataType getConsumedDataType() {
		return SqlTypeUtils.fromTableSchema(tableSchema);
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		// Blink 使用 consumeDataStream() 消费数据, 故该接口不实现
		dataStream.addSink(new ConsoleOutputSinkFunction(tableSchema.getFieldNames()));
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return new ConsoleOutputDataStreamSink(dataStream, tableSchema.getFieldNames());
	}


	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return this;
	}


}
