package com.sdu.blink.sql.sink;

import com.sdu.blink.sql.SqlUtils;
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
public class SimplePrintAppendSink implements AppendStreamTableSink<Row> {

	final TableSchema tableSchema;

	public SimplePrintAppendSink(TableSchema tableSchema) {
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataType getConsumedDataType() {
		return SqlUtils.fromTableSchema(tableSchema);
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		// Blink 使用 consumeDataStream() 消费数据, 故该接口不实现
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
		return new SimpleDataStreamSink(dataStream, tableSchema.getFieldNames());
	}


	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return this;
	}


}
