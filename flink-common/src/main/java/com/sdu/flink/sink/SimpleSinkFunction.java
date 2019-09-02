package com.sdu.flink.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.types.Row;

/**
 * @author hanhan.zhang
 * */
public class SimpleSinkFunction implements SinkFunction<Row> {

	final String[] columnNames;

	public SimpleSinkFunction(String[] columnNames) {
		this.columnNames = columnNames;
	}

	@Override
	public void invoke(Row value, Context context) throws Exception {
		if (columnNames.length != value.getArity()) {
			throw new RuntimeException("row arity length is not equals column length .");
		}

		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for (int i = 0; i < columnNames.length; ++i) {
			if (i == 0) {
				sb.append(columnNames[i]).append(" = ").append(value.getField(i));
			} else {
				sb.append(", ");
				sb.append(columnNames[i]).append(" = ").append(value.getField(i));
			}
		}
		sb.append("]");

		System.out.println("result: " + sb.toString());
	}
}
