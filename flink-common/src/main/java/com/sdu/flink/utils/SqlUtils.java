package com.sdu.flink.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

/**
 * @author hanhan.zhang
 * */
public class SqlUtils {

	private SqlUtils() {}


	public static DataType fromTableSchema(TableSchema tableSchema) {
		DataTypes.Field[] fields = new DataTypes.Field[tableSchema.getFieldCount()];
		for (int i = 0; i < tableSchema.getFieldCount(); ++i) {
			fields[i] = DataTypes.FIELD(tableSchema.getFieldNames()[i],
					tableSchema.getFieldDataTypes()[i]);
		}
		return DataTypes.ROW(fields);
	}

}
