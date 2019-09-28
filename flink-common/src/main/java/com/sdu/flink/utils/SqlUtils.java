package com.sdu.flink.utils;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchema.Builder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * @author hanhan.zhang
 */
public class SqlUtils {

  private SqlUtils() {
  }

  public static TypeInformation<Row> createRowType(TableSchema tableSchema, Map<String, Integer> nameToIndex,
      String[] selectNames) {
    return createTableSchema(tableSchema, nameToIndex, selectNames).toRowType();
  }

  public static TableSchema createTableSchema(TableSchema tableSchema, Map<String, Integer> nameToIndex,
      String[] selectNames) {
    Builder builder = new TableSchema.Builder();
    for (String name : selectNames) {
      builder.field(name, tableSchema.getFieldDataType(nameToIndex.get(name)).orElseThrow(
          () -> new IllegalStateException("Can't find DataType for column: " + name)));
    }

    return builder.build();
  }

  public static DataType fromTableSchema(TableSchema tableSchema, Map<String, Integer> nameToIndex,
      String[] selectNames) {
    DataTypes.Field[] fields = new DataTypes.Field[selectNames.length];
    for (int i = 0; i < selectNames.length; ++i) {
      String name = selectNames[i];
      fields[i] = DataTypes.FIELD(name, tableSchema.getFieldDataType(nameToIndex.get(name)).orElseThrow(
          () -> new IllegalStateException("Can't find DataType for column: " + name)));
    }

    return DataTypes.ROW(fields);
  }

  public static DataType fromTableSchema(TableSchema tableSchema) {
    DataTypes.Field[] fields = new DataTypes.Field[tableSchema.getFieldCount()];
    for (int i = 0; i < tableSchema.getFieldCount(); ++i) {
      fields[i] = DataTypes.FIELD(tableSchema.getFieldNames()[i],
          tableSchema.getFieldDataTypes()[i]);
    }
    return DataTypes.ROW(fields);
  }
}
