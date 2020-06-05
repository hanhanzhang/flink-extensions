package org.apache.flink.table;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.DataTypes.Field;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class DynamicStreamDataSource implements StreamTableSource<Row>, DynamicDataTypeConverter {

  private final SourceFunction<Row> sourceFunction;
  private final Map<String, String> sourceProps;

  DynamicStreamDataSource(SourceFunction<Row> sourceFunction, Map<String, String> sourceProps) {
    this.sourceFunction = requireNonNull(sourceFunction);
    this.sourceProps = requireNonNull(sourceProps);
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
    Tuple2<String[], TypeInformation<?>[]> nameTypes = toNameAndTypes(sourceProps);
    return execEnv.addSource(sourceFunction)
        .returns(new RowTypeInfo(nameTypes.f1, nameTypes.f0));
  }

  @Override
  public TableSchema getTableSchema() {
    return toTableSchema(sourceProps);
  }

  @Override
  public DataType getProducedDataType() {
    TableSchema tableSchema = getTableSchema();
    Field[] physicalColumns = tableSchema.getTableColumns()
        .stream()
        .filter(column -> !column.getExpr().isPresent())
        .map(column -> DataTypes.FIELD(column.getName(), column.getType()))
        .toArray(Field[]::new);
    return DataTypes.ROW(physicalColumns);
  }

}
