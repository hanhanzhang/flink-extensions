package org.apache.flink.table.updater;

import static org.apache.flink.types.DSqlTypeUtils.javaTypeToDataType;

import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.DStreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DSqlTypeUtils;

@Internal
public class DFakeStreamTableTableSource implements DStreamTableSource {

  private TableSchema tableSchema;

  DFakeStreamTableTableSource(List<DColumnStatement> columnStatements) {
    TableSchema.Builder builder = TableSchema.builder();
    for (DColumnStatement columnStatement : columnStatements) {
      DataType dataType = javaTypeToDataType(columnStatement.getType());
      builder.field(columnStatement.getName(), dataType);
    }
    tableSchema = builder.build();
  }

  @Override
  public BroadcastStream<DSchemaTuple> getBroadcastStream(StreamExecutionEnvironment execEnv) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public DataStream<DRecordTuple> getDataStream(StreamExecutionEnvironment execEnv) {
    throw new UnsupportedOperationException("");
  }

  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  @Override
  public DataType getProducedDataType() {
    return DSqlTypeUtils.fromTableSchema(tableSchema);
  }

}
