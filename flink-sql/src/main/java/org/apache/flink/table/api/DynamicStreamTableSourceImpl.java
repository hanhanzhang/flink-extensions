package org.apache.flink.table.api;


import com.sdu.flink.utils.SqlUtils;
import java.util.Collections;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.SimpleSqlElement;

/**
 * {@link StreamTableSourceScan}
 *
 * @author hanhan.zhang
 * */
public class DynamicStreamTableSourceImpl<T> implements DynamicStreamTableSource, DefinedRowtimeAttributes {

  private final TableSchema tableSchema;

  private final SourceFunction<T> sourceFunction;
  private final MapFunction<T, SimpleSqlElement> mapFunction;
  private final SourceFunction<SimpleSqlElement> schemaCheckFunction;

  private final String eventTimeName;
  private final long lateness;

  public DynamicStreamTableSourceImpl(
      SourceFunction<T> sourceFunction, MapFunction<T, SimpleSqlElement> mapFunction, TableSchema tableSchema,
      SourceFunction<SimpleSqlElement> schemaCheckFunction,
      String eventTimeName, long lateness) {
    this.sourceFunction = sourceFunction;
    this.mapFunction = mapFunction;
    this.tableSchema = tableSchema;
    this.schemaCheckFunction = schemaCheckFunction;
    this.eventTimeName = eventTimeName;
    this.lateness = lateness;
  }

  @Override
  public BroadcastStream<SimpleSqlElement> getBroadcastStream(StreamExecutionEnvironment execEnv) {
    DataStream<SimpleSqlElement> schemaUpdateStream = execEnv.addSource(schemaCheckFunction);
    MapStateDescriptor<Void, SimpleSqlElement> broadcastStateDescriptor = new MapStateDescriptor<>(
        "BroadcastSqlProjectSchemaState", Types.VOID, TypeInformation.of(SimpleSqlElement.class));
    return schemaUpdateStream.broadcast(broadcastStateDescriptor);
  }

  @Override
  public DataStream<SimpleSqlElement> getDataStream(StreamExecutionEnvironment execEnv) {
    // TODO: 并发度
    return execEnv.addSource(sourceFunction)
        .map(mapFunction)
        .returns(TypeInformation.of(SimpleSqlElement.class))
        .setParallelism(8);
  }

  @Override
  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
    /*
     * 源头设置水位线, 仅支持单个字段.
     * **/
    if (eventTimeName == null || eventTimeName.isEmpty()) {
      return Collections.emptyList();
    }

    RowtimeAttributeDescriptor descriptor = new RowtimeAttributeDescriptor(
        eventTimeName, new ExistingField(eventTimeName), new BoundedOutOfOrderTimestamps(lateness));

    return Collections.singletonList(descriptor);
  }

  @Override
  public TableSchema getTableSchema() {
    return tableSchema;
  }

  @Override
  public DataType getProducedDataType() {
    return SqlUtils.fromTableSchema(tableSchema);
  }
}
