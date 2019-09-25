package com.sdu.flink.sql.source;

import static org.apache.flink.configuration.DSqlOptions.BROADCAST_STATE_NAME;
import static org.apache.flink.configuration.DSqlOptions.DYNAMIC_SQL_CHECK_INTERVAL;
import static org.apache.flink.configuration.DSqlOptions.DYNAMIC_SQL_ENABLE;

import com.sdu.flink.utils.SqlUtils;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.dynamic.DProjectSchema;
import org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * {@link StreamTableSourceScan}
 *
 * @author hanhan.zhang
 * */
public class DynamicStreamTableSource<T> implements StreamTableSource<Row>, DefinedRowtimeAttributes {

  // 数据流全部数据
  private final SourceFunction<T> sourceFunction;
  private final TableSchema tableSchema;
  private final Map<String, Integer> nameToIndex;
  private final MapFunction<T, Row> mapFunction;

  // 筛选的数据
  private final String[] selectNames;

  private final String eventTimeName;
  private final long lateness;



  public DynamicStreamTableSource(
      SourceFunction<T> sourceFunction, MapFunction<T, Row> mapFunction, TableSchema tableSchema,
      String[] selectNames, String eventTimeName, long lateness) {
    this.sourceFunction = sourceFunction;
    this.mapFunction = mapFunction;
    this.tableSchema = tableSchema;

    nameToIndex = new HashMap<>();
    int i = 0;
    for (String name : tableSchema.getFieldNames()) {
      nameToIndex.put(name, i++);
    }

    this.selectNames = selectNames;
    this.eventTimeName = eventTimeName;
    this.lateness = lateness;
  }

  @Override
  public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
    // TODO: 并发度

    Map<String, String> globalParams = execEnv.getConfig().getGlobalJobParameters().toMap();
    String dynamicEnable = globalParams.get(DYNAMIC_SQL_ENABLE);

    // 禁用动态更新SQL
    if (dynamicEnable == null || dynamicEnable.isEmpty() || !Boolean.parseBoolean(dynamicEnable)) {
      return execEnv.addSource(sourceFunction)
          .map(mapFunction)
          .map(new SelectMapFunction(nameToIndex, selectNames)).returns(SqlUtils.createRowType(tableSchema, nameToIndex, selectNames));
    }

    // 启动动态更新
    long interval = Long.parseLong(globalParams.getOrDefault(DYNAMIC_SQL_CHECK_INTERVAL, "300000"));
    DataStream<DProjectSchema> projectStream = execEnv.addSource(new ProjectCheckSourceFunction(interval));
    MapStateDescriptor<Void, DProjectSchema> broadcastStateDescriptor = new MapStateDescriptor<>(
        BROADCAST_STATE_NAME, Types.VOID, TypeInformation.of(DProjectSchema.class));
    BroadcastStream<DProjectSchema> broadcastStream = projectStream.broadcast(broadcastStateDescriptor);

    DataStream<Row> sourceStream = execEnv.addSource(sourceFunction).map(mapFunction);
    return sourceStream.connect(broadcastStream)
        .process(new ProjectUpdaterProcessFunction(selectNames, nameToIndex))
        // TODO: TypeInformationInfo
        .returns(SqlUtils.createDynamicRowType(tableSchema, nameToIndex, selectNames));
  }

  @Override
  public TableSchema getTableSchema() {
    return SqlUtils.createTableSchema(tableSchema, nameToIndex, selectNames);
  }

  @Override
  public DataType getProducedDataType() {
    return SqlUtils.fromTableSchema(tableSchema, nameToIndex, selectNames);
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

  public static class SelectMapFunction implements MapFunction<Row, Row> {

    private Map<String, Integer> nameToIndex;
    private String[] selectNames;

    SelectMapFunction(Map<String, Integer> nameToIndex, String[] selectNames) {
      this.nameToIndex = nameToIndex;
      this.selectNames = selectNames;
    }

    @Override
    public Row map(Row value) throws Exception {
      Row newRow = new Row(selectNames.length);
      int i = 0;
      for (String name : selectNames) {
        newRow.setField(i++, value.getField(nameToIndex.get(name)));
      }
      return newRow;
    }
  }
}
