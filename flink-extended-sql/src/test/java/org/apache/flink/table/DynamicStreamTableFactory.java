package org.apache.flink.table;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

public class DynamicStreamTableFactory implements TableSourceFactory<Row>, TableSinkFactory, DynamicDataTypeConverter {

  private static final String STREAM_CONNECTOR_TYPE = "connector.type";

  @Override
  public TableSource<Row> createTableSource(TableSourceFactory.Context context) {
    CatalogTable table = context.getTable();
    Map<String, String> properties = table.toProperties();
    String connectorType = properties.get(STREAM_CONNECTOR_TYPE);
    if (StringUtils.isEmpty(connectorType)) {
      throw new TableException("Missing property: " + STREAM_CONNECTOR_TYPE);
    }

    SourceFunction<Row> sourceFunction = new DynamicSourceFunction();
    return new DynamicStreamDataSource(sourceFunction, table.toProperties());
  }

  @Override
  public TableSink<?> createTableSink(TableSinkFactory.Context context) {
    CatalogTable table = context.getTable();
    Map<String, String> properties = table.toProperties();
    String connectorType = properties.get(STREAM_CONNECTOR_TYPE);
    if (StringUtils.isEmpty(connectorType)) {
      throw new TableException("Missing property: " + STREAM_CONNECTOR_TYPE);
    }

    Tuple2<String[], TypeInformation<?>[]> nameAndTypes = toNameAndTypes(properties);
    return new DynamicConsoleAppendSink(nameAndTypes.f0, nameAndTypes.f1);
  }

  @Override
  public Map<String, String> requiredContext() {
    return Collections.emptyMap();
  }


  @Override
  public List<String> supportedProperties() {
    return Collections.singletonList(STREAM_CONNECTOR_TYPE);
  }
}
