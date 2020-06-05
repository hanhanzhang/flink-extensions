package org.apache.flink.table;

import java.io.Serializable;
import java.util.function.Function;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

public class DynamicConsoleAppendSink implements AppendStreamTableSink<Row> {

  private final String[] fieldNames;
  private final TypeInformation<?>[] fieldTypes;

  DynamicConsoleAppendSink(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public String[] getFieldNames() {
    return fieldNames;
  }

  @Override
  public TypeInformation<?>[] getFieldTypes() {
    return fieldTypes;
  }

  @Override
  public TypeInformation<Row> getOutputType() {
    return new RowTypeInfo(fieldTypes, fieldNames);
  }

  @Override
  public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
    // Blink use the method
    SinkFunction<Row> sink = new DynamicConsoleSinkFunction<>(new ConsolePrintFunction(fieldNames, fieldTypes));
    return dataStream.addSink(sink);
  }

  @Override
  public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
    return null;
  }

  public static class ConsolePrintFunction implements Function<Row, String>, Serializable {

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;

    ConsolePrintFunction(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
      this.fieldNames = fieldNames;
      this.fieldTypes = fieldTypes;
    }

    @Override
    public String apply(Row row) {
      return formatRow(fieldNames, fieldTypes, row);
    }

    static String formatRow(String[] fieldNames, TypeInformation<?>[] fieldTypes, Row row) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < fieldNames.length; ++i) {
        Object value = row.getField(i);
        if (i == 0)
          sb.append(fieldNames[i]).append(" = [").append(value).append("]");
        else
          sb.append(", ").append(fieldNames[i]).append(" = [").append(value).append("]");
      }
      return sb.toString();
    }

  }


}
