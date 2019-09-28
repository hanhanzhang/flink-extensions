package org.apache.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.SimpleSqlElement;

public interface DynamicAppendStreamTableSink extends AppendStreamTableSink<SimpleSqlElement> {

  default void emitDataStream(DataStream<SimpleSqlElement> dataStream) {

  }

  default TableSink<SimpleSqlElement> configure(String[] fieldNames,
      TypeInformation<?>[] fieldTypes) {
    return null;
  }


}
