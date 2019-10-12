package org.apache.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.CompositeDRow;

public interface DSqlAppendStreamTableSink extends AppendStreamTableSink<CompositeDRow> {

  default void emitDataStream(DataStream<CompositeDRow> dataStream) {

  }

  default TableSink<CompositeDRow> configure(String[] fieldNames,
      TypeInformation<?>[] fieldTypes) {
    return null;
  }


}
