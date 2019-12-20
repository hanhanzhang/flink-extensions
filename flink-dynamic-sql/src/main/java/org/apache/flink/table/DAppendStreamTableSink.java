package org.apache.flink.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.DRecordTuple;

public interface DAppendStreamTableSink extends AppendStreamTableSink<DRecordTuple> {

  default void emitDataStream(DataStream<DRecordTuple> dataStream) {

  }

  default TableSink<DRecordTuple> configure(String[] fieldNames,
      TypeInformation<?>[] fieldTypes) {
    return null;
  }

}
