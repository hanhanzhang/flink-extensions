package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.CompositeDRow;

public interface DynamicStreamTableSource extends TableSource<CompositeDRow> {

  BroadcastStream<CompositeDRow> getBroadcastStream(StreamExecutionEnvironment execEnv);

  DataStream<CompositeDRow> getDataStream(StreamExecutionEnvironment execEnv);

  @Override
  default TypeInformation<CompositeDRow> getReturnType() {
    return TypeInformation.of(CompositeDRow.class);
  }

}
