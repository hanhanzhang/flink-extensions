package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.SimpleSqlElement;

public interface DynamicStreamTableSource extends TableSource<SimpleSqlElement> {

  BroadcastStream<SimpleSqlElement> getBroadcastStream(StreamExecutionEnvironment execEnv);

  DataStream<SimpleSqlElement> getDataStream(StreamExecutionEnvironment execEnv);

  @Override
  default TypeInformation<SimpleSqlElement> getReturnType() {
    return TypeInformation.of(SimpleSqlElement.class);
  }
}
