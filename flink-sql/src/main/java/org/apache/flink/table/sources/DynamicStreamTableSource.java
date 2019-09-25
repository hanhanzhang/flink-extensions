package org.apache.flink.table.sources;

import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.dynamic.DSchema;

public interface DynamicStreamTableSource<T> extends TableSource<T> {

  BroadcastStream<DSchema> getBroadcastStream(StreamExecutionEnvironment execEnv);

}
