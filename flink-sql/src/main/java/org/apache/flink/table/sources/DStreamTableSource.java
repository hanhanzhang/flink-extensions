package org.apache.flink.table.sources;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DStreamRecord;

public interface DStreamTableSource extends TableSource<DStreamRecord> {

  BroadcastStream<DSchemaTuple> getBroadcastStream(StreamExecutionEnvironment execEnv);

  DataStream<DRecordTuple> getDataStream(StreamExecutionEnvironment execEnv);

  @Override
  default TypeInformation<DStreamRecord> getReturnType() {
    return TypeInformation.of(DStreamRecord.class);
  }

}
