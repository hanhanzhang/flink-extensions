package org.apache.flink.table.api;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public abstract class DStreamSourceFunction<OUT> extends RichSourceFunction<OUT> {

  public abstract DRecordTupleFactory<OUT> getRecordTupleFactory();

  public abstract int getStreamParallelism();

}
