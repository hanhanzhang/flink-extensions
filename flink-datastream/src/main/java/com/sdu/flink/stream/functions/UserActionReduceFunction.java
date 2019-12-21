package com.sdu.flink.stream.functions;

import com.sdu.flink.entry.UserActionEntry;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UserActionReduceFunction implements AggregateFunction<UserActionEntry, IntCounter, Integer> {

  @Override
  public IntCounter createAccumulator() {
    return new IntCounter(0);
  }

  @Override
  public IntCounter add(UserActionEntry value, IntCounter accumulator) {
    accumulator.add(1);
    return accumulator;
  }

  @Override
  public Integer getResult(IntCounter accumulator) {
    return accumulator.getLocalValue();
  }

  @Override
  public IntCounter merge(IntCounter a, IntCounter b) {
    a.add(b.getLocalValue());
    return a;
  }
}
