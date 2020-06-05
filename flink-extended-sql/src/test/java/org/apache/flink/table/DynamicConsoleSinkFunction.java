package org.apache.flink.table;

import java.util.Objects;
import java.util.function.Function;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class DynamicConsoleSinkFunction<T> implements SinkFunction<T> {

  private final Function<T, String> printFunction;

  DynamicConsoleSinkFunction(Function<T, String> printFunction) {
    this.printFunction = Objects.requireNonNull(printFunction);
  }

  @Override
  public void invoke(T value, Context context) throws Exception {
    String message = printFunction.apply(value);
    System.out.println("console received: " + message);
  }

}
