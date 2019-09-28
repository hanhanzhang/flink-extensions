package org.apache.flink.table.runtime;

import java.util.List;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.SimpleSqlElement;
import org.apache.flink.util.Collector;

public class DynamicStreamProcessRunner extends ProcessFunction<SimpleSqlElement, SimpleSqlElement> {

  private final List<Exepression> projectExpression;

  public DynamicStreamProcessRunner(List<Exepression> projectExpression) {
    this.projectExpression = projectExpression;
  }

  @Override
  public void processElement(SimpleSqlElement value, Context ctx, Collector<SimpleSqlElement> out)
      throws Exception {

  }



}
