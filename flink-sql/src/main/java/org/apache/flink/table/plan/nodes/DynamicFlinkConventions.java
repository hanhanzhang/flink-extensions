package org.apache.flink.table.plan.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.flink.table.plan.nodes.datastream.DynamicDataStreamRel;

public class DynamicFlinkConventions {

  public static Convention DYNAMIC_DATA_STREAM = new Convention.Impl("DYNAMIC_DATA_STREAM",
      DynamicDataStreamRel.class);

}
