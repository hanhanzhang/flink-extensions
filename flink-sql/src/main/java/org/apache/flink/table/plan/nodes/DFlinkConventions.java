package org.apache.flink.table.plan.nodes;

import org.apache.calcite.plan.Convention;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamRel;

public class DFlinkConventions {

  public static Convention DYNAMIC_DATA_STREAM = new Convention.Impl("DYNAMIC_DATA_STREAM",
      DDataStreamRel.class);

}
