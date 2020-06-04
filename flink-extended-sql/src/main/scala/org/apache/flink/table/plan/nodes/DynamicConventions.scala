package org.apache.flink.table.plan.nodes

import org.apache.calcite.plan.Convention
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel

object DynamicConventions {

  val DYNAMIC_DATASTREAM = new Convention.Impl("DYNAMIC_DATASTREAM", classOf[DataStreamRel])


}
