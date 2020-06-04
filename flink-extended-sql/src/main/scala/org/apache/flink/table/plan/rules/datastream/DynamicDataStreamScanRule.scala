package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.RelTraitSet
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.datastream.DynamicDataStreamScan
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalDataStreamScan
import org.apache.flink.table.plan.nodes.{DynamicConventions, FlinkConventions}

class DynamicDataStreamScanRule extends ConverterRule(
  classOf[FlinkLogicalDataStreamScan],
  FlinkConventions.LOGICAL,
  DynamicConventions.DYNAMIC_DATASTREAM,
  "DynamicDataStreamScanRule") {

  override def convert(relNode: RelNode): RelNode = {
    val scan = relNode.asInstanceOf[FlinkLogicalDataStreamScan]
    val traitSet: RelTraitSet = relNode.getTraitSet.replace(DynamicConventions.DYNAMIC_DATASTREAM)

    new DynamicDataStreamScan(
      scan.getCluster,
      traitSet,
      scan.catalog,
      scan.dataStream,
      scan.fieldIdxs,
      scan.schema
    )
  }

}

object DynamicDataStreamScanRule {

  val INSTANCE = new DynamicDataStreamScanRule

}
