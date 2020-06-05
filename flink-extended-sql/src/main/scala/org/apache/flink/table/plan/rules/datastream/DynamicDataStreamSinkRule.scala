package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DynamicDataStreamSink
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalSink

class DynamicDataStreamSinkRule
  extends ConverterRule(
    classOf[FlinkLogicalSink],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DynamicDataStreamSinkRule") {

  override def convert(rel: RelNode): RelNode = {
    val sink: FlinkLogicalSink = rel.asInstanceOf[FlinkLogicalSink]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)
    val convInput: RelNode = RelOptRule.convert(sink.getInput(0), FlinkConventions.DATASTREAM)

    new DynamicDataStreamSink(
      rel.getCluster,
      traitSet,
      convInput,
      sink.sink,
      sink.sinkName
    )
  }

}

object DynamicDataStreamSinkRule {

  val INSTANCE = new DynamicDataStreamSinkRule

}