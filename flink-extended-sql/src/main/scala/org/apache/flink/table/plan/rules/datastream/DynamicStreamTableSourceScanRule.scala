package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.table.plan.nodes.FlinkConventions
import org.apache.flink.table.plan.nodes.datastream.DynamicStreamTableSourceScan
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.plan.schema.TableSourceTable
import org.apache.flink.table.sources.StreamTableSource

class DynamicStreamTableSourceScanRule
  extends ConverterRule(
    classOf[FlinkLogicalTableSourceScan],
    FlinkConventions.LOGICAL,
    FlinkConventions.DATASTREAM,
    "DynamicStreamTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]

    val sourceTable = scan.getTable.unwrap(classOf[TableSourceTable[_]])
    sourceTable != null && sourceTable.isStreamingMode
  }

  override def convert(rel: RelNode): RelNode = {
    val scan: FlinkLogicalTableSourceScan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(FlinkConventions.DATASTREAM)

    new DynamicStreamTableSourceScan(
      rel.getCluster,
      traitSet,
      scan.getTable,
      scan.tableSchema,
      scan.tableSource.asInstanceOf[StreamTableSource[_]],
      scan.selectedFields
    )
  }

}

object DynamicStreamTableSourceScanRule {

  val INSTANCE: RelOptRule = new DynamicStreamTableSourceScanRule

}
