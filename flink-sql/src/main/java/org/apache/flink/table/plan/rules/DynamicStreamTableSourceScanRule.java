package org.apache.flink.table.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.DynamicFlinkConventions;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DynamicTableSourceScan;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.TableSource;

public class DynamicStreamTableSourceScanRule extends ConverterRule {

  public static DynamicStreamTableSourceScanRule INSTANCE = new DynamicStreamTableSourceScanRule();

  private DynamicStreamTableSourceScanRule() {
    super(FlinkLogicalTableSourceScan.class, FlinkConventions.LOGICAL(), DynamicFlinkConventions.DYNAMIC_DATA_STREAM, "DynamicStreamTableSourceScanRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FlinkLogicalTableSourceScan scan = call.rel(0);

    TableSource<?> tableSource = scan.tableSource();

    return tableSource instanceof DynamicStreamTableSource;
  }

  @Override
  public RelNode convert(RelNode rel) {
    FlinkLogicalTableSourceScan scan = (FlinkLogicalTableSourceScan) rel;
    RelTraitSet traitSet = rel.getTraitSet().replace(DynamicFlinkConventions.DYNAMIC_DATA_STREAM);

    return new DynamicTableSourceScan(rel.getCluster(), traitSet, scan.getTable(),
        (DynamicStreamTableSource) scan.tableSource(), scan.selectedFields());
  }
}
