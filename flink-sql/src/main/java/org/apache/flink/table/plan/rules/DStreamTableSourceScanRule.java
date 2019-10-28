package org.apache.flink.table.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.DFlinkConventions;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DTableSourceScan;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.sources.DStreamTableSource;
import org.apache.flink.table.sources.TableSource;

public class DStreamTableSourceScanRule extends ConverterRule {

  public static DStreamTableSourceScanRule INSTANCE = new DStreamTableSourceScanRule();

  private DStreamTableSourceScanRule() {
    super(FlinkLogicalTableSourceScan.class, FlinkConventions.LOGICAL(), DFlinkConventions.DYNAMIC_DATA_STREAM, "DStreamTableSourceScanRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FlinkLogicalTableSourceScan scan = call.rel(0);

    TableSource<?> tableSource = scan.tableSource();

    return tableSource instanceof DStreamTableSource;
  }

  @Override
  public RelNode convert(RelNode rel) {
    FlinkLogicalTableSourceScan scan = (FlinkLogicalTableSourceScan) rel;
    RelTraitSet traitSet = rel.getTraitSet().replace(DFlinkConventions.DYNAMIC_DATA_STREAM);

    return new DTableSourceScan(rel.getCluster(), traitSet, scan.getTable(),
        (DStreamTableSource) scan.tableSource(), scan.selectedFields());
  }
}
