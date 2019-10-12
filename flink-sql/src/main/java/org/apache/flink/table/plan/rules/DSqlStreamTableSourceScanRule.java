package org.apache.flink.table.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.DSqlFlinkConventions;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DSqlTableSourceScan;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.TableSource;

public class DSqlStreamTableSourceScanRule extends ConverterRule {

  public static DSqlStreamTableSourceScanRule INSTANCE = new DSqlStreamTableSourceScanRule();

  private DSqlStreamTableSourceScanRule() {
    super(FlinkLogicalTableSourceScan.class, FlinkConventions.LOGICAL(), DSqlFlinkConventions.DYNAMIC_DATA_STREAM, "DSqlStreamTableSourceScanRule");
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
    RelTraitSet traitSet = rel.getTraitSet().replace(DSqlFlinkConventions.DYNAMIC_DATA_STREAM);

    return new DSqlTableSourceScan(rel.getCluster(), traitSet, scan.getTable(),
        (DynamicStreamTableSource) scan.tableSource(), scan.selectedFields());
  }
}
