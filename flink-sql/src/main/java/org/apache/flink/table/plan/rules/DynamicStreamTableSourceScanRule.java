package org.apache.flink.table.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DataStreamScan;

public class DynamicStreamTableSourceScanRule extends ConverterRule {

  public DynamicStreamTableSourceScanRule() {
    super(DataStreamScan.class, FlinkConventions.DATASTREAM(), FlinkConventions.DATASTREAM(), "DynamicStreamTableSourceScanRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {

    return super.matches(call);
  }

  @Override
  public RelNode convert(RelNode rel) {
    return null;
  }
}
