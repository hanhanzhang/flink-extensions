package org.apache.flink.table.plan.rules;

import static org.apache.flink.table.plan.nodes.DFlinkConventions.DYNAMIC_DATA_STREAM;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DDataStreamCalc;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;

public class DDataStreamCalcRule extends ConverterRule {

  public static DDataStreamCalcRule INSTANCE = new DDataStreamCalcRule();

  private DDataStreamCalcRule() {
    super(FlinkLogicalCalc.class, FlinkConventions.LOGICAL(), DYNAMIC_DATA_STREAM, "DDataStreamCalcRule");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
    RelTraitSet traitSet = relNode.getTraitSet().replace(DYNAMIC_DATA_STREAM);
    RelNode convertInput = RelOptRule.convert(calc.getInput(), DYNAMIC_DATA_STREAM);

    return new DDataStreamCalc(relNode.getCluster(),
        traitSet,
        convertInput,
        calc.getProgram());
  }
}
