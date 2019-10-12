package org.apache.flink.table.plan.rules;

import static org.apache.flink.table.plan.nodes.DSqlFlinkConventions.DYNAMIC_DATA_STREAM;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.flink.table.plan.nodes.FlinkConventions;
import org.apache.flink.table.plan.nodes.datastream.DSqlDataStreamCalc;
import org.apache.flink.table.plan.nodes.logical.FlinkLogicalCalc;

public class DSqlDataStreamCalcRule extends ConverterRule {

  public static DSqlDataStreamCalcRule INSTANCE = new DSqlDataStreamCalcRule();

  private DSqlDataStreamCalcRule() {
    super(FlinkLogicalCalc.class, FlinkConventions.LOGICAL(), DYNAMIC_DATA_STREAM, "DSqlDataStreamCalcRule");
  }

  @Override
  public RelNode convert(RelNode relNode) {
    FlinkLogicalCalc calc = (FlinkLogicalCalc) relNode;
    RelTraitSet traitSet = relNode.getTraitSet().replace(DYNAMIC_DATA_STREAM);
    RelNode convertInput = RelOptRule.convert(calc.getInput(), DYNAMIC_DATA_STREAM);

    return new DSqlDataStreamCalc(relNode.getCluster(),
        traitSet,
        convertInput,
        calc.getProgram());
  }
}
