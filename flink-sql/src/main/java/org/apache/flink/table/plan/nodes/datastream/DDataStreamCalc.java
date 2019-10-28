package org.apache.flink.table.plan.nodes.datastream;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.codegen.ConditionRexVisitor;
import org.apache.flink.table.codegen.DConditionInvoker;
import org.apache.flink.table.codegen.DProjectFieldRexVisitor;
import org.apache.flink.table.codegen.DRexInvoker;
import org.apache.flink.table.delegation.DStreamPlanner;
import org.apache.flink.table.runtime.DStreamCalcProcessFunction;
import org.apache.flink.types.DStreamRecord;
import scala.Option;
import scala.collection.Seq;

public class DDataStreamCalc extends Calc implements DDataStreamRel {

  public DDataStreamCalc(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram calcProgram) {
    super(cluster, traits, input, calcProgram);
  }

  @Override
  public Calc copy(RelTraitSet relTraitSet, RelNode child, RexProgram rexProgram) {
    return new DDataStreamCalc(getCluster(),
        relTraitSet,
        child,
        rexProgram);
  }

  @Override
  public DataStream<DStreamRecord> translateToSqlElement(DStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DDataStreamRel inputRelNode = (DDataStreamRel) getInput();
    DataStream<DStreamRecord> inputDataStream = inputRelNode.translateToSqlElement(tableEnv, queryConfig);

    // condition
    List<DConditionInvoker> conditionExpressions;
    if (program.getCondition() != null) {
      RexNode rexNode = program.expandLocalRef(program.getCondition());
      conditionExpressions = new LinkedList<>();
      rexNode.accept(new ConditionRexVisitor(conditionExpressions));

    }

    // projection
    final DProjectFieldRexVisitor projectFieldRexVisitor = new DProjectFieldRexVisitor(deriveRowType());
    final Map<String, DRexInvoker<String>> projectFields = new HashMap<>();
    for (int i = 0; i < program.getNamedProjects().size(); ++i) {
      Pair<RexLocalRef, String> rexLocalRefAndName = program.getNamedProjects().get(i);
      RexNode rexNode = program.expandLocalRef(rexLocalRefAndName.left);
      projectFields.put(rexLocalRefAndName.right, rexNode.accept(projectFieldRexVisitor));
    }

    return inputDataStream.process(new DStreamCalcProcessFunction(projectFields))
        .setParallelism(inputDataStream.getParallelism())
        .returns(TypeInformation.of(DStreamRecord.class));

  }

  @Override
  public String getExpressionString(RexNode expr, Seq<String> inFields,
      Option<Seq<RexNode>> localExprsTable) {
    return DDataStreamRel.super.getExpressionString(expr, inFields, localExprsTable);
  }

  @Override
  public double estimateRowSize(RelDataType rowType) {
    return DDataStreamRel.super.estimateRowSize(rowType);
  }

  @Override
  public double estimateDataTypeSize(RelDataType t) {
    return DDataStreamRel.super.estimateDataTypeSize(t);
  }

}
