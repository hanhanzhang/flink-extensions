package org.apache.flink.table.plan.nodes.datastream;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.codegen.CodeGeneratorRexVisitor;
import org.apache.flink.table.delegation.DSqlStreamPlanner;
import org.apache.flink.table.runtime.DSqlStreamProcessRunner;
import org.apache.flink.table.runtime.Exepression;
import org.apache.flink.types.CompositeDRow;
import scala.Option;
import scala.collection.Seq;

public class DSqlDataStreamCalc extends Calc implements DSqlDataStreamRel {

  public DSqlDataStreamCalc(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram calcProgram) {
    super(cluster, traits, input, calcProgram);
  }

  @Override
  public Calc copy(RelTraitSet relTraitSet, RelNode child, RexProgram rexProgram) {
    return new DSqlDataStreamCalc(getCluster(),
        relTraitSet,
        child,
        rexProgram);
  }

  @Override
  public DataStream<CompositeDRow> translateToSqlElement(DSqlStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DSqlDataStreamRel inputRelNode = (DSqlDataStreamRel) getInput();
    DataStream<CompositeDRow> inputDataStream = inputRelNode.translateToSqlElement(tableEnv, queryConfig);

    // condition
    if (program.getCondition() != null) {
      RexNode rexNode = program.expandLocalRef(program.getCondition());
    }

    CodeGeneratorRexVisitor visitor = new CodeGeneratorRexVisitor(deriveRowType());

    // projection
    List<Exepression> projection = program.getProjectList()
        .stream()
        .map(program::expandLocalRef)
        .map(rexNode -> rexNode.accept(visitor))
        .collect(Collectors.toList());


//    List<Exepression> projection = new ArrayList<>();
//    for (RexLocalRef rexLocalRef : program.getProjectList()) {
//      RexNode rexNode = program.expandLocalRef(rexLocalRef);
//      Exepression exepression = rexNode.accept(visitor);
//      projection.add(exepression);
//    }

    return inputDataStream.process(new DSqlStreamProcessRunner(projection))
        .setParallelism(inputDataStream.getParallelism())
        .returns(TypeInformation.of(CompositeDRow.class));

  }

  @Override
  public String getExpressionString(RexNode expr, Seq<String> inFields,
      Option<Seq<RexNode>> localExprsTable) {
    return DSqlDataStreamRel.super.getExpressionString(expr, inFields, localExprsTable);
  }

  @Override
  public double estimateRowSize(RelDataType rowType) {
    return DSqlDataStreamRel.super.estimateRowSize(rowType);
  }

  @Override
  public double estimateDataTypeSize(RelDataType t) {
    return DSqlDataStreamRel.super.estimateDataTypeSize(t);
  }

}
