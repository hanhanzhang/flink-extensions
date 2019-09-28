package org.apache.flink.table.plan.nodes.datastream;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.DynamicStreamPlanner;
import org.apache.flink.table.runtime.DynamicStreamProcessRunner;
import org.apache.flink.table.runtime.Exepression;
import org.apache.flink.types.SimpleSqlElement;
import scala.Option;
import scala.collection.Seq;

public class DynamicDataStreamCalc extends Calc implements DynamicDataStreamRel {

  public DynamicDataStreamCalc(RelOptCluster cluster, RelTraitSet traits, RelNode input, RexProgram calcProgram) {
    super(cluster, traits, input, calcProgram);
  }

  @Override
  public Calc copy(RelTraitSet relTraitSet, RelNode relNode, RexProgram rexProgram) {
    return null;
  }

  @Override
  public DataStream<SimpleSqlElement> translateToSqlElement(DynamicStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DynamicDataStreamRel inputRelNode = (DynamicDataStreamRel) getInput();
    DataStream<SimpleSqlElement> inputDataStream = inputRelNode.translateToSqlElement(tableEnv, queryConfig);

    // condition
    if (program.getCondition() != null) {

    }

    // projection
    List<Exepression> projection = program.getProjectList()
        .stream()
        .map(program::expandLocalRef)
        .map(rexNode -> rexNode.accept(SimpleRexVisitor.INSTANCE))
        .collect(Collectors.toList());

    return inputDataStream.process(new DynamicStreamProcessRunner(projection))
        .setParallelism(inputDataStream.getParallelism())
        .returns(TypeInformation.of(SimpleSqlElement.class));

  }

  @Override
  public String getExpressionString(RexNode expr, Seq<String> inFields,
      Option<Seq<RexNode>> localExprsTable) {
    return DynamicDataStreamRel.super.getExpressionString(expr, inFields, localExprsTable);
  }

  @Override
  public double estimateRowSize(RelDataType rowType) {
    return DynamicDataStreamRel.super.estimateRowSize(rowType);
  }

  @Override
  public double estimateDataTypeSize(RelDataType t) {
    return DynamicDataStreamRel.super.estimateDataTypeSize(t);
  }

  private static class SimpleRexVisitor implements RexVisitor<Exepression> {

    static SimpleRexVisitor INSTANCE = new SimpleRexVisitor();

    private SimpleRexVisitor() {

    }

    @Override
    public Exepression visitInputRef(RexInputRef rexInputRef) {
      return null;
    }

    @Override
    public Exepression visitLocalRef(RexLocalRef rexLocalRef) {
      return null;
    }

    @Override
    public Exepression visitLiteral(RexLiteral rexLiteral) {
      return null;
    }

    @Override
    public Exepression visitCall(RexCall rexCall) {
      return null;
    }

    @Override
    public Exepression visitOver(RexOver rexOver) {
      return null;
    }

    @Override
    public Exepression visitCorrelVariable(RexCorrelVariable rexCorrelVariable) {
      return null;
    }

    @Override
    public Exepression visitDynamicParam(RexDynamicParam rexDynamicParam) {
      return null;
    }

    @Override
    public Exepression visitRangeRef(RexRangeRef rexRangeRef) {
      return null;
    }

    @Override
    public Exepression visitFieldAccess(RexFieldAccess rexFieldAccess) {
      return null;
    }

    @Override
    public Exepression visitSubQuery(RexSubQuery rexSubQuery) {
      return null;
    }

    @Override
    public Exepression visitTableInputRef(RexTableInputRef rexTableInputRef) {
      return null;
    }

    @Override
    public Exepression visitPatternFieldRef(RexPatternFieldRef rexPatternFieldRef) {
      return null;
    }

  }

}
