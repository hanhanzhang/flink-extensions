package org.apache.flink.table.plan.nodes.datastream;

import static com.sdu.flink.utils.JsonUtils.toJson;
import static org.apache.flink.types.DSchemaType.CONDITION;
import static org.apache.flink.types.DSchemaType.PROJECT;

import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
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
import org.apache.flink.table.delegation.DStreamPlanner;
import org.apache.flink.table.exec.DRexInvoker;
import org.apache.flink.table.exec.DRexInvokerVisitor;
import org.apache.flink.table.runtime.DStreamCalcProcessFunction;
import org.apache.flink.types.DSchemaTupleUtils;
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
  @SuppressWarnings("unchecked")
  public DataStream<DStreamRecord> translateToSqlElement(DStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DDataStreamRel inputRelNode = (DDataStreamRel) getInput();
    DataStream<DStreamRecord> inputDataStream = inputRelNode.translateToSqlElement(tableEnv, queryConfig);

    // projection
    final DRexInvokerVisitor visitor = new DRexInvokerVisitor(deriveRowType());
    final Map<String, DRexInvoker> projectFields = new HashMap<>();
    for (int i = 0; i < program.getNamedProjects().size(); ++i) {
      Pair<RexLocalRef, String> rexLocalRefAndName = program.getNamedProjects().get(i);
      RexNode rexNode = program.expandLocalRef(rexLocalRefAndName.left);
      projectFields.put(rexLocalRefAndName.right, rexNode.accept(visitor));
    }

    // condition
    DRexInvoker conditionInvoker = null;
    if (program.getCondition() != null) {
      RexNode rexNode = program.expandLocalRef(program.getCondition());
      conditionInvoker = rexNode.accept(visitor);
    }

    String streamNodePath = DSchemaTupleUtils.getStreamNodePath(this);

    return inputDataStream.process(new DStreamCalcProcessFunction(streamNodePath, projectFields, conditionInvoker))
        .setParallelism(inputDataStream.getParallelism())
        .returns(TypeInformation.of(DStreamRecord.class));

  }

  @Override
  public Map<String, String> getStreamNodeSchema() {
    Map<String, String> rexInvokers = new HashMap<>(2);

    final DRexInvokerVisitor visitor = new DRexInvokerVisitor(deriveRowType());
    final Map<String, DRexInvoker> projectFields = new HashMap<>();
    for (int i = 0; i < program.getNamedProjects().size(); ++i) {
      Pair<RexLocalRef, String> rexLocalRefAndName = program.getNamedProjects().get(i);
      RexNode rexNode = program.expandLocalRef(rexLocalRefAndName.left);
      projectFields.put(rexLocalRefAndName.right, rexNode.accept(visitor));
    }
    rexInvokers.put(PROJECT.getSchemaTypeName(),
        toJson(projectFields, new TypeToken<Map<String, DRexInvoker>>(){}.getType()));


    DRexInvoker conditionInvoker;
    if (program.getCondition() != null) {
      RexNode rexNode = program.expandLocalRef(program.getCondition());
      conditionInvoker = rexNode.accept(visitor);
      rexInvokers.put(CONDITION.getSchemaTypeName(), toJson(conditionInvoker, DRexInvoker.class));
    }

    return rexInvokers;
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
