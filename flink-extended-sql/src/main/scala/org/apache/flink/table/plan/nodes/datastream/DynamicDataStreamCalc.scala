package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Calc
import org.apache.calcite.rex.RexProgram
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.table.calcite.RelTimeIndicatorConverter
import org.apache.flink.table.codegen.FunctionCodeGenerator
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.DynamicCRowProcessRunner
import org.apache.flink.table.runtime.types.CRow

import scala.collection.JavaConverters._


class DynamicDataStreamCalc(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    inputSchema: RowSchema,
    schema: RowSchema,
    calcProgram: RexProgram,
    ruleDescription: String)
  extends DataStreamCalcBase(
    cluster,
    traitSet,
    input,
    inputSchema,
    schema,
    calcProgram,
    ruleDescription) {

  override def copy(traitSet: RelTraitSet, child: RelNode, program: RexProgram): Calc = {
    new DynamicDataStreamCalc(
      cluster,
      traitSet,
      child,
      inputSchema,
      schema,
      program,
      ruleDescription)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val config = planner.getConfig

    val inputDataStream = getInput.asInstanceOf[DataStreamRel].translateToPlan(planner)

    // materialize time attributes in condition
    val condition = if (calcProgram.getCondition != null) {
      val materializedCondition = RelTimeIndicatorConverter.convertExpression(
        calcProgram.expandLocalRef(calcProgram.getCondition),
        inputSchema.relDataType,
        cluster.getRexBuilder)
      Some(materializedCondition)
    } else {
      None
    }

    // filter out time attributes
    val projection = calcProgram.getProjectList.asScala
      .map(calcProgram.expandLocalRef)

    val generator = new FunctionCodeGenerator(config, false, inputSchema.typeInfo)

    val genFunction = generateFunction(
      generator,
      ruleDescription,
      schema,
      projection,
      condition,
      config,
      classOf[ProcessFunction[CRow, CRow]])

    val inputParallelism = inputDataStream.getParallelism

    val processFunc = new DynamicCRowProcessRunner(
      genFunction.name,
      genFunction.code)

    inputDataStream
      .process(processFunc)
      .name(calcOpName(calcProgram, getExpressionString))
      // keep parallelism to ensure order of accumulate and retract messages
      .setParallelism(inputParallelism)

  }
}
