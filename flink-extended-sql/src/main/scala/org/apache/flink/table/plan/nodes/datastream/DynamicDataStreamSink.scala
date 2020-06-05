package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink => StreamingDataStreamSink}
import org.apache.flink.table.api.{TableException, TableSchema}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.plan.nodes.Sink
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.table.planner.{DataStreamConversions, StreamPlanner}
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.runtime.{DynamicSqlSinkFilterFunction, DynamicSqlSinkMapFunction}
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, TableSink, UpsertStreamTableSink}
import org.apache.flink.table.types.utils.TypeConversions

import _root_.scala.collection.JavaConverters._

class DynamicDataStreamSink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sink: TableSink[_],
    sinkName: String)
  extends Sink(cluster, traitSet, inputRel, sink, sinkName)
  with DataStreamRel {

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DynamicDataStreamSink(cluster, traitSet, inputs.get(0), sink, sinkName)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val inputTransform = writeToSink(planner).asInstanceOf[Transformation[CRow]]
    new DataStream(planner.getExecutionEnvironment, inputTransform)
  }

  private def writeToSink[T](planner: StreamPlanner): Transformation[_] = {
    sink match {
      case retractSink: RetractStreamTableSink[T] =>
        throw new RuntimeException("Unsupported")

      case upsertSink: UpsertStreamTableSink[T] =>
        throw new RuntimeException("Unsupported")

      case appendSink: AppendStreamTableSink[T] =>
        val resultSink = writeToAppendSink(appendSink, planner)
        resultSink.getTransformation
    }
  }

  private def writeToAppendSink[T](
       sink: AppendStreamTableSink[T],
       planner: StreamPlanner): StreamingDataStreamSink[_]= {

    if (!UpdatingPlanChecker.isAppendOnly(getInput)) {
      throw new TableException(
        "AppendStreamTableSink requires that Table has only insert changes.")
    }

    val outputType = TypeConversions.fromDataTypeToLegacyInfo(sink.getConsumedDataType)
      .asInstanceOf[TypeInformation[T]]
    val resultType = getTableSchema
    // translate the Table into a DataStream and provide the type that the TableSink expects.
    val result: DataStream[T] =
      translateInput(
        planner,
        resultType,
        outputType,
        withChangeFlag = false)
    // Give the DataStream to the TableSink to emit it.
    sink.consumeDataStream(result)
  }

  private def translateInput[A](
           planner: StreamPlanner,
           logicalSchema: TableSchema,
           tpe: TypeInformation[A],
           withChangeFlag: Boolean): DataStream[A] = {
    val dataStream = getInput().asInstanceOf[DataStreamRel].translateToPlan(planner)

    //
    val resultStream = dataStream.filter(new DynamicSqlSinkFilterFunction)
        .map(new DynamicSqlSinkMapFunction)
        .returns(getReturnCRowTypeInfo)

    DataStreamConversions.convert(resultStream, logicalSchema, withChangeFlag, tpe, planner.getConfig)
  }

  private def getTableSchema: TableSchema = {
    val fieldTypes = getInput.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo)
      .map(TypeConversions.fromLegacyInfoToDataType)
      .toArray
    TableSchema.builder().fields(sink.getTableSchema.getFieldNames, fieldTypes).build()
  }

  private def getReturnCRowTypeInfo: CRowTypeInfo = {
    val fieldNames = getInput.getRowType.getFieldList.asScala.map(_.getName)
      .toArray
    val fieldTypes = getInput.getRowType.getFieldList.asScala.map(_.getType)
      .map(FlinkTypeFactory.toTypeInfo)
      .toArray
    CRowTypeInfo(new RowTypeInfo(fieldTypes, fieldNames))
  }

}
