package org.apache.flink.table.plan.nodes.datastream

import java.util

import org.apache.calcite.plan.{RelOptCluster, RelOptSchema, RelTraitSet}
import org.apache.calcite.prepare.RelOptTableImpl
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{DynamicBroadcastFunction, DynamicStreamNameUtils, DynamicSqlMonitorFunction}
import org.apache.flink.table.expressions.Cast
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo

import scala.collection.JavaConverters._


class DynamicDataStreamScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    catalog: RelOptSchema,
    dataStream: DataStream[_],
    fieldIdxs: Array[Int],
    schema: RowSchema)
  extends TableScan(
    cluster,
    traitSet,
    RelOptTableImpl.create(catalog, schema.relDataType, List[String]().asJava, null)
) with StreamScan {

  override def deriveRowType(): RelDataType = schema.relDataType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DynamicDataStreamScan(
      cluster,
      traitSet,
      catalog,
      dataStream,
      fieldIdxs,
      schema
    )
  }

  private def translateDataStream(planner: StreamPlanner) : DataStream[CRow] = {
    val config = planner.getConfig

    // get expression to extract timestamp
    val rowtimeExpr: Option[RexNode] =
      if (fieldIdxs.contains(TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER)) {
        // extract timestamp from StreamRecord
        Some(
          Cast(
            org.apache.flink.table.expressions.StreamRecordTimestamp(),
            TimeIndicatorTypeInfo.ROWTIME_INDICATOR)
            .toRexNode(planner.getRelBuilder))
      } else {
        None
      }

    // convert DataStream
    convertToInternalRow(
      schema,
      dataStream.asInstanceOf[DataStream[Any]],
      fieldIdxs,
      config,
      rowtimeExpr)
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val env = planner.getExecutionEnvironment
    // broadcast stream
    val sqlSchemaStream = env.addSource(new DynamicSqlMonitorFunction)
    val stateDescriptor = new MapStateDescriptor[Void, util.Map[String, String]]("BroadcastSqlProjectSchemaState",
      Types.VOID, Types.MAP(Types.STRING, Types.STRING))
    val broadcastStream = sqlSchemaStream.broadcast(stateDescriptor)

    // source stream
    val dataStream = translateDataStream(planner)

    val uniqueNodeName = DynamicStreamNameUtils.getStreamNodeUniqueName(this)
    val sourceFieldNames = deriveRowType().getFieldNames

    val returnTypeInfo = CRowTypeInfo(
      new RowTypeInfo(
        Array[TypeInformation[_]](Types.STRING, Types.STRING),
        Array[String]("type", "data")
      )
    )

    dataStream.connect(broadcastStream)
      .process(new DynamicBroadcastFunction(uniqueNodeName, sourceFieldNames, sourceFieldNames))
      .returns(returnTypeInfo)
  }

  override def explainTerms(pw: RelWriter): RelWriter =
    pw.item("id", s"${dataStream.getId}")
      .item("fields", s"${String.join(", ", schema.relDataType.getFieldNames)}")

}
