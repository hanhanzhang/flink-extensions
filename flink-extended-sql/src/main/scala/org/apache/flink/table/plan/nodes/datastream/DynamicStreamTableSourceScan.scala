package org.apache.flink.table.plan.nodes.datastream

import java.lang.{String => JString}
import java.util.function.{Function => JFunction}
import java.util.{List => JList, Map => JMap}

import org.apache.calcite.plan._
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.java.typeutils.{RowTypeInfo, TupleTypeInfo}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{DynamicBroadcastFunction, DynamicSqlMonitorFunction, DynamicStreamNameUtils, TableException, TableSchema}
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.planner.StreamPlanner
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.sources._
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.table.utils.TypeMappingUtils


class DynamicStreamTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSchema: TableSchema,
    tableSource: StreamTableSource[_],
    selectedFields: Option[Array[Int]])
  extends PhysicalTableSourceScan (
    cluster,
    traitSet,
    table,
    tableSchema,
    tableSource,
    selectedFields
  ) with StreamScan {

  override def deriveRowType(): RelDataType = {
    val rowType = table.getRowType
    selectedFields.map(idxs => {
      val fields = rowType.getFieldList
      val builder = cluster.getTypeFactory.builder()
      idxs.map(fields.get).foreach(builder.add)
      builder.build()
    }).getOrElse(rowType)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val rowCnt = metadata.getRowCount(this)
    planner.getCostFactory.makeCost(rowCnt, rowCnt, rowCnt * estimateRowSize(getRowType))
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DynamicStreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSchema,
      tableSource,
      selectedFields
    )
  }

  override def copy(
     traitSet: RelTraitSet,
     newTableSource: TableSource[_]): PhysicalTableSourceScan = {

    new DynamicStreamTableSourceScan(
      cluster,
      traitSet,
      getTable,
      tableSchema,
      newTableSource.asInstanceOf[StreamTableSource[_]],
      selectedFields
    )
  }

  private def translateDataStream(planner: StreamPlanner): DataStream[CRow] = {

    val config = planner.getConfig
    val inputDataStream = tableSource.getDataStream(planner.getExecutionEnvironment)
      .asInstanceOf[DataStream[Any]]
    // Source输出所有字段数据
    val outputSchema = new RowSchema(table.getRowType)

    val inputDataType = fromLegacyInfoToDataType(inputDataStream.getType)
    val producedDataType = tableSource.getProducedDataType

    // check that declared and actual type of table source DataStream are identical
    if (inputDataType != producedDataType) {
      throw new TableException(s"TableSource of type ${tableSource.getClass.getName} " +
        s"returned a DataStream of data type $inputDataType that does not match with the " +
        s"data type $producedDataType declared by the TableSource.getProducedDataType() method. " +
        s"Please validate the implementation of the TableSource.")
    }

    val nameMapping: JFunction[String, String] = tableSource match {
      case mapping: DefinedFieldMapping if mapping.getFieldMapping != null =>
        new JFunction[String, String] {
          override def apply(t: String): String = mapping.getFieldMapping.get(t)
        }
      case _ =>
        new JFunction[String, String] {
          override def apply(t: String): String = t
        }
    }

    // get expression to extract rowtime attribute
    val rowtimeExpression = TableSourceUtil.getRowtimeAttributeDescriptor(
      tableSource,
      selectedFields)
      .map(desc => TableSourceUtil.getRowtimeExtractionExpression(
        desc.getTimestampExtractor,
        producedDataType,
        TypeConversions.fromLegacyInfoToDataType(TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
        planner.getRelBuilder,
        nameMapping
      ))

    // Source输出所有字段数据
    val fieldIndexes = TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
      tableSource,
      tableSchema.getTableColumns,
      true,
      nameMapping
    )

    // ingest table and convert and extract time attributes if necessary
    val ingestedTable = convertToInternalRow(
      outputSchema,
      inputDataStream,
      fieldIndexes,
      config,
      rowtimeExpression)

    // generate watermarks for rowtime indicator
    val rowtimeDesc: Option[RowtimeAttributeDescriptor] =
      TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, selectedFields)

    val withWatermarks = if (rowtimeDesc.isDefined) {
      throw new RuntimeException("unsupported")
    } else {
      // No need to generate watermarks if no rowtime attribute is specified.
      ingestedTable
    }

    withWatermarks
  }

  override def translateToPlan(planner: StreamPlanner): DataStream[CRow] = {
    val env = planner.getExecutionEnvironment
    // broadcast stream
    val sqlSchemaStream = env.addSource(new DynamicSqlMonitorFunction)
    val valueType = new TupleTypeInfo[Tuple2[JMap[JString, JString], JList[JString]]](
      Types.MAP(Types.STRING, Types.INT),
      Types.LIST(Types.STRING)
    )
    val stateDescriptor = new MapStateDescriptor(
      "BroadcastState",
      Types.STRING,
      valueType
    )
    val broadcastStream = sqlSchemaStream.broadcast(stateDescriptor)

    // source stream
    val dataStream = translateDataStream(planner)

    val uniqueNodeName = DynamicStreamNameUtils.getStreamNodeUniqueName(this)
    val sourceFieldNames = table.getRowType.getFieldNames
    val selectFieldNames = deriveRowType().getFieldNames

    val returnTypeInfo = CRowTypeInfo(
      new RowTypeInfo(
        Array[TypeInformation[_]](Types.STRING, Types.STRING),
        Array[String]("type", "data")
      )
    )

    dataStream.connect(broadcastStream)
      .process(new DynamicBroadcastFunction(uniqueNodeName, sourceFieldNames, selectFieldNames))
      .returns(returnTypeInfo)
  }

}
