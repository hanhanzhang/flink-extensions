package org.apache.flink.table.plan.nodes.datastream;

import static com.sdu.flink.utils.JsonUtils.toJson;
import static org.apache.flink.types.DSchemaType.PROJECT;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.planner.DStreamPlanner;
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan;
import org.apache.flink.table.sources.DProjectFieldsSelectProcessFunction;
import org.apache.flink.table.sources.DStreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.DRecordTuple;
import org.apache.flink.types.DSchemaTuple;
import org.apache.flink.types.DSchemaTupleUtils;
import org.apache.flink.types.DStreamRecord;
import scala.Option;
import scala.collection.Seq;

public class DTableSourceScan extends PhysicalTableSourceScan implements DDataStreamRel {

  private DStreamTableSource tableSource;

  public DTableSourceScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      DStreamTableSource tableSource, Option<int[]> selectedFields) {
    super(cluster, traitSet, table, tableSource, selectedFields);

    this.tableSource = tableSource;
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

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DTableSourceScan(
        getCluster(),
        traitSet,
        getTable(),
        tableSource,
        selectedFields());
  }

  @Override
  public PhysicalTableSourceScan copy(RelTraitSet traitSet, TableSource<?> tableSource) {
    return new DTableSourceScan(
        getCluster(), traitSet, getTable(), (DStreamTableSource) tableSource, selectedFields());
  }

  @Override
  public DataStream<DStreamRecord> translateToSqlElement(DStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DataStream<DRecordTuple> sourceStream = tableSource.getDataStream(tableEnv.getExecutionEnvironment());
    BroadcastStream<DSchemaTuple> broadcastStream = tableSource.getBroadcastStream(tableEnv.getExecutionEnvironment());

    RelDataType dataType = this.deriveRowType();
    List<RelDataTypeField> fieldDataTypes =  dataType.getFieldList();
    Map<String, String> projectNameToTypes = new HashMap<>();
    for (RelDataTypeField field : fieldDataTypes) {
      projectNameToTypes.put(field.getName(), field.getValue().getSqlTypeName().getName());
    }

    String streamNodePath = DSchemaTupleUtils.getStreamNodePath(this);

    return sourceStream.connect(broadcastStream)
        .process(new DProjectFieldsSelectProcessFunction(streamNodePath, projectNameToTypes))
        .returns(TypeInformation.of(DStreamRecord.class));

  }

  @Override
  public Map<String, String> getStreamNodeSchema() {
    RelDataType dataType = this.deriveRowType();
    List<RelDataTypeField> fieldDataTypes =  dataType.getFieldList();
    Map<String, String> projectNameToTypes = new HashMap<>();
    for (RelDataTypeField field : fieldDataTypes) {
      projectNameToTypes.put(field.getName(), field.getValue().getSqlTypeName().getName());
    }

    Map<String, String> streamNodeSchema = new HashMap<>(2);
    streamNodeSchema.put(PROJECT.getSchemaTypeName(), toJson(projectNameToTypes));

    return streamNodeSchema;
  }
}
