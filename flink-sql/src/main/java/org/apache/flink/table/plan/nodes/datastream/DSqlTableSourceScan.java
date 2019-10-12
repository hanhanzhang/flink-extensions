package org.apache.flink.table.plan.nodes.datastream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.DSqlStreamPlanner;
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan;
import org.apache.flink.table.sources.DSqlProjectFieldsUpdaterProcessFunction;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.CompositeDRow;
import scala.Option;
import scala.collection.Seq;

public class DSqlTableSourceScan extends PhysicalTableSourceScan implements DSqlDataStreamRel {

  private DynamicStreamTableSource tableSource;

  public DSqlTableSourceScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      DynamicStreamTableSource tableSource, Option<int[]> selectedFields) {
    super(cluster, traitSet, table, tableSource, selectedFields);

    this.tableSource = tableSource;
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

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new DSqlTableSourceScan(
        getCluster(),
        traitSet,
        getTable(),
        tableSource,
        selectedFields());
  }

  @Override
  public PhysicalTableSourceScan copy(RelTraitSet traitSet, TableSource<?> tableSource) {
    return new DSqlTableSourceScan(
        getCluster(), traitSet, getTable(), (DynamicStreamTableSource) tableSource, selectedFields());
  }

  @Override
  public DataStream<CompositeDRow> translateToSqlElement(DSqlStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DataStream<CompositeDRow> sourceStream = tableSource.getDataStream(tableEnv.getExecutionEnvironment());
    BroadcastStream<CompositeDRow> broadcastStream = tableSource.getBroadcastStream(tableEnv.getExecutionEnvironment());

    List<String> fieldNames = this.deriveRowType().getFieldNames();

    return sourceStream.connect(broadcastStream)
        .process(new DSqlProjectFieldsUpdaterProcessFunction(fieldNames.toArray(new String[fieldNames.size()])))
        .returns(TypeInformation.of(CompositeDRow.class));

  }
}
