package org.apache.flink.table.plan.nodes.datastream;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.delegation.DynamicStreamPlanner;
import org.apache.flink.table.plan.nodes.PhysicalTableSourceScan;
import org.apache.flink.table.sources.DynamicProjectFieldsUpdaterProcessFunction;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.SimpleSqlElement;
import scala.Option;
import scala.collection.Seq;

public class DynamicTableSourceScan extends PhysicalTableSourceScan implements DynamicDataStreamRel {

  private DynamicStreamTableSource tableSource;

  public DynamicTableSourceScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
      DynamicStreamTableSource tableSource, Option<int[]> selectedFields) {
    super(cluster, traitSet, table, tableSource, selectedFields);

    this.tableSource = tableSource;
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

  @Override
  public PhysicalTableSourceScan copy(RelTraitSet traitSet, TableSource<?> tableSource) {
    return null;
  }

  @Override
  public DataStream<SimpleSqlElement> translateToSqlElement(DynamicStreamPlanner tableEnv,
      StreamQueryConfig queryConfig) {

    DataStream<SimpleSqlElement> sourceStream = tableSource.getDataStream(tableEnv.getExecutionEnvironment());
    BroadcastStream<SimpleSqlElement> broadcastStream = tableSource.getBroadcastStream(tableEnv.getExecutionEnvironment());

    List<String> fieldNames = this.deriveRowType().getFieldNames();

    return sourceStream.connect(broadcastStream)
        .process(new DynamicProjectFieldsUpdaterProcessFunction(fieldNames.toArray(new String[fieldNames.size()])))
        .returns(TypeInformation.of(SimpleSqlElement.class));

  }
}
