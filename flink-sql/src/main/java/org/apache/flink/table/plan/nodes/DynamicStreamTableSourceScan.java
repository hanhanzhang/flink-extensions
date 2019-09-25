package org.apache.flink.table.plan.nodes;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.plan.nodes.datastream.StreamTableSourceScan;
import org.apache.flink.table.planner.StreamPlanner;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.sources.DynamicStreamTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import scala.Option;

public class DynamicStreamTableSourceScan extends StreamTableSourceScan {

  public DynamicStreamTableSourceScan(
      RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, DynamicStreamTableSource<?> tableSource, Option<int[]> selectedFields) {
    super(cluster, traitSet, table, (StreamTableSource<?>) tableSource, selectedFields);
  }

  @Override
  public DataStream<CRow> translateToPlan(StreamPlanner planner, StreamQueryConfig queryConfig) {
    DataStream<CRow> sourceStream = super.translateToPlan(planner, queryConfig);



    return sourceStream;
  }
}
