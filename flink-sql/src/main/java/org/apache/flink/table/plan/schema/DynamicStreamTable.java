package org.apache.flink.table.plan.schema;

import org.apache.flink.table.plan.stats.FlinkStatistic;
import org.apache.flink.table.sources.DynamicStreamTableSource;

public class DynamicStreamTable extends TableSourceTable {

  public DynamicStreamTable(DynamicStreamTableSource tableSource, FlinkStatistic statistic) {
    super(tableSource, true, statistic);
  }

}
